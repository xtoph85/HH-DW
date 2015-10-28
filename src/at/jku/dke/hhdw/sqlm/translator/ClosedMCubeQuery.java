package at.jku.dke.sqlm.translator;

import java.util.HashMap;
import java.util.LinkedList;

import at.jku.dke.sqlm.interpreter.DataDictionary;
import at.jku.dke.sqlm.parser.ASTAggregationFunction;
import at.jku.dke.sqlm.parser.ASTAttributeUnit;
import at.jku.dke.sqlm.parser.ASTAttributeUnitConversionRule;
import at.jku.dke.sqlm.parser.ASTClosedMCubeQuery;
import at.jku.dke.sqlm.parser.ASTComparisonOperator;
import at.jku.dke.sqlm.parser.ASTCubeBlock;
import at.jku.dke.sqlm.parser.ASTDiceBlock;
import at.jku.dke.sqlm.parser.ASTDiceMultilevelObjectIDList;
import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTInputCubeBlock;
import at.jku.dke.sqlm.parser.ASTMultilevelFactMeasureID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectAttributeID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectLevelID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.ASTNumberValue;
import at.jku.dke.sqlm.parser.ASTObjectCollectionConstructor;
import at.jku.dke.sqlm.parser.ASTProjectionBlock;
import at.jku.dke.sqlm.parser.ASTSliceBlock;
import at.jku.dke.sqlm.parser.ASTSliceExpressionPath;
import at.jku.dke.sqlm.parser.ASTStringValue;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Checks if there exist sub-queries and recursive process them. For each closed
 * Multilevel Cube Query the slice, dice and projection parts get translated.
 * @param queryRootNode is the root SQLMNode of the current closed Multilevel Cube Query
 * @param dimList List contains the cube coordinate sequence of the queried Multilevel Cube
 * @param mcubeName String contains the name of the queried Multilevel Cube
 * @throws SQLException 
 */
public class ClosedMCubeQuery {

	private DataDictionary dataDict;
	private LinkedList<String> dimensionSequenceList;
	private SQLMNode closedMCubeQueryRootNode;
	private String mcubeName;
	private StringBuffer queryCode;
	private int affixOfCurrentlyTranslatedDimension;
	private int mObjectAttributeUnitAffix = 0;
	private int mCubeAffix = 0;
	private int mObjectValueAffix = 0;
	
	public ClosedMCubeQuery(SQLMNode queryRootNode, LinkedList<String> dimList, String mcubeName){
		queryCode = new StringBuffer();
		dimensionSequenceList = dimList;
		closedMCubeQueryRootNode = queryRootNode;
		this.mcubeName = mcubeName;
	}
	
	public String getQueryCode(){
		this.translateQuery(closedMCubeQueryRootNode);
		return queryCode.toString();
	}
	
	private void translateQuery(SQLMNode currentSubQueryRootNode){
		//check for SubQuery
		if(currentSubQueryRootNode.jjtGetChild(ASTDiceBlock.class)!= null){
			SQLMNode diceBlock = currentSubQueryRootNode.jjtGetChild(ASTDiceBlock.class);
			if(diceBlock.jjtGetChild(ASTInputCubeBlock.class).jjtGetChild(ASTClosedMCubeQuery.class) != null){
				this.translateQuery(diceBlock.jjtGetChild(ASTInputCubeBlock.class).jjtGetChild(ASTClosedMCubeQuery.class));
			}
		}else{ //CubeBlock
			SQLMNode cubeBlock = currentSubQueryRootNode.jjtGetChild(ASTCubeBlock.class);
			if(cubeBlock.jjtGetChild(ASTClosedMCubeQuery.class) != null){
				this.translateQuery(cubeBlock.jjtGetChild(ASTClosedMCubeQuery.class));
			}
		}
		translateCurrentQueryLevel(currentSubQueryRootNode);
	}
	
	private void translateCurrentQueryLevel(SQLMNode currentSubQueryRootNode){
		queryCode.append(translateSlicePartOfCurrentQueryLevel(currentSubQueryRootNode));
		queryCode.append(translateProjectionPartOfCurrentQueryLevel(currentSubQueryRootNode));
		queryCode.append(translateDicePartOfCurrentQueryLevel(currentSubQueryRootNode));
		queryCode.append("\n  queryview := queryview.evaluate;\n");

	}
	
	private String translateSlicePartOfCurrentQueryLevel(SQLMNode currentSubQueryRootNode){
		StringBuffer sb = new StringBuffer();
		HashMap<Integer, String> usedDimensions = this.getUsedDimensionOfCurrentQueryLevel(currentSubQueryRootNode);
		if(currentSubQueryRootNode.jjtGetChild(ASTSliceBlock.class) != null){
			SQLMNode sliceNode = currentSubQueryRootNode.jjtGetChild(ASTSliceBlock.class);
			for(int h=0;h<dimensionSequenceList.size();h++){
				String searchedDimension = dimensionSequenceList.get(h);
				this.affixOfCurrentlyTranslatedDimension = h;
				LinkedList<String> alreadyTranslatedLevels = new LinkedList<String>();
				//run through all slice expressions
				for(int i=0;i<sliceNode.jjtGetNumChildren();i++){
					SQLMNode sliceExpressionPath = sliceNode.jjtGetChild(i).jjtGetChild(ASTSliceExpressionPath.class);
					String currentDimension = (String)(sliceExpressionPath.jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue();
					//translation is sorted by dimensions
					if(searchedDimension.equals(currentDimension)){
						String currentLevel = (String)(sliceExpressionPath.jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue();
						//translation is sorted by mobject-levels
						if(!alreadyTranslatedLevels.contains(currentLevel)){
							alreadyTranslatedLevels.add(currentLevel);
							sb.append(this.translateSlicePredicates(sliceNode, currentDimension, currentLevel));
							sb.append(this.updateQueryview(usedDimensions, currentDimension));
						}
					}
				}
			}
		}
		return sb.toString();
	}
	
	
	private String translateSlicePredicates(SQLMNode slicePartRootNode, String currentDimension, String currentLevel){
		StringBuffer sb = new StringBuffer();
		SQLMNode currentSlicePredicate;
		boolean variablesDeclared = false;
		for(int j=0;j<slicePartRootNode.jjtGetNumChildren();j++){
			currentSlicePredicate = slicePartRootNode.jjtGetChild(j);
			if( ((String)(currentSlicePredicate.jjtGetChild(0).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue()).equals(currentDimension) && 
					((String)(currentSlicePredicate.jjtGetChild(0).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue()).equals(currentLevel) ){
				if(!variablesDeclared){
					sb.append(declareVariablesForSlicePredicateTranslation(currentSlicePredicate));
					variablesDeclared = true;
				}
				if(currentSlicePredicate.jjtGetChild(ASTAttributeUnit.class) != null){
					if(currentSlicePredicate.jjtGetChild(ASTAttributeUnit.class).jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){
						sb.append(declareReferencesForMObjectAsAtrributeUnits(currentSlicePredicate));
					}
					sb.append(declareMCubeReferenceForAttributeUnit(currentSlicePredicate));
				}
				if(currentSlicePredicate.jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){
					sb.append(declareReferencesForMObjectAsValue(currentSlicePredicate));
				}
				sb.append(addSliceExpressionToSlicePredicate(currentSlicePredicate));
			}
		}
		return sb.toString();
	}
	
	private String declareVariablesForSlicePredicateTranslation(SQLMNode currentSlicePredicate){
		StringBuffer sb = new StringBuffer();
		sb.append("\n  SELECT REF(d) INTO dim_ref\n  FROM dimensions d\n  WHERE d.dname = '");
		//DimensionHry
		sb.append( (String)(currentSlicePredicate.jjtGetChild(0).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue() ); 
		sb.append("';\n  pred_"); 
		sb.append(affixOfCurrentlyTranslatedDimension);
		sb.append(":= slice_predicate_ty(dim_ref, '");
		//MObjectLevel
		sb.append( (String)(currentSlicePredicate.jjtGetChild(0).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue() ); 
		sb.append("');");
		return sb.toString();
	}
	
	private String declareReferencesForMObjectAsAtrributeUnits(SQLMNode currentSlicePredicate){
		StringBuffer sb = new StringBuffer();
		//get dimension ref
		sb.append("\n  SELECT VALUE(d) INTO attr_dim");
		sb.append(mObjectAttributeUnitAffix);
		sb.append(" \n  FROM dimensions d \n  WHERE  d.dname = '");
		//dimension hierarchy
		sb.append( (String)(currentSlicePredicate.jjtGetChild(ASTAttributeUnit.class).jjtGetChild(0).jjtGetChild(1)).jjtGetValue() ); 
		sb.append("';");
		//get m-object ref
		sb.append("\n  attr_unit");
		sb.append(mObjectAttributeUnitAffix);
		sb.append("_ref := attr_dim");
		sb.append(mObjectAttributeUnitAffix);
		sb.append(".get_mobject_ref('");
		//m-object name
		sb.append( (String)(currentSlicePredicate.jjtGetChild(ASTAttributeUnit.class).jjtGetChild(0).jjtGetChild(0)).jjtGetValue() );
		sb.append("');");
		return sb.toString();
	}
	
	private String declareMCubeReferenceForAttributeUnit(SQLMNode currentSlicePredicate){
		StringBuffer sb = new StringBuffer();
		//get cube reference
		//sb.append("\n  SELECT VALUE(mc) INTO attr_units");
		sb.append("\n  SELECT REF(mc) INTO attr_units");
		sb.append(mCubeAffix);
		sb.append("_cube_ref \n  FROM mcubes mc \n  WHERE  mc.cname = '");
		sb.append( (String)(currentSlicePredicate.jjtGetChild(ASTAttributeUnitConversionRule.class).jjtGetChild(0)).jjtGetValue() );
		sb.append("';");
		return sb.toString();
	}
	
	private String declareReferencesForMObjectAsValue(SQLMNode currentSlicePredicate){
		StringBuffer sb = new StringBuffer();
		//get dimension reference
		sb.append("\n  SELECT VALUE(d) INTO value_dim");
		sb.append(mObjectValueAffix);
		sb.append(" \n  FROM dimensions d \n  WHERE d.dname = '");
		//dimension hierarchy name
		sb.append( (String)(currentSlicePredicate.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(1)).jjtGetValue() ); 
		sb.append("';");
		return sb.toString();
	}
	
	private String addSliceExpressionToSlicePredicate(SQLMNode currentSlicePredicate){
		StringBuffer sb = new StringBuffer();
		sb.append(createSliceExpression(currentSlicePredicate));
		sb.append(addSliceConversionExpression(currentSlicePredicate));
		sb.append(addMathSign(currentSlicePredicate));
		sb.append(addAttributeValue(currentSlicePredicate));
		return sb.toString();
	}
	
	private String createSliceExpression(SQLMNode currentSlicePredicate){
		StringBuffer sb = new StringBuffer();
		sb.append("\n  pred_");
		sb.append(affixOfCurrentlyTranslatedDimension);
		sb.append(".add_expression('");
		//MObjAttributeName
		sb.append( (String)(currentSlicePredicate.jjtGetChild(0).jjtGetChild(ASTMultilevelObjectAttributeID.class)).jjtGetValue() ); 
		sb.append("', " );
		return sb.toString();
	}
	
	private String addSliceConversionExpression(SQLMNode currentSlicePredicate){
		StringBuffer sb = new StringBuffer();
		if(currentSlicePredicate.jjtGetChild(ASTAttributeUnit.class) != null){
			//add attribute unit
			if(currentSlicePredicate.jjtGetChild(ASTAttributeUnit.class).jjtGetChild(ASTNumberValue.class) != null){
				sb.append("ANYDATA.convertNumber(");
				sb.append((String)(currentSlicePredicate.jjtGetChild(ASTNumberValue.class)).jjtGetValue());
			}else{
				if(currentSlicePredicate.jjtGetChild(ASTAttributeUnit.class).jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){
					//sb.append("', ANYDATA.convertRef(attr_unit");
					sb.append("ANYDATA.convertRef(attr_unit");
					sb.append(mObjectAttributeUnitAffix);
					sb.append("_ref");
					mObjectAttributeUnitAffix++;
				}else{
					sb.append("ANYDATA.convertVarchar2(");
					sb.append((String)(currentSlicePredicate.jjtGetChild(ASTStringValue.class)).jjtGetValue());
				}
			}
			sb.append("), " );
			//add attribute conversion rule
			sb.append(" ANYDATA.convertRef(attr_units");
			sb.append(mCubeAffix);
			//sb.append("_cube_ref, ");
			sb.append("_cube_ref), ");
			mCubeAffix++;
		}
		return sb.toString();
	}
	
	private String addMathSign(SQLMNode currentSlicePredicate){
		StringBuffer sb = new StringBuffer();
		sb.append("'" );
		sb.append( (String)(currentSlicePredicate.jjtGetChild(ASTComparisonOperator.class)).jjtGetValue() ); 
		return sb.toString();
	}
	
	private String addAttributeValue(SQLMNode currentSlicePredicate){
		StringBuffer sb = new StringBuffer();
		if(currentSlicePredicate.jjtGetChild(ASTNumberValue.class) != null){
			sb.append("', ANYDATA.convertNumber(");
			sb.append((String)(currentSlicePredicate.jjtGetChild(ASTNumberValue.class)).jjtGetValue());
		}else{//m-object value
			if(currentSlicePredicate.jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){
				sb.append("', ANYDATA.convertRef(value_dim");
				sb.append(mObjectValueAffix);
				sb.append(".get_mobject_ref('");
				sb.append( (String)(currentSlicePredicate.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(0)).jjtGetValue() );
				sb.append("')");
				//sb.append("', ANYDATA.convertRef(value_unit");
				//sb.append(mObjectValueCount);
				//sb.append("_ref");
				mObjectValueAffix++;
			}else{//String value
				if(currentSlicePredicate.jjtGetChild(ASTStringValue.class) != null){
					sb.append("', ANYDATA.convertVarchar2(");
					if(currentSlicePredicate.jjtGetChild(ASTAggregationFunction.class) != null){ //no need?
						sb.append("'");
					}
					sb.append((String)(currentSlicePredicate.jjtGetChild(ASTStringValue.class)).jjtGetValue());
					if(currentSlicePredicate.jjtGetChild(ASTAggregationFunction.class) != null){ // also no need?
						sb.append("'");
					}
				}else{ //ObjectCollectionConstructor value
					if(currentSlicePredicate.jjtGetChild(ASTObjectCollectionConstructor.class) != null){
						
					}else{ //ObjectConstructor value
						
					}
				}
			}
		}
		sb.append("));");
		return sb.toString();
	}
	
	private String updateQueryview(HashMap<Integer, String> usedDimensions, String currentDimension){
		StringBuffer sb = new StringBuffer();
		sb.append("\n\n  queryview := queryview.slice(");
		boolean match = false; 
		for(int k=0;k<dimensionSequenceList.size(); k++){
			match = false; 
			//abgleichen der Reihenfolge - wenn der richtige Wert für die n-Position gefunden ist, wird er eingefügt
			if(usedDimensions.containsValue(dimensionSequenceList.get(k))){
				if(dimensionSequenceList.get(k).equals(currentDimension)){
					match = true;
					sb.append("pred_");
					sb.append(k);
				}
			}
			if(!match){
				sb.append("NULL");
			}
			if(k+1 < dimensionSequenceList.size()){
				sb.append(", ");
			}
		}
		sb.append(");");
		return sb.toString();
	}
	
	private String translateDicePartOfCurrentQueryLevel(SQLMNode currentSubQueryRootNode) {
		StringBuffer sb = new StringBuffer();
		dataDict = DataDictionary.getDataDictionary();
		if(currentSubQueryRootNode.jjtGetChild(ASTDiceBlock.class) != null ){
			sb.append("\n  queryview := queryview.dice(");
			SQLMNode mObjectIdList = currentSubQueryRootNode.jjtGetChild(ASTDiceBlock.class)
										.jjtGetChild(ASTDiceMultilevelObjectIDList.class);
			if(mObjectIdList.jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){ //QualifiedID = Überprüfung
				//Reihenfolge muss nicht extern geholt werden da schon vor dem Aufruf dieser Methode gemacht
				//DimObj werden in der richtigen Reihefolge eingefügt
				for(int hashMapKey=1;hashMapKey<=dataDict.getNumberOfValuesOfSequence(mcubeName); hashMapKey++){ //Werte der HashMap
					for(int i=0;i<mObjectIdList.jjtGetNumChildren();i++){ //Werte des AST
						//abgleichen der Reihenfolge - wenn der richtige Wert für die n-Position gefunden ist, wird er eingefügt
						if(((String)(mObjectIdList.jjtGetChild(i).jjtGetChild(1)).jjtGetValue()).equals((dataDict.getSequence(mcubeName)).get(hashMapKey))){
							sb.append("'");
							sb.append((String)(mObjectIdList.jjtGetChild(i).jjtGetChild(0)).jjtGetValue());
							sb.append("'");
							if(hashMapKey < dataDict.getNumberOfValuesOfSequence(mcubeName)){
								sb.append(", ");
							}
						}
					}
				}
			}else{ //unqualified ID = keine Überprüfung
				for(int i=0;i<mObjectIdList.jjtGetNumChildren();i++){
					sb.append("'");
					sb.append((String)(mObjectIdList.jjtGetChild(i)).jjtGetValue());
					sb.append("'");
					if(i+1<mObjectIdList.jjtGetNumChildren()){
						sb.append(", ");
					}
				}
			}
			sb.append(");");
		}
		return sb.toString();
	}
	

	private String translateProjectionPartOfCurrentQueryLevel(SQLMNode currentSubQueryRootNode) {
		StringBuffer sb = new StringBuffer();
		SQLMNode projectionBlock = currentSubQueryRootNode.jjtGetChild(ASTProjectionBlock.class);
		if(projectionBlock.jjtGetNumChildren()>0){
			sb.append("\n  queryview := queryview.project(names_tty(");
			for(int i=0;i<projectionBlock.jjtGetNumChildren();i++){
				if(projectionBlock.jjtGetChild(i)instanceof ASTMultilevelFactMeasureID){  
					sb.append("'");
					sb.append((String)(projectionBlock.jjtGetChild(i)).jjtGetValue());
					sb.append("'");
					if(i+1<projectionBlock.jjtGetNumChildren()){
						sb.append(", ");
					}
				}
			}
			sb.append("));");
		}
		return sb.toString();
	}
	
	private HashMap<Integer, String> getUsedDimensionOfCurrentQueryLevel(SQLMNode currentSubQueryRootNode){
		HashMap<Integer, String> usedDimensions = new HashMap<Integer, String>();
		//saving the used dimensions in the slice-part for auto-generating needed slice predicate objects
		if(currentSubQueryRootNode.jjtGetChild(ASTSliceBlock.class) != null){
			SQLMNode sliceBlock = currentSubQueryRootNode.jjtGetChild(ASTSliceBlock.class);
			int key = 0;
			for(int i=0;i<sliceBlock.jjtGetNumChildren();i++){
				String dimensionHierarchy = (String)(sliceBlock.jjtGetChild(i).jjtGetChild(0).jjtGetChild(1)).jjtGetValue();
				if(!usedDimensions.containsValue(dimensionHierarchy)){
					usedDimensions.put(key, dimensionHierarchy);
					key++;
				}
			}
		}
		return usedDimensions;
	}
	
}
