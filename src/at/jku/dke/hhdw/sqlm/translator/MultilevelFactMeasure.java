package at.jku.dke.sqlm.translator;

import java.sql.SQLException;
import java.util.ArrayList;

import at.jku.dke.sqlm.interpreter.DataDictionary;
import at.jku.dke.sqlm.parser.ASTAggregationFunction;
import at.jku.dke.sqlm.parser.ASTDefault;
import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMetalevelID;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactConnectionLevelHierarchy;
import at.jku.dke.sqlm.parser.ASTMultilevelFactConnectionLevelMeasure;
import at.jku.dke.sqlm.parser.ASTMultilevelFactID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactMeasureValueBlock;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.ASTNumberType;
import at.jku.dke.sqlm.parser.ASTNumberValue;
import at.jku.dke.sqlm.parser.ASTObjectCollectionConstructor;
import at.jku.dke.sqlm.parser.ASTObjectConstructor;
import at.jku.dke.sqlm.parser.ASTVarchar2Type;
import at.jku.dke.sqlm.parser.SQLMNode;

public class MultilevelFactMeasure {

	private StringBuffer translation;
	private SQLMNode measures;
	private String dimensionType;
	private String mcubeName;
	private String measureName;
	private DataDictionary dataDict;
	
	/**
	 * Provides the translation for add and set measure code
	 * @param sn is the root SQLMNode(sn) of a statement which may contain a ADD and SET MEASURE AST
	 * @throws SQLException 
	 */
	public MultilevelFactMeasure(SQLMNode sn){
		this.measures = sn;
		this.mcubeName = (String)measures.jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeID.class).jjtGetValue();
	}
	
	public String translate(ArrayList<Integer> cubeCoordinateOrder, String conlevelType) throws SQLException{
		translation = new StringBuffer();
		translateAddMeasure(cubeCoordinateOrder, conlevelType);
		translateSetMeasure();
		return translation.toString();
	}
	
	public String translateOnlyAddMeasure(ArrayList<Integer> cubeCoordinateOrder, String conlevelType){
		translation = new StringBuffer();
		translateAddMeasure(cubeCoordinateOrder, conlevelType);
		return translation.toString();
	}
	
	public String translateOnlySetMeasure() throws SQLException{
		translation = new StringBuffer();
		translateSetMeasure();
		return translation.toString();
	}
	
	/**
	 * Provides the translation for add measure code
	 * @param sn is the root SQLMNode(sn) of a statement which may contain a ADD MEASURE AST 
	 */
	private void translateAddMeasure(ArrayList<Integer> cubeCoordinateOrder, String conlevelType){
		if(measures.jjtGetChild(ASTMultilevelFactConnectionLevelHierarchy.class) != null ){
			SQLMNode measureList = measures.jjtGetChild(ASTMultilevelFactConnectionLevelHierarchy.class);
			//run through the MultilevelFactConnectionLevelDefinitions
			for(int i=0; i<measureList.jjtGetNumChildren();i++){ 
				if(measureList.jjtGetChild(i).jjtGetChild(ASTMultilevelFactConnectionLevelMeasure.class) != null){
					for(int j=1; j<measureList.jjtGetChild(i).jjtGetNumChildren();j++){
						translation.append("  mrel.add_measure('");
						//measure name
						translation.append((String)(measureList.jjtGetChild(i).jjtGetChild(j).jjtGetChild(0)).jjtGetValue());
						translation.append("', "); 
						translation.append(conlevelType); 
						translation.append("('");
						int count = 1;
						for(int k : cubeCoordinateOrder){
							translation.append((String)(measureList.jjtGetChild(i).jjtGetChild(0).jjtGetChild(k)).jjtGetValue());
							if(count<cubeCoordinateOrder.size()){
								translation.append("', '");
							}
							count++;
						}
						translation.append("'), '");
						//measure type
						translation.append((String)(measureList.jjtGetChild(i).jjtGetChild(j).jjtGetChild(1)).jjtGetValue());
						//add additional information of the value
						if(measureList.jjtGetChild(i).jjtGetChild(j).jjtGetChild(1) instanceof ASTVarchar2Type){
							translation.append("(");
							translation.append((String)(measureList.jjtGetChild(i).jjtGetChild(j).jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
							translation.append(")");
						}
						if(measureList.jjtGetChild(i).jjtGetChild(j).jjtGetChild(1) instanceof ASTNumberType){
							//add DataLength if existing
							if(measureList.jjtGetChild(i).jjtGetChild(j).jjtGetChild(1).jjtGetNumChildren() > 0){
								translation.append("(");
								translation.append((String)(measureList.jjtGetChild(i).jjtGetChild(j).jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
							}
							//add DataScale if existing
							if(measureList.jjtGetChild(i).jjtGetChild(j).jjtGetChild(1).jjtGetNumChildren() > 1){
								translation.append(", ");
								translation.append((String)(measureList.jjtGetChild(i).jjtGetChild(j).jjtGetChild(1).jjtGetChild(1)).jjtGetValue());
							}
							if(measureList.jjtGetChild(i).jjtGetChild(j).jjtGetChild(1).jjtGetNumChildren() > 0){
								translation.append(")");
							}
						}
						translation.append("'); \n");
					}
				}
			}
		}
	}
	
	/**
	 * Provides the translation for set measure code
	 * @param sn is the root SQLMNode(sn) of a statement which may contain a SET MEASURE AST 
	 * @throws SQLException 
	 */
	private void translateSetMeasure() throws SQLException{
		SQLMNode measureBlock = measures.jjtGetChild(ASTMultilevelFactMeasureValueBlock.class);
		if(measureBlock != null){
			for(int i=0;i<measureBlock.jjtGetNumChildren();i++){
				SQLMNode measure = measureBlock.jjtGetChild(i);
				this.measureName = (String)(measure.jjtGetChild(0)).jjtGetValue();
				dataDict = DataDictionary.getDataDictionary();
				dataDict.setUpdatedCube(mcubeName, measureName);
				int valuePos = 1;
				if(measure.jjtGetChild(ASTMetalevelID.class) != null){
					valuePos++;
				}
				if(measure.jjtGetChild(valuePos) instanceof ASTMultilevelObjectQualifiedID){
					//get dimension reference for m-object as value
					String dimensionName = (String)(measure.jjtGetChild(valuePos).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue();
					//translation.append("  SELECT VALUE (dim) INTO value_dim \n  FROM dimensions dim \n  WHERE dim.dname = '");
					//translation.append(dimensionName);
					//translation.append("'; \n");
					dataDict = DataDictionary.getDataDictionary();
					dimensionType = dataDict.getDimensionHierarchyId(dimensionName);
					
					translation.append("  SELECT TREAT(REF(dim) AS REF dimension_");
					translation.append(dimensionType);
					translation.append("_ty) INTO ref_value_dim_");
					translation.append(dimensionType);
					translation.append("\n  FROM dimensions dim \n  WHERE dim.dname = '");
					translation.append(dimensionName);
					translation.append("'; \n");
					
					translation.append("  utl_ref.select_object(");
					translation.append("ref_value_dim_");
					translation.append(dimensionType);
					translation.append(", ");
					translation.append("value_dim_");
					translation.append(dimensionType);
					translation.append("); \n");
				}
				translation.append("  mrel.set_measure('");
				//measure name
				translation.append(measureName);
				translation.append("', ");
				translateMetaLevel(measure);
				
				/**
				//measure meta-level
				if(measure.jjtGetChild(ASTMetalevelID.class) != null){
					translation.append("'");
					translation.append((String)(measure.jjtGetChild(1)).jjtGetValue());
					translation.append("'");
				}else{
					translation.append("null");
				}
				//measure DEFAULT/SHARED value
				if(measure.jjtGetChild(ASTDefault.class) != null ){
					translation.append(", TRUE, ");
				}else{
					translation.append(", FALSE, ");
				}
				**/
				if(measure.jjtGetChild(valuePos) instanceof ASTNumberValue){
					translation.append("ANYDATA.convertNumber(");
					translation.append((String)(measure.jjtGetChild(valuePos)).jjtGetValue());
				}else{
					if(measure.jjtGetChild(valuePos) instanceof ASTMultilevelObjectQualifiedID){
						//translation.append("ANYDATA.convertRef(value_dim.get_mobject_ref('");
						translation.append("ANYDATA.convertRef(");
						translation.append("value_dim_");
						translation.append(dimensionType);
						translation.append(".get_mobject_ref('");
						translation.append((String)(measure.jjtGetChild(valuePos).jjtGetChild(0)).jjtGetValue());
					}else{
						if(measure.jjtGetChild(valuePos) instanceof ASTObjectConstructor){
							translation.append("ANYDATA.convertObject(");
							translation.append((String)(measure.jjtGetChild(valuePos)).jjtGetValue());
						}else{
							if(measure.jjtGetChild(valuePos) instanceof ASTObjectCollectionConstructor){
								translation.append("ANYDATA.convertCollection(");
								translation.append((String)(measure.jjtGetChild(valuePos)).jjtGetValue());
							}else{
								translation.append("ANYDATA.convertVarchar2(");
								if(measure.jjtGetChild(valuePos) instanceof ASTAggregationFunction){
									translation.append("'");
								}
								translation.append((String)(measure.jjtGetChild(valuePos)).jjtGetValue());
							}
						}
					}
				}
				if(measure.jjtGetChild(valuePos) instanceof ASTAggregationFunction){
					translation.append("'");
				}
				if(measure.jjtGetChild(valuePos) instanceof ASTMultilevelObjectQualifiedID){
					translation.append("')");
				}
				translation.append("));\n");
			}
		}
	}
	
	private void translateMetaLevel(SQLMNode measure){
		if(!(measure.jjtGetChild(ASTMetalevelID.class) == null && measure.jjtGetChild(ASTDefault.class) == null)){
			//measure meta-level
			if(measure.jjtGetChild(ASTMetalevelID.class) != null){
				translation.append("'");
				translation.append((String)(measure.jjtGetChild(1)).jjtGetValue());
				translation.append("'");
			}else{
				translation.append("null");
			}
			//measure DEFAULT/SHARED value
			if(measure.jjtGetChild(ASTDefault.class) != null ){
				translation.append(", TRUE, ");
			}else{
				translation.append(", FALSE, ");
			}
		}
	}
	
	
}
