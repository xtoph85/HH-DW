package at.jku.dke.sqlm.translator;

import java.sql.SQLException;
import java.util.HashMap;

import at.jku.dke.sqlm.interpreter.DataDictionary;
import at.jku.dke.sqlm.parser.ASTAggregationFunction;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeCoordinate;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactMeasureValueAssignment;
import at.jku.dke.sqlm.parser.ASTMultilevelFactMeasureValueBlock;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.ASTNumberValue;
import at.jku.dke.sqlm.parser.ASTObjectCollectionConstructor;
import at.jku.dke.sqlm.parser.ASTObjectConstructor;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will update multiple Multilevel Facts
 * @param sn is the root SQLMNode(sn) of the BULK UPDATE MULTILEVEL FACT AST
 * @throws SQLException 
 */
public class BulkUpdateMultilevelFact extends Statement{

	StringBuffer translation;
	SQLMNode rootNode;
	SQLMNode measure;
	String mcubeId;
	String mcubeType;
	String valueTY;
	String valueTTY;
	DataDictionary dataDict;
	HashMap<String, String> mObjValueDim = new HashMap<String, String>();
	
	public BulkUpdateMultilevelFact(SQLMNode sn){
		this.rootNode = sn;
	}
	
	public String translate() throws SQLException{
		translation = new StringBuffer();
		this.getDataFromDictionary();
		this.createNeededVariables();
		this.setMultilevelObjectsReferences();
		this.specifyMultilevelFactBulkUpdate();
		return translation.toString();
	}

	private void getDataFromDictionary() throws SQLException {
		dataDict = DataDictionary.getDataDictionary();
		mcubeId = (String)(rootNode.jjtGetChild(0).jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeID.class)).jjtGetValue();
		mcubeType = dataDict.getMCubeType(mcubeId);
		valueTY = dataDict.getMFactValueType(mcubeId);
		valueTTY = valueTY.substring(0, valueTY.length()-1) + "ty";
	}

	private void createNeededVariables() {
		translation.append("DECLARE \n  m_cube ");
		translation.append(mcubeType);
		translation.append(";\n ");
		//get and declare value_dim for m-objects as value
		int count = 0;
		for(int i=0;i<rootNode.jjtGetNumChildren();i++){
			measure = rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelFactMeasureValueBlock.class).jjtGetChild(ASTMultilevelFactMeasureValueAssignment.class);
			if(measure.jjtGetChild(1) instanceof ASTMultilevelObjectQualifiedID){
				String value = "value_dim"+count;
				String key = (String)(measure.jjtGetChild(1).jjtGetChild(1)).jjtGetValue();
				mObjValueDim.put(key, value);
				translation.append("  ");
				translation.append(value);
				translation.append(" dimension_ty;\n");
			}
		}
	}

	private void setMultilevelObjectsReferences() {
		translation.append("BEGIN \n  SELECT TREAT ( VALUE (mc) AS ");
		translation.append(mcubeType);
		translation.append(")\n  INTO m_cube \n  FROM mcubes mc \n  WHERE mc.cname = '");
		translation.append(mcubeId);
		translation.append("';");
		//get dimension ref for m-objects as value
		for(int i=0;i<rootNode.jjtGetNumChildren();i++){
			measure = rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelFactMeasureValueBlock.class).jjtGetChild(ASTMultilevelFactMeasureValueAssignment.class);
			//get dimension reference for m-object as value
			if(measure.jjtGetChild(1) instanceof ASTMultilevelObjectQualifiedID){
				String dimName = (String)(measure.jjtGetChild(1).jjtGetChild(1)).jjtGetValue();
				translation.append("  SELECT VALUE (dim) INTO ");
				translation.append(mObjValueDim.get(dimName));
				translation.append("\n  FROM dimensions dim \n  WHERE dim.dname = '");
				translation.append(dimName);
				translation.append("'; \n");
			}
		}
		translation.append("\n  m_cube.bulk_set_measure('");
		translation.append( (String)(rootNode.jjtGetChild(0).jjtGetChild(ASTMultilevelFactMeasureValueBlock.class).jjtGetChild(0).jjtGetChild(0)).jjtGetValue() );
		translation.append("', ");
		translation.append(valueTTY);
		translation.append("(");
	}

	private void specifyMultilevelFactBulkUpdate() throws SQLException {
		SQLMNode cubeCoordinate;
		for(int i=0;i<rootNode.jjtGetNumChildren();i++){
			cubeCoordinate = rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeCoordinate.class);
			translation.append(valueTY);
			translation.append("('");
			//get the Coordinate ID's for the MLFact
			//Qualified ID
			if(cubeCoordinate.jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){ 
				translation.append(dataDict.getMCubeSequence(rootNode.jjtGetChild(i)));
			}else{ //UnQualified ID
				for(int j=0;j<cubeCoordinate.jjtGetNumChildren();j++){
					translation.append((String)(cubeCoordinate.jjtGetChild(j)).jjtGetValue());
					if(j+1<cubeCoordinate.jjtGetNumChildren()){
						translation.append("', '");
					}
				}
			}
			translation.append("', ");
			//current measure
			measure = rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelFactMeasureValueBlock.class).jjtGetChild(ASTMultilevelFactMeasureValueAssignment.class);
			//get the value of the measure
			if(measure.jjtGetChild(1) instanceof ASTNumberValue){
				translation.append("ANYDATA.convertNumber(");
				translation.append((String)(measure.jjtGetChild(1)).jjtGetValue());
			}else{
				if(measure.jjtGetChild(1) instanceof ASTMultilevelObjectQualifiedID){
					String dimName = (String)(measure.jjtGetChild(1).jjtGetChild(1)).jjtGetValue();
					translation.append("ANYDATA.convertRef(");
					translation.append(mObjValueDim.get(dimName));
					translation.append(".get_mobject_ref('");
					translation.append((String)(measure.jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
				}else{
					if(measure.jjtGetChild(1) instanceof ASTObjectConstructor){
						translation.append("ANYDATA.convertObject(");
						translation.append((String)(measure.jjtGetChild(1)).jjtGetValue());
					}else{
						if(measure.jjtGetChild(1) instanceof ASTObjectCollectionConstructor){
							translation.append("ANYDATA.convertCollection(");
							translation.append((String)(measure.jjtGetChild(1)).jjtGetValue());
						}else{
							translation.append("ANYDATA.convertVarchar2(");
							if(measure.jjtGetChild(1) instanceof ASTAggregationFunction){
								translation.append("'");
							}
							translation.append((String)(measure.jjtGetChild(1)).jjtGetValue());
						}
					}
				}
			}
			if(measure.jjtGetChild(1) instanceof ASTAggregationFunction){
				translation.append("'");
			}
			if(measure.jjtGetChild(1) instanceof ASTMultilevelObjectQualifiedID){
				translation.append("')");
			}
			
			translation.append("))");
			if(i+1<rootNode.jjtGetNumChildren()){
				translation.append(", \n\t\t\t\t");
			}
			//during the run through check for the highest end-line
			if((rootNode.jjtGetChild(i)).jjtGetErrorLine() > rootNode.jjtGetErrorLine()){
				rootNode.jjtSetErrorLine((rootNode.jjtGetChild(i)).jjtGetErrorLine());
			}
		}
		translation.append(")); \n");
		translation.append("END;\n\n");
	}

}
