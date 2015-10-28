package at.jku.dke.sqlm.translator;

import java.sql.SQLException;
import java.util.ArrayList;

import at.jku.dke.sqlm.interpreter.DataDictionary;
import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMetalevelID;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeCoordinate;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactConnectionLevelHierarchy;
import at.jku.dke.sqlm.parser.ASTMultilevelFactID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactMeasureValueBlock;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.SQLMNode;



/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will generate a Multilevel Fact
 * @param sn is the root SQLMNode of the CREATE MULTILEVEL FACT AST
 */
public class CreateMultilevelFact extends Statement{

	
	private StringBuffer translation;
	private SQLMNode rootNode;
	private boolean mobjectAsValue;
	private DataDictionary dataDict;
	
	private ArrayList<Integer> order;
	private String mcubeId;
	private String mcubeType;
	private String conlevelType;
	
	public CreateMultilevelFact(SQLMNode sn){
		this.rootNode = sn;
		this.mobjectAsValue = super.existMObjectAsValues(sn);
	}
	
	public String translate() throws SQLException{
		translation = new StringBuffer();
		dataDict = DataDictionary.getDataDictionary();
		order = new ArrayList<Integer>();
		
		this.createNeededVariables();
		this.getMCubeReference();
		this.translateMeasureInformation();
		this.createClosing();
		
		return translation.toString();
	}	
	
	private void createNeededVariables() throws SQLException{
		order = new ArrayList<Integer>();
		mcubeId = (String)(rootNode.jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeID.class)).jjtGetValue();
		mcubeType = dataDict.getMCubeType(mcubeId);
		conlevelType = dataDict.getConlevelType(mcubeId);
		translation.append("DECLARE \n");
		translation.append("  m_cube_ref REF ");
		translation.append(mcubeType);
		translation.append(";\n  m_cube ");
		translation.append(mcubeType);
		translation.append(";\n  mrel_ref REF ");
		translation.append(dataDict.getMRelType(mcubeId));
		translation.append(";\n  mrel ");
		translation.append(dataDict.getMRelType(mcubeId));
		translation.append(";\n  conlvl ");
		translation.append(dataDict.getConlevelType(mcubeId));
		translation.append(";\n");
		//declaration for m-object values
		if(mobjectAsValue){
			declareDimensionTypeForUnitConversion();
			//translation.append("  value_dim dimension_ty;\n");
		}
	}
	
	private void getMCubeReference() throws SQLException{
		translation.append("BEGIN \n  SELECT TREAT(REF(mc) AS REF ");
		translation.append(mcubeType);
		translation.append(")\n  INTO m_cube_ref \n  FROM mcubes mc \n  WHERE mc.cname = '");
		translation.append(mcubeId);
		translation.append("'; \n");
		translation.append("  utl_ref.select_object(m_cube_ref, m_cube);\n");
		translation.append("  mrel_ref := m_cube.create_mrel('");
		
		SQLMNode cubeCoordinate = rootNode.jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeCoordinate.class);
		//check if the sequence of the cube coordinate must be verified 
		//QualifiedID = verification
		if(cubeCoordinate.jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){
			translation.append(dataDict.getMCubeSequence(rootNode));
			translation.append("'); \n");
			order = dataDict.getMCubeOrder(rootNode);
		}else{ //UnQualifiedID = no verification
			for(int i=0;i<cubeCoordinate.jjtGetNumChildren();i++){
				order.add(i);
				translation.append((String)(cubeCoordinate.jjtGetChild(i)).jjtGetValue());
				if(i+1 == cubeCoordinate.jjtGetNumChildren()){
					translation.append("');");
				}else{
					translation.append("', '");
				}
			}
		}
	}
	
	private void declareDimensionTypeForUnitConversion() throws SQLException{
		SQLMNode measureBlock = rootNode.jjtGetChild(ASTMultilevelFactMeasureValueBlock.class);
		if(measureBlock != null){
			for(int i=0;i<measureBlock.jjtGetNumChildren();i++){
				SQLMNode measureValueAssignment = measureBlock.jjtGetChild(i);
				int valuePos = 1;
				if(measureValueAssignment.jjtGetChild(ASTMetalevelID.class) != null){
					valuePos++;
				}
				if(measureValueAssignment.jjtGetChild(valuePos) instanceof ASTMultilevelObjectQualifiedID){
					DataDictionary dataDict = DataDictionary.getDataDictionary();
					String dimensionType = dataDict.getDimensionHierarchyId((String)(measureValueAssignment.jjtGetChild(valuePos).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
					//ref-type declaration
					translation.append("  ref_value_dim_");
					translation.append(dimensionType);
					translation.append(" REF dimension_");
					translation.append(dimensionType);
					translation.append("_ty;\n");
					//type declaration
					translation.append("  value_dim_");
					translation.append(dimensionType);
					translation.append(" dimension_");
					translation.append(dimensionType);
					translation.append("_ty;\n");
				}
			}
		}
	}
	
	private void translateMeasureInformation() throws SQLException{
		//add/set measures
		if(rootNode.jjtGetChild(ASTMultilevelFactConnectionLevelHierarchy.class) != null ||  
				rootNode.jjtGetChild(ASTMultilevelFactMeasureValueBlock.class) != null){
			MultilevelFactMeasure measure = new MultilevelFactMeasure(rootNode);
			String measureCode = measure.translate(order, conlevelType);
			if(measureCode.length() > 0){
				translation.append(" \n  utl_ref.select_object(mrel_ref, mrel); \n");
				translation.append(measureCode);
			}
		}
	}
	
	private void createClosing(){
		translation.append("END; \n\n");
	}
	
}
