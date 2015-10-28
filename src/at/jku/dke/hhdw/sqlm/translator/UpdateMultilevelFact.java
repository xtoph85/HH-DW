package at.jku.dke.sqlm.translator;

import java.sql.SQLException;

import at.jku.dke.sqlm.interpreter.DataDictionary;
import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMetalevelID;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeCoordinate;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactMeasureValueBlock;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will update a Multilevel Fact
 * @param sn is the root SQLMNode of the UPDATE MULTILEVEL FACT AST
 * @throws SQLException 
 */
public class UpdateMultilevelFact extends Statement{

	
	private StringBuffer translation;
	private SQLMNode rootNode;
	private boolean mobjectAsValue;
	private DataDictionary dataDict;
	
	private String mcubeId;
	private String mcubeType;
	
	public UpdateMultilevelFact(SQLMNode sn){
		this.rootNode = sn;
		this.mobjectAsValue = super.existMObjectAsValues(sn);
	}
	
	public String translate() throws SQLException{
		translation = new StringBuffer();
		dataDict = DataDictionary.getDataDictionary();
		
		this.createNeededVariables();
		this.getMCubeReference();
		this.translateSetMultilevelObjectAttributes();
		this.createClosing();
		
		return translation.toString();
	}

	private void createNeededVariables() throws SQLException{
		dataDict = DataDictionary.getDataDictionary();
		mcubeId = (String)(rootNode.jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeID.class)).jjtGetValue();
		mcubeType = dataDict.getMCubeType(mcubeId);
		String mrelType = dataDict.getMRelType(mcubeId);
		translation.append("DECLARE \n");
		translation.append("  m_cube_ref REF ");
		translation.append(mcubeType);
		translation.append(";\n  m_cube ");
		translation.append(mcubeType);
		translation.append(";\n  mrel_ref REF ");
		translation.append(mrelType);
		translation.append(";\n  mrel ");
		translation.append(mrelType);
		translation.append(";\n");
		
		//declaration for m-object values
		if(mobjectAsValue){
			declareDimensionTypeForUnitConversion();
		}
		
		/**
		translation.append("DECLARE \n  m_cube ");
		translation.append(mcubeType);
		translation.append(";\n  mrel_ref REF mrel_ty; \n  mrel ");
		translation.append(mrelType);
		translation.append(";\n ");
		if(mobjectAsValue){
			translation.append("  value_dim dimension_ty;\n");
		}
		**/
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
	
	private void getMCubeReference() throws SQLException{
		translation.append("BEGIN \n  SELECT TREAT(REF(mc) AS REF ");
		translation.append(mcubeType);
		translation.append(")\n  INTO m_cube_ref \n  FROM mcubes mc \n  WHERE mc.cname = '");
		translation.append(mcubeId);
		translation.append("'; \n");
		translation.append("  utl_ref.select_object(m_cube_ref, m_cube);\n");
		
		/**
		translation.append("BEGIN \n  SELECT TREAT ( VALUE (mc) AS ");
		translation.append(mcubeType);
		translation.append(")\n  INTO m_cube \n  FROM mcubes mc \n  WHERE mc.cname = '");
		translation.append(mcubeId);
		translation.append("'; \n");
		**/
		translation.append("  mrel_ref := m_cube.get_mrel_ref('");
		
		SQLMNode cubeCoordinate = rootNode.jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeCoordinate.class);
		//check if the order of the cube coordinate sequence must be validated
		//QualifiedID = validation needed
		if(cubeCoordinate.jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){
			translation.append(dataDict.getMCubeSequence(rootNode));
			translation.append("'); \n");
		}else{ //UnQualifiedID = no validation
			for(int i=0;i<cubeCoordinate.jjtGetNumChildren();i++){
				translation.append((String)(cubeCoordinate.jjtGetChild(i)).jjtGetValue());
				if(i+1 == cubeCoordinate.jjtGetNumChildren()){
					translation.append("');");
				}else{
					translation.append("', '");
				}
			}
		}
	}
	
	private void translateSetMultilevelObjectAttributes() throws SQLException{
		translation.append("\n  utl_ref.select_object(mrel_ref, mrel); \n");
		MultilevelFactMeasure measure = new MultilevelFactMeasure(rootNode);
		String measureCode = measure.translateOnlySetMeasure();
		if(measureCode.length() > 0){
			translation.append(measureCode);
		}
	}
	
	private void createClosing(){
		translation.append("END;\n\n");
	}

}
