package at.jku.dke.sqlm.translator;

import java.sql.SQLException;
import java.util.ArrayList;
import at.jku.dke.sqlm.interpreter.DataDictionary;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeCoordinate;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactConnectionLevelDefinition;
import at.jku.dke.sqlm.parser.ASTMultilevelFactConnectionLevelHierarchy;
import at.jku.dke.sqlm.parser.ASTMultilevelFactConnectionLevelMeasure;
import at.jku.dke.sqlm.parser.ASTMultilevelFactID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactMeasureValueBlock;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will create multiple Multilevel Facts
 * @param sn is the root SQLMNode(sn) of the BULK CREATE MULTILEVEL FACT AST
 */
public class BulkCreateMultilevelFact extends Statement{

	StringBuffer translation;
	SQLMNode rootNode;
	SQLMNode cubeCoordinate;
	DataDictionary dataDict;
	String mcubeId;
	String mcubeType;
	String mrelType;
	String conlevelType;
	String concreteType;
	
	public BulkCreateMultilevelFact(SQLMNode sn){
		this.rootNode = sn;
	}
	
	public String translate() throws SQLException{
		translation = new StringBuffer();
		this.getDataFromDictionary();
		this.createNeededVariables();
		this.specifyMultilevelFactBulkCreation();
		this.translateMultilevelFactMeasures();
		return translation.toString();
	}

	private void getDataFromDictionary() throws SQLException {
		dataDict = DataDictionary.getDataDictionary();
		mcubeId = (String)(rootNode.jjtGetChild(0).jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeID.class)).jjtGetValue();
		mcubeType = dataDict.getMCubeType(mcubeId);
		mrelType = dataDict.getMRelType(mcubeId);
		conlevelType = dataDict.getConlevelType(mcubeId);
	}

	private void createNeededVariables() throws SQLException {
		translation.append("DECLARE \n  m_cube ");
		translation.append(mcubeType);
		translation.append(";\n  coordinates coordinate");
		concreteType = dataDict.getMCubeType( (String)(rootNode.jjtGetChild(0).jjtGetChild(0).jjtGetChild(1)).jjtGetValue() ).substring(5, 16);
		translation.append(concreteType);
		translation.append("_tty;\n");
		translation.append("BEGIN \n  SELECT TREAT ( VALUE (mc) AS ");
		translation.append(dataDict.getMCubeType( (String)(rootNode.jjtGetChild(0).jjtGetChild(0).jjtGetChild(1)).jjtGetValue() ));
		translation.append(")\n  INTO m_cube \n  FROM mcubes mc \n  WHERE mc.cname = '");
		translation.append((String)(rootNode.jjtGetChild(0).jjtGetChild(0).jjtGetChild(1)).jjtGetValue());
		translation.append("'; \n");
		
		translation.append("  coordinates := coordinate");
		translation.append(concreteType);
		translation.append("_tty(");
	}

	private void specifyMultilevelFactBulkCreation() throws SQLException {
		for(int i=0;i<rootNode.jjtGetNumChildren();i++){
			cubeCoordinate = rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeCoordinate.class);
			translation.append("coordinate");
			translation.append(concreteType);
			translation.append("_ty('");
			//QualifiedId
			if(cubeCoordinate.jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){ 
				translation.append(dataDict.getMCubeSequence(rootNode.jjtGetChild(i)));
			}else{ //UnQualifiedId
				for(int j=0;j<cubeCoordinate.jjtGetNumChildren();j++){
					translation.append((String)(cubeCoordinate.jjtGetChild(j)).jjtGetValue());
					if(j+1<cubeCoordinate.jjtGetNumChildren()){
						translation.append("', '");
					}
				}
			}
			translation.append("')");
			if(i+1<rootNode.jjtGetNumChildren()){
				translation.append(", ");
			}
		}
		translation.append("); \n");
		translation.append("  m_cube.bulk_create_mrel(coordinates);\nEND;\n\n");
	}

	private void translateMultilevelFactMeasures() throws SQLException {
		for(int i=0;i<rootNode.jjtGetNumChildren();i++){
			SQLMNode currentNode = rootNode.jjtGetChild(i);
			if( (currentNode.jjtGetChild(ASTMultilevelFactConnectionLevelHierarchy.class)!= null && 
					currentNode.jjtGetChild(ASTMultilevelFactConnectionLevelHierarchy.class).
					jjtGetChild(ASTMultilevelFactConnectionLevelDefinition.class). 
						jjtGetChild(ASTMultilevelFactConnectionLevelMeasure.class) != null ) || 
						currentNode.jjtGetChild(ASTMultilevelFactMeasureValueBlock.class) != null){
				//translation.setLength(0);
				translation.append("DECLARE \n  m_cube ");
				translation.append(mcubeType);
				translation.append("; \n  mrel_ref REF mrel_ty; \n  mrel ");
				translation.append(mrelType);
				translation.append(";\n  conlvl ");
				translation.append(conlevelType);
				translation.append(";\n");
				//declaration for m-object values
				boolean exist = this.existMObjectAsValues(currentNode);
				if(exist){
					translation.append("  value_dim dimension_ty;\n");
				}
				translation.append("BEGIN \n  SELECT TREAT ( VALUE (mc) AS ");
				translation.append(mcubeType);
				translation.append(" )\n  INTO m_cube \n  FROM mcubes mc \n  WHERE mc.cname = '");
				translation.append((String)(currentNode.jjtGetChild(0).jjtGetChild(1)).jjtGetValue());
				translation.append("'; \n");
				translation.append("  mrel_ref := m_cube.get_mrel_ref('");
				
				ArrayList<Integer> order = new ArrayList<Integer>();
				//check if the order of the cube coordinate sequence must be validated
				cubeCoordinate = currentNode.jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeCoordinate.class);
				//QualifiedID = validation needed
				if(cubeCoordinate.jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){
					translation.append(dataDict.getMCubeSequence(currentNode));
					translation.append("'); \n");
					order = dataDict.getMCubeOrder(currentNode);
				}else{ //UnQualifiedID = no validation
					for(int j=0;j<cubeCoordinate.jjtGetNumChildren();j++){
						order.add(j); 
						translation.append((String)(cubeCoordinate.jjtGetChild(j)).jjtGetValue());
						if(j+1 == cubeCoordinate.jjtGetNumChildren()){
							translation.append("');");
						}else{
							translation.append("', '");
						}			
					}
				}
				//get add/set measure code
				MultilevelFactMeasure measure = new MultilevelFactMeasure(rootNode.jjtGetChild(i));
				
				translation.append(" \n  utl_ref.select_object(mrel_ref, mrel); \n");
				translation.append(measure.translate(order, conlevelType));
				translation.append("END;\n\n");
			}
		}
	}
	
	
}
