package at.jku.dke.sqlm.translator;

import java.sql.SQLException;
import java.util.ArrayList;

import at.jku.dke.sqlm.interpreter.DataDictionary;
import at.jku.dke.sqlm.parser.ASTAlterMultilevelFactAddMeasure;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeCoordinate;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactConnectionLevelID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactConnectionLevelMeasure;
import at.jku.dke.sqlm.parser.ASTMultilevelFactID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactMeasureID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.ASTNumberType;
import at.jku.dke.sqlm.parser.ASTVarchar2Type;
import at.jku.dke.sqlm.parser.SQLMNode;


/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will alter a Multilevel Fact
 * @param sn is the root SQLMNode of the ALTER MULTILEVEL FACT AST
 * @throws SQLException 
 */
public class AlterMultilevelFact extends Statement {

	StringBuffer translation;
	SQLMNode rootNode;
	private DataDictionary dataDict;
	
	private ArrayList<Integer> order;
	private String conlevelType;
	
	public AlterMultilevelFact(SQLMNode sn){
		this.rootNode = sn;
	}
	
	public String translate() throws SQLException{
		translation = new StringBuffer();
		dataDict = DataDictionary.getDataDictionary();
		order = new ArrayList<Integer>();
		
		this.createNeededVariablesAndReferences();
		this.translateMeasureInformation();
		this.createClosing();
		
		return translation.toString();
	}
	

	private void createNeededVariablesAndReferences() throws SQLException{
		String mcubeId = (String)(rootNode.jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeID.class)).jjtGetValue();
		String mcubeType = dataDict.getMCubeType(mcubeId);
		String mrelType = dataDict.getMRelType(mcubeId);
		conlevelType = dataDict.getConlevelType(mcubeId);
		
		translation.append("DECLARE \n  m_cube ");
		translation.append(mcubeType);
		translation.append("; \n  mrel_ref REF mrel_ty; \n  mrel ");
		translation.append(mrelType);
		translation.append(";\n  conlvl ");
		translation.append(conlevelType);
		translation.append(";\nBEGIN \n  SELECT TREAT ( VALUE (mc) AS ");
		translation.append(mcubeType);
		translation.append(" )\n  INTO m_cube \n  FROM mcubes mc \n  WHERE mc.cname = '");
		translation.append(mcubeId);
		translation.append("'; \n");
		translation.append("  mrel_ref := m_cube.get_mrel_ref('");
		
		SQLMNode cubeCoordinate = rootNode.jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeCoordinate.class);
		//check if the order of the cube coordinate sequence must be validated
		//QualifiedID = validation needed
		if(cubeCoordinate.jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){ 
			translation.append(dataDict.getMCubeSequence(rootNode));
			translation.append("'); \n");
			order = dataDict.getMCubeOrder(rootNode);
		}else{ //UnQualifiedID = no validation
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
	
	
	private void translateMeasureInformation() {
		translation.append("  utl_ref.select_object(mrel_ref, mrel); \n");
		for(int i=1;i<rootNode.jjtGetNumChildren();i++){
			SQLMNode measure = rootNode.jjtGetChild(i);
			//AddMeasure
			if(measure instanceof ASTAlterMultilevelFactAddMeasure){
				for(int j=0;j<rootNode.jjtGetNumChildren();j++){
					measure = rootNode.jjtGetChild(j);
					translation.append("  mrel.add_measure('");
					translation.append((String)(measure.jjtGetChild(ASTMultilevelFactConnectionLevelMeasure.class).jjtGetChild(ASTMultilevelFactMeasureID.class)).jjtGetValue());
					translation.append("', "); 
					translation.append(conlevelType); 
					translation.append("('");
					int count = 1;
					for(int k : order){
						translation.append((String)(measure.jjtGetChild(ASTMultilevelFactConnectionLevelID.class).jjtGetChild(k)).jjtGetValue());
						if(count<order.size()){
							translation.append("', '");
						}
						count++;
					}
					translation.append("'), '"); 
					//data type
					translation.append((String)(measure.jjtGetChild(ASTMultilevelFactConnectionLevelMeasure.class).jjtGetChild(1)).jjtGetValue()); 
					//add additional information of the value
					if(measure.jjtGetChild(1) instanceof ASTVarchar2Type){
						translation.append("(");
						translation.append((String)(measure.jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
						translation.append(")");
					}
					if(measure.jjtGetChild(1) instanceof ASTNumberType){
						//add DataLength if existing
						if(measure.jjtGetChild(1).jjtGetNumChildren() > 0){
							translation.append("(");
							translation.append((String)(measure.jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
						}
						//add DataScale if existing
						if(measure.jjtGetChild(1).jjtGetNumChildren() > 1){
							translation.append(", ");
							translation.append((String)(measure.jjtGetChild(1).jjtGetChild(1)).jjtGetValue());
						}
						if(measure.jjtGetChild(1).jjtGetNumChildren() > 0){
							translation.append(")");
						}
					}
					translation.append("'); \n");
					//last position must be skipped
					if(j+2==rootNode.jjtGetNumChildren()){
						j++;
					}
				}
			//DropMeasure
			}else{
				translation.append("  mrel.delete_measure('");
				translation.append((String)(measure.jjtGetChild(ASTMultilevelFactMeasureID.class)).jjtGetValue());
				translation.append("'); \n");
			}			
		}
	}

	
	private void createClosing() {
		translation.append("END; \n\n");
	}
	
	

}
