package at.jku.dke.sqlm.translator;

import java.util.HashMap;

import at.jku.dke.sqlm.interpreter.DataDictionary;
import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectUnqualifiedID;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will generate a Multilevel Cube
 * @param sn is the root SQLMNode of the CREATE MULTILEVEL CUBE AST
 * @throws SQLException 
 */
public class CreateMultilevelCube extends Statement{
	
	private StringBuffer translation;
	private SQLMNode rootNode;
	private HashMap<Integer, String> hm;
	
	public CreateMultilevelCube(SQLMNode sn){
		rootNode = sn;
	}
	
	public String translate(){
		translation = new StringBuffer();
		hm = new HashMap<Integer, String>();
		this.createVariablesAndRefrences();
		this.updateDataDictionary();
		return translation.toString();
	}

	private void createVariablesAndRefrences(){
		//get MlCubeID		
		translation.append("DECLARE \n  dimensions names_tty; \n  root_coordinate names_tty; \n" +
				"  mcube_ref REF mcube_ty;\nBEGIN \n");
		translation.append("  dimensions := names_tty(");
		for(int i=1;i<rootNode.jjtGetNumChildren();i++){
			translation.append("'");
			translation.append((String)(rootNode.jjtGetChild(i).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
			//store the sequence inside the DataDictionary
			hm.put(i, (String)(rootNode.jjtGetChild(i).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
			if(i+1<rootNode.jjtGetNumChildren()){
				translation.append("', ");
			}else{
				translation.append("'); \n");
			}
		}
		//get the coordinate of the MlCube
		translation.append("  root_coordinate := names_tty(");
		for(int i=1;i<rootNode.jjtGetNumChildren();i++){			
			translation.append("'");
			translation.append((String)(rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectUnqualifiedID.class)).jjtGetValue());
			if(i+1<rootNode.jjtGetNumChildren()){
				translation.append("', ");
			}else{
				translation.append("'); \n");
			}
		}
		translation.append("  mcube_ref := mcube.create_mcube('");
		translation.append((String)rootNode.jjtGetChild(ASTMultilevelCubeID.class).jjtGetValue());
		translation.append("', dimensions, root_coordinate); \nEND;\n\n");
	}
	
	private void updateDataDictionary() {
		//save MLCubeID + cube coordinate sequence at the DataDictionary for later usage
		DataDictionary dataDict = DataDictionary.getDataDictionary();
		dataDict.setSequence((String)rootNode.jjtGetChild(ASTMultilevelCubeID.class).jjtGetValue(), hm);
	}
}
