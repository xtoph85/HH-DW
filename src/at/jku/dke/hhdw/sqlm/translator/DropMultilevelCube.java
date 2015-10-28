package at.jku.dke.sqlm.translator;

import java.sql.SQLException;

import at.jku.dke.sqlm.interpreter.DataDictionary;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeID;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will drop a Multilevel Cube
 * @param sn is the root SQLMNode of the DROP MULTILEVEL CUBE AST
 * @throws SQLException 
 */
public class DropMultilevelCube extends Statement{

	private StringBuffer translation;
	private SQLMNode rootNode;
	private DataDictionary dataDict;
	
	public DropMultilevelCube(SQLMNode sn){
		this.rootNode = sn;
	}
	
	public String translate() throws SQLException{
		translation = new StringBuffer();
		
		translation.append("DECLARE \n");
		translation.append("BEGIN \n  mcube.delete_mcube('");
		translation.append((String)(rootNode.jjtGetChild(ASTMultilevelCubeID.class)).jjtGetValue());
		translation.append("'); \nEND;\n\n");
		//delete the sequence out of the DataDictionary
		dataDict = DataDictionary.getDataDictionary();
		dataDict.deleteSequence((String)(rootNode.jjtGetChild(ASTMultilevelCubeID.class)).jjtGetValue());
		
		return translation.toString();
	}

}
