package at.jku.dke.sqlm.translator;

import java.sql.SQLException;

import at.jku.dke.sqlm.interpreter.DataDictionary;
import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectUnqualifiedID;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will drop a Multilevel Object
 * @param sn is the root SQLMNode of the DROP MULTILEVEL OBJECT AST
 */
public class DropMultilevelObject extends Statement{
	
	private StringBuffer translation;
	private SQLMNode rootNode;
	private DataDictionary dataDict;
	
	public DropMultilevelObject(SQLMNode sn){
		this.rootNode = sn;
	}
	
	public String translate() throws SQLException{
		translation = new StringBuffer();
		
		String dimHryID = (String)(rootNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue();
		dataDict = DataDictionary.getDataDictionary();
		translation.append("DECLARE \n  mobject mobject_");
		translation.append(dataDict.getDimensionHierarchyId(dimHryID));
		translation.append("_ty; \n");
		translation.append("BEGIN \n  mobject.delete_mobject('");
		translation.append((String)(rootNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTMultilevelObjectUnqualifiedID.class)).jjtGetValue());
		translation.append("'); \nEND;\n\n");
		
		return translation.toString();
	}
}
