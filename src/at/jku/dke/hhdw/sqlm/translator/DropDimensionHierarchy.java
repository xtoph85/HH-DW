package at.jku.dke.sqlm.translator;

import java.sql.SQLException;

import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will drop a Dimension Hierarchy
 * @param sn is the root SQLMNode of the DROP DIMENSION HIERARCHY AST 
 */
public class DropDimensionHierarchy extends Statement{

	private StringBuffer translation;
	private SQLMNode rootNode;
	
	public DropDimensionHierarchy(SQLMNode sn){
		this.rootNode = sn;
	}
	
	public String translate() throws SQLException{
		translation = new StringBuffer();
		
		translation.append("DECLARE \nBEGIN \n  dimension.delete_dimension('");
		translation.append((String)(rootNode.jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
		translation.append("'); \nEND;\n\n");
		
		return translation.toString();
	}
}
