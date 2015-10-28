package at.jku.dke.sqlm.translator;

import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.SQLMNode;


/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will generate a Dimension Hierarchy
 * @param rootNode is the root SQLMNode(sn) of the CREATE DIMENSION HIERARCHY AST
 * @throws SQLException 
 */
public class CreateDimensionHierarchy extends Statement{

	private StringBuffer translation;
	private SQLMNode rootNode;
	
	public CreateDimensionHierarchy(SQLMNode sn){
		translation = new StringBuffer();
		rootNode = sn;
	}
	
	public String translate(){
		translation.append("DECLARE \n  dim REF dimension_ty; \n");
		translation.append("BEGIN \n  dim := dimension.create_dimension('");
		translation.append((String)(rootNode.jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
		translation.append("'); \nEND;\n\n");
		return translation.toString();
	}
	
}



