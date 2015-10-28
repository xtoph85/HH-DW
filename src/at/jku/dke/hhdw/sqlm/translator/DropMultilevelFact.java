package at.jku.dke.sqlm.translator;

import at.jku.dke.sqlm.parser.ASTMultilevelCubeCoordinate;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectUnqualifiedID;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will drop a Multilevel Fact
 * @param sn is the root SQLMNode of the DROP MULTILEVEL FACT AST 
 */
public class DropMultilevelFact extends Statement{

	private StringBuffer translation;
	private SQLMNode rootNode;
	
	public DropMultilevelFact(SQLMNode sn){
		this.rootNode = sn;
	}
	
	public String translate(){
		translation = new StringBuffer();
		
		SQLMNode mlFactID = rootNode.jjtGetChild(ASTMultilevelFactID.class);
		translation.append("DECLARE \n  m_cube mcube_ty; \n  mrel mrel_ty; \n ");
		translation.append("BEGIN \n  SELECT TREAT ( VALUE (mc) AS mcube_ty)\n  INTO m_cube \n  FROM mcubes mc \n  WHERE mc.cname = '");
		translation.append((String)(mlFactID.jjtGetChild(ASTMultilevelCubeID.class)).jjtGetValue());
		translation.append("'; \n");
		translation.append("  mrel_ref := m_cube.get_mrel_ref('");
		//TODO: qualified und unqualified berücksichtigen
		for(int i=0;i<mlFactID.jjtGetChild(ASTMultilevelCubeCoordinate.class).jjtGetNumChildren();i++){
			translation.append((String)(mlFactID.jjtGetChild(ASTMultilevelCubeCoordinate.class).jjtGetChild(i).
					jjtGetChild(ASTMultilevelObjectUnqualifiedID.class)).jjtGetValue());
			if(i+1<mlFactID.jjtGetChild(ASTMultilevelCubeCoordinate.class).jjtGetNumChildren()){
				translation.append("', '");
			}			
		}
		translation.append("'); \n");
		translation.append("  utl_ref.select_object(mrel_ref, mrel); \n  mrel.delete(); \nEND;\n\n");
		
		return translation.toString();
	}

}
