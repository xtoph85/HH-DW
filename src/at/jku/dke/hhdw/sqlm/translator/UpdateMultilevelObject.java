package at.jku.dke.sqlm.translator;

import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectUnqualifiedID;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will update a Multilevel Object
 * @param sn is the root SQLMNode(sn) of the UPDATE MULTILEVEL OBJECT AST
 * @throws SQLException 
 */
public class UpdateMultilevelObject extends Statement {

	private StringBuffer translation;
	private SQLMNode rootNode;
	private boolean mobjectAsValue;
	
	public UpdateMultilevelObject(SQLMNode sn){
		this.rootNode = sn;
		this.mobjectAsValue = super.existMObjectAsValues(sn);
	}
	
	public String translate(){
		translation = new StringBuffer();
		this.createNeededVariables();
		this.createReferences();
		this.translateSetMultilevelObjectAttributes();
		this.createClosing();
		
		return translation.toString();
	}
	
	private void createNeededVariables(){
		translation.append("DECLARE\n  d dimension_ty;\n  mobject_ref REF mobject_ty;\n  mobject mobject_ty;\n");
		if(mobjectAsValue){
			translation.append("  value_dim dimension_ty;\n");
		}
	}
	
	private void createReferences(){
		translation.append("BEGIN\n  SELECT VALUE (dim) INTO d\n  FROM dimensions dim\n  WHERE dim.dname = '");
		translation.append((String)(rootNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
		translation.append("'; \n  mobject_ref := d.get_mobject_ref ('");
		translation.append((String)(rootNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTMultilevelObjectUnqualifiedID.class)).jjtGetValue());
		translation.append("'); \n  utl_ref.select_object(mobject_ref, mobject); \n");
	}
	
	
	private void translateSetMultilevelObjectAttributes(){
		//translation.append("  utl_ref.select_object(mobject_ref, mobject); \n");
		MultilevelObjectAttribute attribute = new MultilevelObjectAttribute(rootNode);
		String attributeCode = attribute.translateOnlySetAttribute();
		if(attributeCode.length() > 0){
			translation.append(attributeCode);
		}
	}
	
	private void createClosing(){
		translation.append("END;\n\n");
	}
}
