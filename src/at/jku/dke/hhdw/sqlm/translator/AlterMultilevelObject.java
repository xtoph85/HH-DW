package at.jku.dke.sqlm.translator;

import at.jku.dke.sqlm.parser.ASTAlterMultilevelObjectAddAttribute;
import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectAttributeID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectLevelID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectUnqualifiedID;
import at.jku.dke.sqlm.parser.ASTNumberType;
import at.jku.dke.sqlm.parser.ASTVarchar2Type;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will alter a Multilevel Object
 * @param sn is the root SQLMNode of the ALTER MULTILEVEL OBJECT AST
 */
public class AlterMultilevelObject extends Statement {

	StringBuffer translation;
	SQLMNode rootNode;
	
	public AlterMultilevelObject(SQLMNode sn){
		this.rootNode = sn;
	}
	
	public String translate(){
		translation = new StringBuffer();
		
		this.createNeededVariablesAndReferences();
		this.translateAttributeInformation();
		this.createClosing();
		
		return translation.toString();
	}
	
	
	private void createNeededVariablesAndReferences(){
		translation.append("DECLARE \n  d dimension_ty; \n  mobject_ref REF mobject_ty; \n  mobject mobject_ty; \n");
		translation.append("BEGIN \n  SELECT VALUE (dim) INTO d \n  FROM dimensions dim \n  WHERE dim.dname = '");
		translation.append((String)rootNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTDimensionHierarchyID.class).jjtGetValue());
		translation.append("'; \n  mobject_ref := d.get_mobject_ref('");
		translation.append((String)rootNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTMultilevelObjectUnqualifiedID.class).jjtGetValue());
		translation.append("'); \n  utl_ref.select_object(mobject_ref, mobject); \n");
	}
	
	private void translateAttributeInformation(){
		for(int i=1;i<rootNode.jjtGetNumChildren();i++){
			//AddAttribute
			if(rootNode.jjtGetChild(i) instanceof ASTAlterMultilevelObjectAddAttribute){
				//run through the attributes
				for(int j=0;j+1<rootNode.jjtGetChild(i).jjtGetNumChildren();j++){
					SQLMNode attribute = rootNode.jjtGetChild(i).jjtGetChild(j);
					translation.append("  mobject.add_attribute('");
					translation.append((String)attribute.jjtGetChild(ASTMultilevelObjectAttributeID.class).jjtGetValue());
					translation.append("', '");
					translation.append((String)(rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue());
					translation.append("',  '");
					//attribute value
					translation.append((String)(attribute.jjtGetChild(1)).jjtGetValue());
					//add additional information of the value
					if(attribute.jjtGetChild(1) instanceof ASTVarchar2Type){
						//add DataLength
						translation.append("(");
						translation.append((String)(attribute.jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
						translation.append(")");
					}
					if(attribute.jjtGetChild(1) instanceof ASTNumberType){
						//add DataLength if existing
						if(attribute.jjtGetChild(1).jjtGetNumChildren() > 0){
							translation.append("(");
							translation.append((String)(attribute.jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
						}
						//add DataScale if existing
						if(attribute.jjtGetChild(1).jjtGetNumChildren() > 1){
							translation.append(", ");
							translation.append((String)(attribute.jjtGetChild(1).jjtGetChild(1)).jjtGetValue());
						}
						if(attribute.jjtGetChild(1).jjtGetNumChildren() > 0){
							translation.append(")");
						}
					}
					translation.append("'); \n");
					
				}
			//DropAttribute
			}else{
				translation.append("  mobject.delete_attribute('");
				translation.append((String)(rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectAttributeID.class)).jjtGetValue());
				translation.append("'); \n");
			}
		}
	}
	
	private void createClosing(){
		translation.append("END; \n\n");
	}
	
}
