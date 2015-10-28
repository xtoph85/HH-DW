package at.jku.dke.sqlm.translator;

import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectLevelHierarchy;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectLevelID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectLevelParentLevels;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectParents;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectUnqualifiedID;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will generate a Multilevel Object
 * @param sn is the root SQLMNode of the CREATE MULTILEVEL OBJECT AST
 * @param mobjectAsValue indicates if an optional variable is needed or not 
 */
public class CreateMultilevelObject extends Statement {

	StringBuffer translation;
	SQLMNode rootNode;
	boolean mobjectAsValue;
	
	public CreateMultilevelObject(SQLMNode sn){
		this.rootNode = sn;
		this.mobjectAsValue = super.existMObjectAsValues(sn);
	}
	
	public String translate(){
		translation = new StringBuffer();
		this.createNeededVariables();
		this.createStartingCode();
		//this.translateMlObjectParentInformation();
		this.translateMlObjectLevelHierarchyInformation();
		this.specifyMlObjectCreation();
		this.translateMlObjectAttributes();
		this.createClosing();
		return translation.toString();
	}
	
	private void createNeededVariables(){
		translation.append("DECLARE \n  d dimension_ty;");
		translation.append("\n  mobject_ref REF mobject_ty;");
		translation.append("\n  mobject mobject_ty;");
		//translation.append("\n  parents mobject_trty;");
		translation.append("\n  levelhierarchy level_hierarchy_tty; \n");
		
		/**
		SQLMNode objectParents = rootNode.jjtGetChild(ASTMultilevelObjectParents.class);
		if(objectParents != null){
			translation.append("  parent_onames names_tty := names_tty('");
			for(int i=0;i<objectParents.jjtGetNumChildren();i++){
				translation.append((String)(objectParents.jjtGetChild(i)).jjtGetValue());
				if(i+1<objectParents.jjtGetNumChildren()){
					translation.append("', '");
				}
			}
			translation.append("');\n");
		}else{
			translation.append("  parent_onames names_tty := names_tty(NULL);\n");
		}
		**/
		if(mobjectAsValue){
			translation.append("  value_dim dimension_ty;\n");
		}
	}
	
	private void createStartingCode(){
		translation.append("BEGIN \n  SELECT VALUE (dim) INTO d \n  FROM dimensions dim \n  WHERE dim.dname = '");
		translation.append((String)(rootNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
		translation.append("'; \n");
	}
	
	private void translateMlObjectParentInformation(){
		//get MlObjectParents-Object if exist	
		SQLMNode objectParents = rootNode.jjtGetChild(ASTMultilevelObjectParents.class);
		if(objectParents != null){
			translation.append("  parents := mobject_trty(); \n");
			for(int i=0;i<objectParents.jjtGetNumChildren();i++){
				translation.append("  parents.extend; \n  parents(parents.LAST) := d.get_mobject_ref('");
				translation.append((String)(objectParents.jjtGetChild(i)).jjtGetValue());
				translation.append("'); \n");
			}
		}
	}
	
	private void translateMlObjectParentInformationNew(){
		//mobject_trty(dim.get_mobject_ref('Austria'), dim.get_mobject_ref('Alps')),
		//get MlObjectParents-Object if exist	
		SQLMNode objectParents = rootNode.jjtGetChild(ASTMultilevelObjectParents.class);
		if(objectParents != null){
			translation.append("mobject_trty(");
			for(int i=0;i<objectParents.jjtGetNumChildren();i++){
				if(i>0){
					translation.append(", ");
				}
				translation.append("d.get_mobject_ref('");
				translation.append((String)(objectParents.jjtGetChild(i)).jjtGetValue());
				translation.append("')");
			}
			translation.append("), ");
		}
	}
	
	private void translateMlObjectLevelHierarchyInformation(){
		if(rootNode.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class) != null ){
			translation.append("  levelhierarchy := level_hierarchy_tty(");
			SQLMNode objectLevelHierarchy = rootNode.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class);
			for(int i=0;i<objectLevelHierarchy.jjtGetNumChildren();i++){
				translation.append("level_hierarchy_ty('");
				translation.append((String)(objectLevelHierarchy.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue());
				SQLMNode currentParentLevel = objectLevelHierarchy.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelParentLevels.class);
				if(currentParentLevel.jjtGetNumChildren() == 0){
					translation.append("', NULL)");
				}else{
					translation.append("', '");
					translation.append((String)(currentParentLevel.jjtGetChild(0)).jjtGetValue());
					translation.append("')");
					//check if more then one parent exists
					if(currentParentLevel.jjtGetNumChildren() > 1){
						for(int j=1;j<currentParentLevel.jjtGetNumChildren();j++){
							translation.append(", level_hierarchy_ty('");
							translation.append((String)(objectLevelHierarchy.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue());
							translation.append("', '");
							translation.append((String)(currentParentLevel.jjtGetChild(j)).jjtGetValue());
							translation.append("')");
						}
					}
				}
				if(i+1<objectLevelHierarchy.jjtGetNumChildren()){
					translation.append(", ");
				}
			}
			translation.append("); \n");
		}
	}
	
	private void specifyMlObjectCreation() {
		translation.append("  mobject_ref := d.create_mobject('");
		//get MlObjUnqualifiedID
		translation.append((String)(rootNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTMultilevelObjectUnqualifiedID.class)).jjtGetValue());
		translation.append("', ");
		//get MlObjLevelID
		translation.append("'");
		translation.append((String)(rootNode.jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue());
		translation.append("', ");
		//set MlObjParents if exist
		if(rootNode.jjtGetChild(ASTMultilevelObjectParents.class) != null ){
			//translation.append("parents, ");
			translateMlObjectParentInformationNew();
		}else{
			translation.append("NULL, ");
		}
		translation.append("levelhierarchy); \n");
		
	}
	
	private void translateMlObjectAttributes(){
		//get Code for adding/setting attribute values
		MultilevelObjectAttribute attribute = new MultilevelObjectAttribute(rootNode);
		String attributeCode = attribute.translate();
		if(attributeCode.length() > 0){
			translation.append("  utl_ref.select_object(mobject_ref, mobject); \n");
			translation.append(attributeCode);
		}
	}
	
	private void createClosing(){
		translation.append("END;\n\n");
	}
	
}
