package at.jku.dke.sqlm.translator;

import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectAttributeValueBlock;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectLevelAttribute;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectLevelDefinition;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectLevelHierarchy;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectLevelID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectParents;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectUnqualifiedID;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will create multiple Multilevel Objects
 * @param sn is the root SQLMNode of the BULK CREATE MULTILEVEL OBJECT AST
 * @throws SQLException 
 */
public class BulkCreateMultilevelObject extends Statement{

	StringBuffer translation;
	SQLMNode rootNode;
	SQLMNode firstNode;
	
	public BulkCreateMultilevelObject(SQLMNode sn){
		this.rootNode = sn;
	}
	
	public String translate(){
		translation = new StringBuffer();
		this.createNeededVariables();
		this.setMultilevelObjectsReferences();
		this.setMultilevelObjectsParentReferences();
		this.setLevelHierarchyReferences();
		this.specifyMultilevelObjectBulkCreation();
		this.translateMultilevelObjectAttributes();
		return translation.toString();
	}

	private void createNeededVariables() {
		firstNode = rootNode.jjtGetChild(0);
		translation.append("DECLARE \n  d dimension_ty; \n  parents mobject_trty; \n  levelhierarchy level_hierarchy_tty; \n  onames names_tty; \n");
		translation.append("BEGIN \n  SELECT VALUE (dim) INTO d \n  FROM dimensions dim \n  WHERE dim.dname = '");
		translation.append((String)(firstNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
		translation.append("'; \n");
		
	}
	
	private void setMultilevelObjectsReferences() {
		//insert the names of the MlObjects into names_tty
		translation.append("  onames := names_tty(");
		for(int i=0;i<rootNode.jjtGetNumChildren();i++){
			translation.append("'");
			translation.append((String)(rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTMultilevelObjectUnqualifiedID.class)).jjtGetValue());
			translation.append("'");
			if(i+1<rootNode.jjtGetNumChildren()){
				translation.append(", ");
			}
			//during the run through the MlObjects check for the greatest end-line
			if((rootNode).jjtGetErrorLine() > rootNode.jjtGetErrorLine()){
				rootNode.jjtSetErrorLine((rootNode.jjtGetChild(i)).jjtGetErrorLine());
			}
		}
		translation.append("); \n");
	}
	
	private void setMultilevelObjectsParentReferences() {
		//get the parents of the MlObjects
		if(firstNode.jjtGetChild(ASTMultilevelObjectParents.class) != null ){
			translation.append("  parents := mobject_trty(); \n");
			for(int i=0;i<firstNode.jjtGetChild(ASTMultilevelObjectParents.class).jjtGetNumChildren();i++){
				translation.append("  parents.extend; \n  parents(parents.LAST) := d.get_mobject_ref('");
				translation.append((String)(firstNode.jjtGetChild(ASTMultilevelObjectParents.class).jjtGetChild(i)).jjtGetValue());
				translation.append("'); \n");
			}
		}
	}
	
	private void setLevelHierarchyReferences() {
		//get the LevelHierarchy of the MlObject if it exist
		if(firstNode.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class) != null ){
			translation.append("  levelhierarchy := level_hierarchy_tty(");
			SQLMNode levelHierarchy = firstNode.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class);
			for(int i=0;i<levelHierarchy.jjtGetNumChildren();i++){
				translation.append("level_hierarchy_ty('");
				translation.append((String)(levelHierarchy.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue());
				//if no input -> NULL, else value
				if(levelHierarchy.jjtGetChild(i).jjtGetChild(1).jjtGetNumChildren() == 0){
					translation.append("', NULL)"); 
				}else{
					translation.append("', '");
					translation.append((String)(levelHierarchy.jjtGetChild(i).jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
					translation.append("')");
					//check if more than one parent exists
					if(levelHierarchy.jjtGetChild(i).jjtGetChild(1).jjtGetNumChildren() > 1){
						for(int j=1;j<levelHierarchy.jjtGetChild(i).jjtGetChild(1).jjtGetNumChildren();j++){
							translation.append(", level_hierarchy_ty('");
							translation.append((String)(levelHierarchy.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue());
							translation.append("', '");
							translation.append((String)(levelHierarchy.jjtGetChild(i).jjtGetChild(1).jjtGetChild(j)).jjtGetValue());
							translation.append("')");
						}
					}
				}
				if(i+1<levelHierarchy.jjtGetNumChildren()){
					translation.append(", ");
				}
			}
			translation.append("); \n");
		}
	}
	
	private void specifyMultilevelObjectBulkCreation(){
		//bulkCreateDimObj Statement
		translation.append("  d.bulk_create_mobject(onames, ");
		//set DimObjLevelID
		translation.append("'");
		translation.append((String)(firstNode.jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue());
		translation.append("', ");
		//set DimObjParents if exist
		if(firstNode.jjtGetChild(ASTMultilevelObjectParents.class) != null ){
			translation.append("parents, ");
		}else{
			translation.append("NULL, ");
		}
		//set DimObjLvlHierarchy
		translation.append("levelhierarchy); \nEND;\n\n");
	}
	
	private void translateMultilevelObjectAttributes(){
		//generate add/set attribute Statements
		for(int i=0;i<rootNode.jjtGetNumChildren();i++){
			//translation.setLength(0);
			if( (rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelHierarchy.class) != null && 
					rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).
						jjtGetChild(ASTMultilevelObjectLevelDefinition.class).
							jjtGetChild(ASTMultilevelObjectLevelAttribute.class) != null) ||
							rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class) != null	){
				MultilevelObjectAttribute attribute = new MultilevelObjectAttribute(rootNode.jjtGetChild(i));
				String attributeCode = attribute.translate();
				if(attributeCode.length() > 0){
					SQLMNode objectID = rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectQualifiedID.class);
					translation.append("DECLARE \n  d dimension_ty ; \n  mobject_ref REF mobject_ty; \n  mobject mobject_ty; \n");
					if(super.existMObjectAsValues(rootNode.jjtGetChild(i))){
						translation.append("  value_dim dimension_ty;\n");
					}
					translation.append("BEGIN \n  SELECT VALUE (dim) INTO d \n  FROM dimensions dim \n  WHERE dim.dname = '");
					translation.append((String)(objectID.jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
					translation.append("'; \n  mobject_ref := d.get_mobject_ref('");
					translation.append((String)(objectID.jjtGetChild(ASTMultilevelObjectUnqualifiedID.class)).jjtGetValue());
					translation.append("'); \n  utl_ref.select_object(mobject_ref, mobject); \n");
					translation.append(attributeCode);
					translation.append("END;\n\n");
				}
			}
		}
	}
	
}
