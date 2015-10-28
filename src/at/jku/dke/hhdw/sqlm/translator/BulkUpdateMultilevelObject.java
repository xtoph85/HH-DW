package at.jku.dke.sqlm.translator;

import java.util.HashMap;

import at.jku.dke.sqlm.parser.ASTAggregationFunction;
import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectAttributeID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectAttributeValueAssignment;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectAttributeValueBlock;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.ASTNumberValue;
import at.jku.dke.sqlm.parser.ASTObjectCollectionConstructor;
import at.jku.dke.sqlm.parser.ASTObjectConstructor;
import at.jku.dke.sqlm.parser.SQLMNode;

/**
 * Generates PL/SQL code out of the provided information inside the AST, which
 * will update multiple Multilevel Objects
 * @param sn is the root SQLMNode(sn) of the BULK UPDATE MULTILEVEL OBJECT AST
 * @throws SQLException 
 */
public class BulkUpdateMultilevelObject extends Statement{

	StringBuffer translation;
	SQLMNode rootNode;
	SQLMNode attribute;
	HashMap<String, String> mObjValueDim = new HashMap<String, String>();
	
	
	public BulkUpdateMultilevelObject(SQLMNode sn){
		this.rootNode = sn;
	}
	
	public String translate(){
		translation = new StringBuffer();
		this.createNeededVariables();
		this.setMultilevelObjectsReferences();
		this.specifyMultilevelObjectBulkUpdate();
		
		return translation.toString();
	}

	private void createNeededVariables() {
		translation.append("DECLARE \n  d dimension_ty; \n ");
		//create value_dim if m-object as value
		//run through all UpdateMultilevelObject statements under the BulkUpdateMultilevelObject rootNode 
		for(int i=0;i<rootNode.jjtGetNumChildren();i++){
			attribute = rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class).jjtGetChild(ASTMultilevelObjectAttributeValueAssignment.class);
			if(attribute.jjtGetChild(1) instanceof ASTMultilevelObjectQualifiedID){
				String value = "value_dim"+i;
				//get the dimension of the m-object
				String key = (String)(attribute.jjtGetChild(1).jjtGetChild(1)).jjtGetValue();
				mObjValueDim.put(key, value);
				translation.append("  ");
				translation.append(value);
				translation.append(" dimension_ty;\n");
			}
		}
	}
	
	private void setMultilevelObjectsReferences() {
		translation.append("BEGIN \n  SELECT VALUE (dim) INTO d \n  FROM dimensions dim \n  WHERE dim.dname = '");
		translation.append((String)(rootNode.jjtGetChild(0).jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
		translation.append("'; \n");
		
		//get dimension reference for m-object as value 
		/**
		if(rootNode.jjtGetChild(0).jjtGetChild(1).jjtGetChild(1) instanceof ASTMultilevelObjectQualifiedID){ 
			translation.append("  SELECT VALUE (dim) INTO value_dim \n  FROM dimensions dim \n  WHERE dim.dname = '");
			translation.append((String)(rootNode.jjtGetChild(0).jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
			translation.append("'; \n");
		}**/
		
		//get dimension reference for m-object as value
		for(int i=0;i<rootNode.jjtGetNumChildren();i++){
			attribute = rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class).jjtGetChild(ASTMultilevelObjectAttributeValueAssignment.class);
			if(attribute.jjtGetChild(1) instanceof ASTMultilevelObjectQualifiedID){
				translation.append("  SELECT VALUE (dim) INTO value_dim"+ i + "\n  FROM dimensions dim \n  WHERE dim.dname = '");
				translation.append((String)(attribute.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
				translation.append("'; \n");
			}
		}
		
		//get dimension ref for m-objects as value
		for(int i=0;i<rootNode.jjtGetNumChildren();i++){
			attribute = rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class).jjtGetChild(ASTMultilevelObjectAttributeValueAssignment.class);
			//get dimension reference for m-object as value
			if(attribute.jjtGetChild(1) instanceof ASTMultilevelObjectQualifiedID){
				String dimName = (String)(attribute.jjtGetChild(1).jjtGetChild(1)).jjtGetValue();
				translation.append("  SELECT VALUE (dim) INTO ");
				translation.append(mObjValueDim.get(dimName));
				translation.append("\n  FROM dimensions dim \n  WHERE dim.dname = '");
				translation.append(dimName);
				translation.append("'; \n");
			}
		}
	}


	private void specifyMultilevelObjectBulkUpdate() {
		translation.append("  d.bulk_set_attribute('");
		translation.append((String)(rootNode.jjtGetChild(0).jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class).
				jjtGetChild(ASTMultilevelObjectAttributeValueAssignment.class).jjtGetChild(ASTMultilevelObjectAttributeID.class)).jjtGetValue());
		translation.append("', mobject_value_tty(");
		for(int i=0;i<rootNode.jjtGetNumChildren();i++){
			attribute = rootNode.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class).jjtGetChild(ASTMultilevelObjectAttributeValueAssignment.class);
			translation.append("mobject_value_ty('");
			//add m-object name
			translation.append((String)(rootNode.jjtGetChild(i).jjtGetChild(0).jjtGetChild(0)).jjtGetValue());
			translation.append("', ");
			//attribute value
			if(attribute.jjtGetChild(1) instanceof ASTNumberValue){
				translation.append("ANYDATA.convertNumber(");
				translation.append((String)(attribute.jjtGetChild(1)).jjtGetValue());
			}else{
				if(attribute.jjtGetChild(1) instanceof ASTMultilevelObjectQualifiedID){
					String dimName = (String)(attribute.jjtGetChild(1).jjtGetChild(1)).jjtGetValue();
					translation.append("ANYDATA.convertRef(");
					translation.append(mObjValueDim.get(dimName));
					translation.append(".get_mobject_ref('");
					translation.append((String)(attribute.jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
				}else{
					if(attribute.jjtGetChild(1) instanceof ASTObjectConstructor){
						translation.append("ANYDATA.convertObject(");
						translation.append((String)(attribute.jjtGetChild(1)).jjtGetValue());
					}else{
						if(attribute.jjtGetChild(1) instanceof ASTObjectCollectionConstructor){
							translation.append("ANYDATA.convertCollection(");
							translation.append((String)(attribute.jjtGetChild(1)).jjtGetValue());
						}else{
							translation.append("ANYDATA.convertVarchar2(");
							if(attribute.jjtGetChild(1) instanceof ASTAggregationFunction){
								translation.append("'");
							}
							translation.append((String)(attribute.jjtGetChild(1)).jjtGetValue());
						}
					}
				}
			}
			if(attribute.jjtGetChild(1) instanceof ASTAggregationFunction){
				translation.append("'");
			}
			if(attribute.jjtGetChild(1) instanceof ASTMultilevelObjectQualifiedID){
				translation.append("')");
			}
			if(i+1<rootNode.jjtGetNumChildren()){
				translation.append(")), \n\t\t\t");
			}
			//during the run through check for the highest end-line
			if((rootNode.jjtGetChild(i)).jjtGetErrorLine() > rootNode.jjtGetErrorLine()){
				rootNode.jjtSetErrorLine((rootNode.jjtGetChild(i)).jjtGetErrorLine());
			}
		}
		translation.append("))));\n");		
		translation.append("END;\n\n");
	}
	
}
