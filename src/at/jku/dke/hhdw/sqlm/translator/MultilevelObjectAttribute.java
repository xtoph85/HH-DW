package at.jku.dke.sqlm.translator;

import at.jku.dke.sqlm.parser.ASTAggregationFunction;
import at.jku.dke.sqlm.parser.ASTDefault;
import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMetalevelID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectAttributeValueBlock;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectLevelHierarchy;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.ASTNumberType;
import at.jku.dke.sqlm.parser.ASTNumberValue;
import at.jku.dke.sqlm.parser.ASTObjectCollectionConstructor;
import at.jku.dke.sqlm.parser.ASTObjectConstructor;
import at.jku.dke.sqlm.parser.ASTVarchar2Type;
import at.jku.dke.sqlm.parser.SQLMNode;

public class MultilevelObjectAttribute {
	
	StringBuffer translation;
	SQLMNode rootNode;
	
	public MultilevelObjectAttribute(SQLMNode sn){
		this.rootNode = sn;
	}
	
	public String translate(){
		translation = new StringBuffer();
		translateAddAttribute();
		tranlsateSetAttribute();
		return translation.toString();
	}
	
	public String translateOnlyAddAttribute(){
		translation = new StringBuffer();
		translateAddAttribute();
		return translation.toString();
	}
	
	public String translateOnlySetAttribute(){
		translation = new StringBuffer();
		tranlsateSetAttribute();
		return translation.toString();
	}

	private void translateAddAttribute() {
		if(rootNode.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class) != null){
			SQLMNode levelHry = rootNode.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class);
			//run through the LevelDefinitions
			for(int i=0;i<levelHry.jjtGetNumChildren();i++){
				//check if attributes to add exist
				if(levelHry.jjtGetChild(i).jjtGetNumChildren() > 2){
					//get the attribute information
					for(int j=2;j<levelHry.jjtGetChild(i).jjtGetNumChildren();j++){
						SQLMNode currentAttribute = levelHry.jjtGetChild(i).jjtGetChild(j);
						translation.append("  mobject.add_attribute('");
						//attribute id
						translation.append((String)(currentAttribute.jjtGetChild(0)).jjtGetValue());
						translation.append("', '");
						//m-object id where the attribute gets added
						translation.append((String)(levelHry.jjtGetChild(i).jjtGetChild(0)).jjtGetValue());
						translation.append("', '");
						//attribute type
						translation.append((String)(currentAttribute.jjtGetChild(1)).jjtGetValue());
						//add additional information of the value
						if(currentAttribute.jjtGetChild(1) instanceof ASTVarchar2Type){
							//add DataLength
							translation.append("(");
							translation.append((String)(currentAttribute.jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
							translation.append(")");
						}
						if(currentAttribute.jjtGetChild(1) instanceof ASTNumberType){
							//add DataLength if existing
							if(currentAttribute.jjtGetChild(1).jjtGetNumChildren() > 0){
								translation.append("(");
								translation.append((String)(currentAttribute.jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
							}
							//add DataScale if existing
							if(currentAttribute.jjtGetChild(1).jjtGetNumChildren() > 1){
								translation.append(", ");
								translation.append((String)(currentAttribute.jjtGetChild(1).jjtGetChild(1)).jjtGetValue());
							}
							if(currentAttribute.jjtGetChild(1).jjtGetNumChildren() > 0){
								translation.append(")");
							}
						}
						translation.append("'); \n");
					}
				}
			}
		}
	}

	private void tranlsateSetAttribute() {
		if(rootNode.jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class) != null){
			SQLMNode attributeBlock = rootNode.jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class);
			SQLMNode attributeValue;
			//run through the attributes
			for(int i=0;i<attributeBlock.jjtGetNumChildren();i++){
				SQLMNode attribute = attributeBlock.jjtGetChild(i);
				int valuePos = 1;
				if(attribute.jjtGetChild(ASTMetalevelID.class) != null){
					valuePos++;
				}
				attributeValue = attribute.jjtGetChild(valuePos);
				if(attributeValue instanceof ASTMultilevelObjectQualifiedID){
					//get dimension reference for m-object as value
					translation.append("  SELECT VALUE (dim) INTO value_dim ");
					translation.append("\n  FROM dimensions dim \n  WHERE dim.dname = '");
					translation.append((String)(attributeValue.jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());
					translation.append("'; \n");
				}
				translation.append("  mobject.set_attribute('");
				//attribute id
				translation.append((String)(attribute.jjtGetChild(0)).jjtGetValue());
				translation.append("', ");
				//attribute meta-level information
				tranlsateAttributeMetaLevel(attribute);
				/**
				if(attribute.jjtGetChild(ASTMetalevelID.class) != null){
					translation.append("'");
					translation.append((String)(attribute.jjtGetChild(1)).jjtGetValue());
					translation.append("'");
				}else{
					translation.append("null");
				}
				//attribute DEFAULT/SHARED value
				if(attribute.jjtGetChild(ASTDefault.class) != null ){
					translation.append(", TRUE, ");
				}else{
					translation.append(", FALSE, ");
				}
				**/
				//attribute value
				if(attributeValue instanceof ASTNumberValue){
					translation.append("ANYDATA.convertNumber(");
					translation.append((String)attributeValue.jjtGetValue());
				}else{
					if(attributeValue instanceof ASTMultilevelObjectQualifiedID){
						translation.append("ANYDATA.convertRef(value_dim.get_mobject_ref('");
						translation.append((String)(attributeValue.jjtGetChild(0)).jjtGetValue());
					}else{
						if(attributeValue instanceof ASTObjectConstructor){
							translation.append("ANYDATA.convertObject(");
							translation.append((String)attributeValue.jjtGetValue());
						}else{
							if(attributeValue instanceof ASTObjectCollectionConstructor){
								translation.append("ANYDATA.convertCollection(");
								translation.append((String)attributeValue.jjtGetValue());
							}else{
								translation.append("ANYDATA.convertVarchar2(");
								if(attributeValue instanceof ASTAggregationFunction){
									translation.append("'");
								}
								translation.append((String)attributeValue.jjtGetValue());
							}
						}
					}
				}
				if(attributeValue instanceof ASTAggregationFunction){
					translation.append("'");
				}
				if(attributeValue instanceof ASTMultilevelObjectQualifiedID){
					translation.append("')");
				}
				translation.append("));\n");
			}
		}
		
	}
	
	private void tranlsateAttributeMetaLevel(SQLMNode attribute) {
		if(!(attribute.jjtGetChild(ASTMetalevelID.class) == null && attribute.jjtGetChild(ASTDefault.class) == null)){
			//attribute meta-level information
			if(attribute.jjtGetChild(ASTMetalevelID.class) != null){
				translation.append("'");
				translation.append((String)(attribute.jjtGetChild(1)).jjtGetValue());
				translation.append("'");
			}else{
				translation.append("null");
			}
			//attribute DEFAULT/SHARED value
			if(attribute.jjtGetChild(ASTDefault.class) != null){
				translation.append(", TRUE, ");
			}else{
				translation.append(", FALSE, ");
			}
		}
	}
	
}
