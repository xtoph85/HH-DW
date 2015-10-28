package at.jku.dke.sqlm.translator;

import java.sql.SQLException;

import at.jku.dke.sqlm.parser.ASTMultilevelFactMeasureValueBlock;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectAttributeValueBlock;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectQualifiedID;
import at.jku.dke.sqlm.parser.SQLMNode;

public abstract class Statement{
	
	public abstract String translate() throws SQLException; //{
	//	return null;
	//}
	
	/**
	 * Checks if a m-object reference is used as a value and returns true
	 * when a m-object is used as value, else false
	 * @param sn is the root SQLMNode(sn) of the MULTILEVEL OBJECT ATTRIBUTE VALUE BLOCK AST
	 * @throws SQLException 
	 */
	protected boolean existMObjectAsValues(SQLMNode sn){
		boolean exist = false;
		if(sn.jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class) != null){
			SQLMNode attributeBlock = sn.jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class);
			//run through the attributes
			for(int i=0;i<attributeBlock.jjtGetNumChildren();i++){
				//check for m-objects as value
				if(attributeBlock.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null ){
					exist = true;
				}
			}
		}
		if(sn.jjtGetChild(ASTMultilevelFactMeasureValueBlock.class) != null){
			SQLMNode measureBlock = sn.jjtGetChild(ASTMultilevelFactMeasureValueBlock.class);
			//run through the measures
			for(int i=0;i<measureBlock.jjtGetNumChildren();i++){
				//check for m-objects as value
				if(measureBlock.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null ){
					exist = true;
				}
			}
		}
		return exist;
	}
	
}
