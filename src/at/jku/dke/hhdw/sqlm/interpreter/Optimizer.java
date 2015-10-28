package at.jku.dke.sqlm.interpreter;

import java.util.ArrayList;
import java.util.HashMap;
import at.jku.dke.sqlm.parser.*;

/**
*Optimizer class: 
*
*The optimizer runs throw the provided AST and
*checks for statements which can be merged to
*one single BULK statement.
*/

public class Optimizer implements SQLMParserTreeConstants{
	
	public Optimizer(){}
	
	public void run(SQLMNode sn){
		this.optimizeUpdateMultilevelObject(sn);
		this.optimizeCreateMultilevelObject(sn);
		this.optimizeUpdateMultilevelFact(sn);
		this.optimizeCreateMultilevelFact(sn);
		this.clearTree(sn);
	}
	

	/**
	 * Method checks for CreateMultilevelObject statements which
	 * can be concentrated to one BULK CreateMultilevelObject statement 
	 */
	private void optimizeCreateMultilevelObject(SQLMNode sn){
		HashMap<String, ArrayList<Integer>> options = new HashMap<String, ArrayList<Integer>>();
		ArrayList<String> keys = new ArrayList<String>();
		StringBuffer currentKey = new StringBuffer();
		SQLMNode currentNode;
		Integer grouping = 0;
		String lastkey = "";
		//generate BULK feature out of the statement values for identifying associated statements
		for(int i=0;i<sn.jjtGetNumChildren();i++) {
			currentNode = (SQLMNode)sn.jjtGetChild(i);
			if(currentNode instanceof ASTCreateMultilevelObject){
				currentKey.setLength(0);
				//add Dimension
				currentKey.append((String)currentNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).
						jjtGetChild(ASTDimensionHierarchyID.class).jjtGetValue());
				//add TopLevel
				currentKey.append((String)currentNode.jjtGetChild(ASTMultilevelObjectLevelID.class).jjtGetValue());
				//add Parents if existing
				if(currentNode.jjtGetChild(ASTMultilevelObjectParents.class) != null){
					SQLMNode parent;
					for(int j=0;j<currentNode.jjtGetChild(ASTMultilevelObjectParents.class).jjtGetNumChildren();j++){
						parent = currentNode.jjtGetChild(ASTMultilevelObjectParents.class).jjtGetChild(j);
						currentKey.append((String)parent.jjtGetValue());
					}
				}
				//add LevelHierarchy if existing
				if(currentNode.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class) != null){
					SQLMNode levelDefinition;
					for(int j=0;j<currentNode.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetNumChildren();j++){
						levelDefinition = currentNode.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(j);
						currentKey.append((String)levelDefinition.jjtGetChild(ASTMultilevelObjectLevelID.class).jjtGetValue());
						for(int h=0;h<levelDefinition.jjtGetChild(ASTMultilevelObjectLevelParentLevels.class).jjtGetNumChildren();h++){
							currentKey.append((String)(levelDefinition.jjtGetChild(ASTMultilevelObjectLevelParentLevels.class).jjtGetChild(h)).jjtGetValue());
						}
					}
				}
				//add grouping as only statements next to each other shall be bundled
				if(!lastkey.equals(currentKey.toString())){
					grouping++;
				}
				lastkey = currentKey.toString();
				currentKey.append(grouping);
				//save keys
				if(options.containsKey(currentKey.toString())){
					options.get(currentKey.toString()).add(i);
				}else{
					keys.add(currentKey.toString());
					ArrayList<Integer> list = new ArrayList<Integer>();
					list.add(i);
					options.put(currentKey.toString(), list);
				}
			}
		}
		ArrayList<SQLMNode> bulk_stmts = new ArrayList<SQLMNode>();
		ArrayList<Integer> bulk_stmts_pos = new ArrayList<Integer>();
		//check HashMap for bulk opportunities
		for(int i=0;i<keys.size();i++){
			//if a key is related to more than one statement a bulk-statement will be created
			if( options.get(keys.get(i)).size()>1 ){
				ArrayList<Integer> bulk = options.get(keys.get(i));
				//create and fill-up the bulk statement
				ASTBulkCreateMultilevelObject bcmlo = new ASTBulkCreateMultilevelObject(JJTBULKCREATEMULTILEVELOBJECT);
				for(int j=0;j<bulk.size();j++){
					bcmlo.jjtAddChild(sn.jjtGetChild( bulk.get(j) ), bcmlo.jjtGetNumChildren());
					sn.jjtSetNodeNull( bulk.get(j) );
				}
				bulk_stmts.add(bcmlo);
				bulk_stmts_pos.add(bulk.get(0));
			}
		}
		int aberration = 0;
		//insert the bulk statements into the AST
		for(int i=0;i<bulk_stmts.size();i++){
			if( (bulk_stmts_pos.get(i)+aberration)<sn.jjtGetNumChildren()){
				sn.jjtInsertChild(bulk_stmts.get(i), bulk_stmts_pos.get(i)+aberration);
			}else{
				sn.jjtAddChild(bulk_stmts.get(i), sn.jjtGetNumChildren());
			}
			aberration++;
		}
		//delete nodes from the AST which are now inside the bulk statement
		sn.jjtClearEmptyChildNodes();
	}
	
	/**
	 * Method checks for CreateMultilevelFact statements which
	 * can be concentrated to one BULK CreateMultilevelFact statement
	 */
	private void optimizeCreateMultilevelFact(SQLMNode sn){
		HashMap<String, ArrayList<Integer>> options = new HashMap<String, ArrayList<Integer>>();
		ArrayList<String> keys = new ArrayList<String>();
		StringBuffer currentKey = new StringBuffer();
		SQLMNode currentNode;
		Integer grouping = 0;
		Integer lastPos = 0;
		String lastkey = "";
		//generate BULK feature out of the statement values for identifying associated statements
		for(int i=0;i<sn.jjtGetNumChildren();i++) {
			currentNode = sn.jjtGetChild(i);
			if(currentNode instanceof ASTCreateMultilevelFact){
				currentKey.setLength(0);
				//cube ID
				currentKey.append((String)currentNode.jjtGetChild(ASTMultilevelFactID.class).
						jjtGetChild(ASTMultilevelCubeID.class).jjtGetValue());
				//cube coordinate amount
				SQLMNode cubeCoordinate = currentNode.jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeCoordinate.class);
				currentKey.append("coordinateAmount");
				currentKey.append(cubeCoordinate.jjtGetNumChildren());
				//m-fact hierarchy
				if(currentNode.jjtGetChild(ASTMultilevelFactConnectionLevelHierarchy.class) != null){
					currentKey.append("HRY");
				}
				//add grouping as only statements next to each other shall be bundled
				if(!lastkey.equals(currentKey.toString()) || lastPos != i-1){
					grouping++;
				}
				lastkey = currentKey.toString();
				currentKey.append(grouping);
				lastPos = i;
				//save keys into HashMap
				if(options.containsKey(currentKey.toString())){
					options.get(currentKey.toString()).add(i);
				}else{
					keys.add(currentKey.toString());
					ArrayList<Integer> list = new ArrayList<Integer>();
					list.add(i);
					options.put(currentKey.toString(), list);
				}
			}
		}
		ArrayList<SQLMNode> bulk_stmts = new ArrayList<SQLMNode>();
		ArrayList<Integer> bulk_stmts_pos = new ArrayList<Integer>();
		//check HashMap for bulk opportunities
		for(int i=0;i<keys.size();i++){
			if( options.get(keys.get(i)).size()>1 ){
				ArrayList<Integer> bulk = options.get(keys.get(i));
				//create and fill-up the bulk statement
				ASTBulkCreateMultilevelFact bcmlf = new ASTBulkCreateMultilevelFact(JJTBULKCREATEMULTILEVELFACT);
				for(int j=0;j<bulk.size();j++){
					bcmlf.jjtAddChild(sn.jjtGetChild( bulk.get(j) ), bcmlf.jjtGetNumChildren());
					sn.jjtSetNodeNull( bulk.get(j) );
				}
				bulk_stmts.add(bcmlf);
				bulk_stmts_pos.add(bulk.get(0));
			}
		}
		int aberration = 0;
		//insert the bulk statements into the AST
		for(int i=0;i<bulk_stmts.size();i++){
			if( (bulk_stmts_pos.get(i)+aberration)<sn.jjtGetNumChildren()){
				sn.jjtInsertChild(bulk_stmts.get(i), bulk_stmts_pos.get(i)+aberration);
			}else{
				sn.jjtAddChild(bulk_stmts.get(i), sn.jjtGetNumChildren());
			}
			aberration++;
		}
		//delete nodes from the AST which are now inside the bulk statement
		sn.jjtClearEmptyChildNodes();
	}
	
	/**
	 * Method checks for SET attribute parts in CreateMultilevelObject statements 
	 * and UpdateMultilevelObject statements which can be concentrated 
	 * to one BULK UpdateMultilevelObject statement
	 */
	private void optimizeUpdateMultilevelObject(SQLMNode sn){
		HashMap<String, ArrayList<Indices>> options = new HashMap<String, ArrayList<Indices>>();
		ArrayList<String> keys = new ArrayList<String>();
		StringBuffer currentKey = new StringBuffer();
		StringBuffer tempkey = new StringBuffer();
		SQLMNode currentNode;
		//generate BULK feature out of the statement values for identifying associated statements
		for(int i=0;i<sn.jjtGetNumChildren();i++) {
			currentNode = sn.jjtGetChild(i);
			SQLMNode attrValueBlock = currentNode.jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class);
			if(currentNode instanceof ASTUpdateMultilevelObject || 
					(currentNode instanceof ASTCreateMultilevelObject && attrValueBlock != null )){
				for(int j=0;j<attrValueBlock.jjtGetNumChildren();j++){
					currentKey.setLength(0);
					currentKey = new StringBuffer();
					//dimension id
					currentKey.append((String)currentNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).
							jjtGetChild(ASTDimensionHierarchyID.class).jjtGetValue());
					//attribute name
					SQLMNode attribute = attrValueBlock.jjtGetChild(j);
					if(attribute.jjtGetChild(ASTMetalevelID.class) == null){
						currentKey.append( (String)attribute.jjtGetChild(ASTMultilevelObjectAttributeID.class).jjtGetValue() );
						//currentKey.append(attribute.jjtGetNumChildren());
					}else{
						currentKey.append(i);
						currentKey.append(j);
					}
					
					//save keys into HashMap
					if(options.containsKey(currentKey.toString())){
						//get last inserted statement position for the current key and check if they are neighbors
						if((i-1) == options.get(currentKey.toString()).get(options.get(currentKey.toString()).size()-1).getStatementPosition() ){
							Indices indi = new Indices(i,j);
							options.get(currentKey.toString()).add(indi);
						}else{
							ArrayList<Indices> list = new ArrayList<Indices>();
							tempkey = new StringBuffer();
							tempkey.append(currentKey.toString());
							tempkey.append(i);
							list = options.get(currentKey.toString());
							options.remove(currentKey.toString());
							options.put(tempkey.toString(), list);
							keys.add(tempkey.toString());
							
							list = new ArrayList<Indices>();
							Indices indices = new Indices(i,j);
							list.add(indices);
							options.put(currentKey.toString(), list);
						}
					}else{
						keys.add(currentKey.toString());
						ArrayList<Indices> list = new ArrayList<Indices>();
						Indices indices = new Indices(i,j);
						list.add(indices);
						options.put(currentKey.toString(), list);
					}
				}
			}
		}
		
		ArrayList<SQLMNode> createdBulkStmt = new ArrayList<SQLMNode>();
		ArrayList<Integer> createdBulkStmtPos = new ArrayList<Integer>();
		//check HashMap (options) for bulk opportunities
		for(int i=0;i<keys.size();i++){
			if( options.get(keys.get(i)).size()>1 ){
				ArrayList<Indices> bulk = options.get(keys.get(i));
				ASTBulkUpdateMultilevelObject bumlo = new ASTBulkUpdateMultilevelObject(JJTBULKUPDATEMULTILEVELOBJECT);
				bumlo.setLengthOfChildren(bulk.size());
				createdBulkStmt.add(bumlo);
				createdBulkStmtPos.add(bulk.get(bulk.size()-1).getStatementPosition());
				for(int j=0;j<bulk.size();j++){
					currentNode = sn.jjtGetChild(bulk.get(j).getStatementPosition());
					SQLMNode attributeBlock = currentNode.jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class);
					if(sn.jjtGetChild( bulk.get(j).getStatementPosition() ) instanceof ASTUpdateMultilevelObject &&
							attributeBlock.jjtGetNumChildren()==1 ){
						bumlo.jjtAddChildBlind(sn.jjtGetChild( bulk.get(j).getStatementPosition() ), j);
						sn.jjtSetNodeNull( bulk.get(j).getStatementPosition() );
					}else{
						int attributePos = (bulk.get(j).getValuePosition());
						ASTUpdateMultilevelObject udo = new ASTUpdateMultilevelObject(JJTUPDATEMULTILEVELOBJECT);
						udo.jjtSetErrorLine(currentNode.jjtGetErrorLine());
						udo.setLengthOfChildren(2);
						ASTMultilevelObjectQualifiedID id = new ASTMultilevelObjectQualifiedID(JJTMULTILEVELOBJECTQUALIFIEDID);
						
						ASTMultilevelObjectUnqualifiedID dim_obj = new ASTMultilevelObjectUnqualifiedID(JJTMULTILEVELOBJECTUNQUALIFIEDID);
						ASTDimensionHierarchyID dim_hry = new ASTDimensionHierarchyID(JJTDIMENSIONHIERARCHYID);
						dim_obj.jjtSetValue((currentNode.jjtGetChild(0).jjtGetChild(0)).jjtGetValue());
						dim_hry.jjtSetValue(currentNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class).jjtGetChild(ASTDimensionHierarchyID.class).jjtGetValue());
						id.jjtAddChild(dim_obj, 0);
						id.jjtAddChild(dim_hry, 1);
						udo.jjtAddChildBlind(id, 0);
						
						ASTMultilevelObjectAttributeValueAssignment doava = (ASTMultilevelObjectAttributeValueAssignment)attributeBlock.jjtGetChild(attributePos);
						
						ASTMultilevelObjectAttributeValueBlock doavb = new ASTMultilevelObjectAttributeValueBlock(JJTMULTILEVELOBJECTATTRIBUTEVALUEBLOCK);
						doavb.jjtAddChild(doava, 0);
						
						udo.jjtAddChildBlind(doavb, 1);
						bumlo.jjtAddChildBlind(udo, j);
						//delete the reallocated attributes
						attributeBlock.jjtSetNodeNull(attributePos);
					}
				}
			}
		}
		//delete attribute values which are now inside the BULK statement
		for(int i=0;i<sn.jjtGetNumChildren();i++){
			currentNode = sn.jjtGetChild(i);
			if(currentNode instanceof ASTUpdateMultilevelObject || currentNode instanceof ASTCreateMultilevelObject){
				SQLMNode attributeBlock = currentNode.jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class);
				if(attributeBlock != null){ //currentNode instanceof ASTUpdateMultilevelObject && 
					attributeBlock.jjtClearEmptyChildNodes();
					//check for empty tree sections, delete when empty
					if(attributeBlock.jjtGetNumChildren()==0){
						currentNode.jjtDeleteChild(currentNode.jjtGetNumChildren()-1);
					}
				}
			}
			if(currentNode instanceof ASTUpdateMultilevelObject && currentNode.jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class) == null){
				if(currentNode.jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){
					currentNode.jjtDeleteChild(currentNode.jjtGetNumChildren()-1);
					sn.jjtSetNodeNull(i);
				}
			}
		}
		//insert the bulk statements into the AST
		int aberration = 1;
		for(int i=0;i<createdBulkStmt.size();i++){
			if( ((createdBulkStmtPos.get(i))+aberration)<sn.jjtGetNumChildren()){
				sn.jjtInsertChild(createdBulkStmt.get(i), ((createdBulkStmtPos.get(i))+aberration));
			}else{
				sn.jjtAddChild(createdBulkStmt.get(i), sn.jjtGetNumChildren());
			}
			aberration++;
		}
		//delete nodes which are now inside the bulk statement
		sn.jjtClearEmptyChildNodes();
	}
	
	/**
	 * Method checks for SET measure parts in CreateMultilevelFact statements 
	 * and UpdateMultilevelFact statements which can be concentrated 
	 * to one BULK UpdateMultilevelFact statement
	 */
	private void optimizeUpdateMultilevelFact(SQLMNode sn){
		HashMap<String, ArrayList<Indices>> options = new HashMap<String, ArrayList<Indices>>();
		ArrayList<String> keys = new ArrayList<String>();
		StringBuffer currentKey = new StringBuffer();
		SQLMNode currentNode;
		//generate BULK feature out of the statement values for identifying associated statements
		for(int i=0;i<sn.jjtGetNumChildren();i++) {
			currentNode = sn.jjtGetChild(i);
			SQLMNode measureValueBlock = currentNode.jjtGetChild(ASTMultilevelFactMeasureValueBlock.class);
			if(currentNode instanceof ASTUpdateMultilevelFact || 
					(currentNode instanceof ASTCreateMultilevelFact && measureValueBlock != null )){
				for(int j=0;j<measureValueBlock.jjtGetNumChildren();j++){
					currentKey.setLength(0);
					//cube id
					currentKey.append((String)currentNode.jjtGetChild(ASTMultilevelFactID.class).
							jjtGetChild(ASTMultilevelCubeID.class).jjtGetValue());
					//measure name
					SQLMNode measure = measureValueBlock.jjtGetChild(j);
					if(measure.jjtGetChild(ASTMetalevelID.class) == null){
						currentKey.append( (String)measure.jjtGetChild(ASTMultilevelFactMeasureID.class).jjtGetValue() );
						currentKey.append(measure.jjtGetNumChildren());
					}else{
						currentKey.append(i);
						currentKey.append(j);
					}
					//insert into HashMap
					if(options.containsKey(currentKey.toString())){
						if((i-1) == options.get(currentKey.toString()).get(options.get(currentKey.toString()).size()-1).getStatementPosition() ){
							Indices indi = new Indices(i,j);
							options.get(currentKey.toString()).add(indi);
						}else{
							StringBuffer tempkey = new StringBuffer();
							ArrayList<Indices> list = new ArrayList<Indices>();
							tempkey.append(currentKey.toString());
							tempkey.append(i);
							list = options.get(currentKey.toString());
							options.remove(currentKey.toString());
							options.put(tempkey.toString(), list);
							//added
							keys.add(tempkey.toString());
							
							list = new ArrayList<Indices>();
							Indices indices = new Indices(i,j);
							list.add(indices);
							options.put(currentKey.toString(), list);
						}
					}else{
						keys.add(currentKey.toString());
						ArrayList<Indices> list = new ArrayList<Indices>();
						Indices indi = new Indices(i,j);
						list.add(indi);
						options.put(currentKey.toString(), list);
					}
				}
			}
		}
		ArrayList<SQLMNode> createdBulkStmt = new ArrayList<SQLMNode>();
		ArrayList<Integer> createdBulkStmtPos = new ArrayList<Integer>();
		//check HashMap for bulk opportunities
		for(int i=0;i<keys.size();i++){
			if( options.get(keys.get(i)).size()>1 ){
				ArrayList<Indices> bulk = options.get(keys.get(i));
				ASTBulkUpdateMultilevelFact bumlf = new ASTBulkUpdateMultilevelFact(JJTBULKUPDATEMULTILEVELFACT);
				bumlf.setLengthOfChildren(bulk.size());
				createdBulkStmt.add(bumlf);
				createdBulkStmtPos.add(bulk.get(bulk.size()-1).getStatementPosition());
				for(int j=0;j<bulk.size();j++){
					currentNode = sn.jjtGetChild( bulk.get(j).getStatementPosition() );
					SQLMNode measureBlock = currentNode.jjtGetChild(ASTMultilevelFactMeasureValueBlock.class);
					if(sn.jjtGetChild( bulk.get(j).getStatementPosition() ) instanceof ASTUpdateMultilevelObject &&
							measureBlock.jjtGetNumChildren()==1 ){
						bumlf.jjtAddChildBlind(sn.jjtGetChild( bulk.get(j).getStatementPosition() ), j);
						sn.jjtSetNodeNull( bulk.get(j).getValuePosition() );
					}else{
						int measurePos = bulk.get(j).getValuePosition();
						ASTUpdateMultilevelFact umlf = new ASTUpdateMultilevelFact(JJTUPDATEMULTILEVELFACT);
						umlf.setLengthOfChildren(2);
						umlf.jjtSetErrorLine( currentNode.jjtGetErrorLine() );
						ASTMultilevelFactID mlfid = new ASTMultilevelFactID(JJTMULTILEVELFACTID);
						//generate MultilevelFactID
						ASTMultilevelCubeCoordinate mlcc = new ASTMultilevelCubeCoordinate(JJTMULTILEVELCUBECOORDINATE);
						SQLMNode cubeCoordinate = currentNode.jjtGetChild(0).jjtGetChild(0);
						for(int n=0;n<cubeCoordinate.jjtGetNumChildren();n++){
							ASTMultilevelObjectUnqualifiedID dim_obj = new ASTMultilevelObjectUnqualifiedID(JJTMULTILEVELOBJECTUNQUALIFIEDID);
							if(cubeCoordinate.jjtGetChild(n).jjtGetNumChildren()>1){
								ASTMultilevelObjectQualifiedID id = new ASTMultilevelObjectQualifiedID(JJTMULTILEVELOBJECTQUALIFIEDID);
								ASTDimensionHierarchyID dim_hry = new ASTDimensionHierarchyID(JJTDIMENSIONHIERARCHYID);
								dim_hry.jjtSetValue((cubeCoordinate.jjtGetChild(n).jjtGetChild(1)).jjtGetValue());
								dim_obj.jjtSetValue((cubeCoordinate.jjtGetChild(n).jjtGetChild(0)).jjtGetValue());
								id.jjtAddChild(dim_obj, 0);
								id.jjtAddChild(dim_hry, 1);
								mlcc.jjtAddChild(id, n);
							}else{
								dim_obj.jjtSetValue((cubeCoordinate.jjtGetChild(n)).jjtGetValue());
								mlcc.jjtAddChild(dim_obj, n);
							}
						}
						mlfid.jjtAddChild(mlcc, 0);
						ASTMultilevelCubeID mlcid = new ASTMultilevelCubeID(JJTMULTILEVELCUBEID);
						mlcid.jjtSetValue((currentNode.jjtGetChild(0).jjtGetChild(1)).jjtGetValue());
						mlfid.jjtAddChild(mlcid, 1);
						umlf.jjtAddChildBlind(mlfid, 0);
						//Measure hinzufügen
						ASTMultilevelFactMeasureValueAssignment mlfmv = 
							(ASTMultilevelFactMeasureValueAssignment) measureBlock.jjtGetChild(measurePos);
						ASTMultilevelFactMeasureValueBlock mlfmvb = new ASTMultilevelFactMeasureValueBlock(JJTMULTILEVELFACTMEASUREVALUEBLOCK);
						
						mlfmvb.jjtAddChild(mlfmv, 0);
						umlf.jjtAddChildBlind(mlfmvb, 1);
						bumlf.jjtAddChildBlind(umlf, j);
						//löschen der verschobenen Measures
						measureBlock.jjtSetNodeNull(measurePos);
					}
				}
			}
		}
		//delete measure values which are now inside the BULK statement
		for(int i=0;i<sn.jjtGetNumChildren();i++){
			currentNode = sn.jjtGetChild(i);
			if(currentNode instanceof ASTUpdateMultilevelFact || currentNode instanceof ASTCreateMultilevelFact){
				SQLMNode measureBlock = currentNode.jjtGetChild(ASTMultilevelFactMeasureValueBlock.class);
				if(measureBlock != null){
					measureBlock.jjtClearEmptyChildNodes();
					//check for empty tree sections, delete when empty
					if(measureBlock.jjtGetNumChildren()==0){
						currentNode.jjtDeleteChild(currentNode.jjtGetNumChildren()-1);
					}
				}
			}
		}
		//insert the bulk statements into the AST
		int aberration = 1;
		for(int i=0;i<createdBulkStmt.size();i++){
			if( (createdBulkStmtPos.get(i)+aberration)<sn.jjtGetNumChildren()){
				sn.jjtInsertChild(createdBulkStmt.get(i), createdBulkStmtPos.get(i)+aberration);
			}else{
				sn.jjtAddChild(createdBulkStmt.get(i), sn.jjtGetNumChildren());
			}
			aberration++;
		}
		//delete nodes which are now inside the bulk statement
		sn.jjtClearEmptyChildNodes();
	}
	
	private void clearTree(SQLMNode sn) {
		for(int i=0;i<sn.jjtGetNumChildren();i++) {
			if(sn.jjtGetChild(i) instanceof ASTUpdateMultilevelFact && 
					sn.jjtGetChild(ASTMultilevelFactMeasureValueBlock.class) == null){
				sn.jjtDeleteChild(i);
				i--;
			}else if(sn.jjtGetChild(i) instanceof ASTUpdateMultilevelObject && 
					sn.jjtGetChild(ASTMultilevelObjectAttributeValueBlock.class) == null){
				sn.jjtDeleteChild(i);
				i--;
			}
		}
	}
	
	/**
	 * Intern class for storing the position of a sub-tree
	 * inside of a statement-AST
	 */
	private class Indices {
        int stmtPos;
        int valuePos;
        
        public Indices(int i, int j) {
        	stmtPos = i;
        	valuePos = j;
		}
        public int getStatementPosition(){
        	return stmtPos;
        }
        public int getValuePosition(){
        	return valuePos;
        }
    }
	
}
