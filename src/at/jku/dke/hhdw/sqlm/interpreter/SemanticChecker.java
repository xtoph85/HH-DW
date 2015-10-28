package at.jku.dke.sqlm.interpreter;

/*
 * SemanticChecker
 * 
 * Version information
 *
 * 06.05.2010
 * 
 * Copyright by Thomas Pecksteiner
 */

import java.util.ArrayList;
import java.util.LinkedList;

import at.jku.dke.sqlm.exceptions.LevelHierarchyException;
import at.jku.dke.sqlm.parser.*;

/** 
*
*The SemanticChecker class provides certain consistency checks
*which checks if the provided information is translatable. Also
*provides the class a method which fills up the missing parts of
*level hierarchies when the user specified them only in abbreviated
*form.
*
*/

public class SemanticChecker implements SQLMParserTreeConstants {

	DataDictionary dataDict;
	DataAccess dataAccess;
	
	public SemanticChecker(){}
	
	/**
	 * calls the different semantic checks of the class in the correct order
	 * @param sn is the root SQLMNode(sn) of the assigned statement
	 * @throws Exception 
	 */
	public void run(SQLMNode sn) throws Exception{
		this.levelHierarchyLoopCheck(sn);
		this.completeLevelHierarchy(sn);
		this.levelHierarchyConsistencyCheck(sn);
	}
	
	/**
	 * completes a level hierarchy which is defined in abbreviated form
	 * @param sn is the root SQLMNode(sn) of the assigned statement
	 * @throws Exception 
	 */
	private void completeLevelHierarchy(SQLMNode sn) throws Exception{
		dataDict = DataDictionary.getDataDictionary();
		dataAccess = DataAccess.getDataAccessMgr();
		StringBuffer sb = new StringBuffer();
		SQLMNode stmt;
		int lvlHryNode = 2;
		
		if(sn instanceof ASTBulkCreateMultilevelObject ){
			stmt = (SQLMNode) sn.jjtGetChild(0);
		}else{
			stmt = sn;
		}
		if(stmt.jjtGetChild(ASTMultilevelObjectParents.class) != null){
			sb.setLength(0);
			sb.append("SELECT * \nFROM TABLE(mobject_");
			String dimId = dataDict.getDimensionHierarchyId( (String)(stmt.jjtGetChild(0).jjtGetChild(1)).jjtGetValue() );
			sb.append(dimId);
			sb.append("_ty.calculate_inherited_levels(");
					
			//Parents
			if(stmt.jjtGetChild(ASTMultilevelObjectParents.class) != null ){
				sb.append("names_tty('");
				for(int j=0;j<stmt.jjtGetChild(ASTMultilevelObjectParents.class).jjtGetNumChildren();j++){
					sb.append((String)(stmt.jjtGetChild(ASTMultilevelObjectParents.class).jjtGetChild(j)).jjtGetValue());
					if(j+1<stmt.jjtGetChild(ASTMultilevelObjectParents.class).jjtGetNumChildren()){
						sb.append("', '");
					}
				}
				sb.append("'),\n");
				lvlHryNode++;
			}
					
			//Toplevel
			sb.append("'");
			sb.append((String)(stmt.jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue());
			sb.append("',\n");
					
			//LevelHry of the current Statement
			sb.append("level_hierarchy_tty(");
			if(stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class) != null ){
				SQLMNode hierarchy = stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class);
				for(int j=0;j<hierarchy.jjtGetNumChildren();j++){
					sb.append("level_hierarchy_ty('");
					sb.append((String)(hierarchy.jjtGetChild(j).jjtGetChild(0)).jjtGetValue());
					if(hierarchy.jjtGetChild(j).jjtGetChild(1).jjtGetNumChildren() == 0){
						sb.append("', 'NULL')");
					}else{
						sb.append("', '");
						sb.append((String)(hierarchy.jjtGetChild(j).jjtGetChild(1).jjtGetChild(0)).jjtGetValue());
						sb.append("')");
						if(hierarchy.jjtGetChild(j).jjtGetChild(1).jjtGetNumChildren() > 1){
							for(int k=1;k<hierarchy.jjtGetChild(j).jjtGetChild(1).jjtGetNumChildren();k++){
								sb.append(", level_hierarchy_ty('");
								sb.append((String)(hierarchy.jjtGetChild(j).jjtGetChild(0)).jjtGetValue());
								sb.append("', '");
								sb.append((String)(hierarchy.jjtGetChild(j).jjtGetChild(1).jjtGetChild(k)).jjtGetValue());
								sb.append("')");
							}
						}
					}
					if(j+1<hierarchy.jjtGetNumChildren()){
						sb.append(", ");
					}
				}
			}
			sb.append(")))");
					
			LinkedList<ArrayList<Object>> lvl_hry = dataAccess.executeQueryReturnList(sb.toString());
			if(lvl_hry.size() == 0){
				throw new Exception("Exception: level hierarchy fillup failed");
			}
			//some or whole LevelHry is existing -> fill up the missing parts if necessary
			if(stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class) != null ){ 
				SQLMNode hierarchy = stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class);
				//if the amount of levelhry-definitions of the tree is lower than the overall amount 
				if(stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetNumChildren() != lvl_hry.size()){
					//System.out.println("Start filling up the LevelHry");
					for(int j=0;j<lvl_hry.size();j++){
						//check if the levelhry-definition is missing in the tree
						boolean missing = true;
						for(int k=0;k<stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetNumChildren();k++){
							if( ((String)lvl_hry.get(j).get(0)).equals( (String)(hierarchy.jjtGetChild(k).jjtGetChild(0)).jjtGetValue() ) ){
								missing = false;
							}
						}
						if(missing){
							if(j>0 && ((String)lvl_hry.get(j).get(0)).equals( (String)lvl_hry.get(j-1).get(0) )){
								ASTMultilevelObjectLevelID dolplid = new ASTMultilevelObjectLevelID(15);
								dolplid.jjtSetValue((String)lvl_hry.get(j).get(1));
								hierarchy.jjtGetChild(hierarchy.jjtGetNumChildren()-1).
									jjtGetChild(1).jjtAddChild(dolplid, hierarchy.jjtGetChild(hierarchy.jjtGetNumChildren()-1).jjtGetChild(1).jjtGetNumChildren());
							}else{
								ASTMultilevelObjectLevelDefinition dold = new ASTMultilevelObjectLevelDefinition(27);
								ASTMultilevelObjectLevelID dolid = new ASTMultilevelObjectLevelID(15);
								dolid.jjtSetValue((String)lvl_hry.get(j).get(0));
								ASTMultilevelObjectLevelParentLevels dolpl = new ASTMultilevelObjectLevelParentLevels(28);
								ASTMultilevelObjectLevelID dolplid = new ASTMultilevelObjectLevelID(15);
								dolplid.jjtSetValue((String)lvl_hry.get(j).get(1));
								dold.jjtAddChild(dolid, 0);
								dold.jjtAddChild(dolpl, 1);
								dolpl.jjtAddChild(dolplid, 0);
								((SQLMNode)stmt.jjtGetChild(lvlHryNode)).jjtInsertChild(dold, j);
							}
						}
					}
				}
						
			}else{ //no LevelHry in the current tree-section of the ASTDocument -> fill in whole LevelHry
				int variance = 0;
				for(int j=0;j<lvl_hry.size();j++){
					if(j==0){
						ASTMultilevelObjectLevelHierarchy mlolh = new ASTMultilevelObjectLevelHierarchy(31);
						stmt.jjtAddChildFlexible(mlolh, lvlHryNode);
					}
					//current LevelHry-section has the same ID as the last one -> insert the parentID at the last LevelHry
					if(j>0 && ((String)lvl_hry.get(j).get(0)).equals( (String)lvl_hry.get(j-1).get(0) )){
						variance++;
						ASTMultilevelObjectLevelID dolplid = new ASTMultilevelObjectLevelID(17);
						dolplid.jjtSetValue((String)lvl_hry.get(j).get(1));
						(stmt.jjtGetChild(lvlHryNode).jjtGetChild(j-variance).jjtGetChild(1)).jjtAddChild(
										dolplid, stmt.jjtGetChild(lvlHryNode).jjtGetChild(j-variance).jjtGetChild(1).jjtGetNumChildren());
					}else{ //new LevelHry-section
						ASTMultilevelObjectLevelDefinition dold = new ASTMultilevelObjectLevelDefinition(32);
						ASTMultilevelObjectLevelID dolid = new ASTMultilevelObjectLevelID(17);
						dolid.jjtSetValue((String)lvl_hry.get(j).get(0));
						ASTMultilevelObjectLevelParentLevels dolpl = new ASTMultilevelObjectLevelParentLevels(30);
						ASTMultilevelObjectLevelID dolplid = new ASTMultilevelObjectLevelID(17);
						dolplid.jjtSetValue((String)lvl_hry.get(j).get(1));
						dold.jjtAddChild(dolid, 0);
						dold.jjtAddChild(dolpl, 1);
						dolpl.jjtAddChild(dolplid, 0);
						stmt.jjtGetChild(lvlHryNode).jjtAddChild(dold, stmt.jjtGetChild(lvlHryNode).jjtGetNumChildren());
					}
				}
			}
		}
	}
	
	/**
	 * checks if the level hierarchy contains each parent-level of a 
	 * defined level, except of the top-level
	 * @param sn is the root SQLMNode(sn) of the assigned statement
	 * @throws Exception 
	 */
	private void levelHierarchyConsistencyCheck(SQLMNode sn) throws LevelHierarchyException{
		SQLMNode stmt;
		if(sn instanceof ASTBulkCreateMultilevelObject ){
			stmt = (SQLMNode) sn.jjtGetChild(0);
		}else{
			stmt = sn;
		}
		//top-level of the level hierarchy
		String topLevel = (String)(stmt.jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue();
		//run through all the level-hierarchy definitions and check for inconsistencies
		for(int j=0;j<stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetNumChildren();j++){
			//top-level is an exception
			if(!topLevel.equals( (String)(stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(j).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue() )){
				boolean consistent = false;
				//parent level-hierarchy definitions
				for(int k=0;k<stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(j).jjtGetChild(ASTMultilevelObjectLevelParentLevels.class).jjtGetNumChildren();k++){
					//level-hierarchy definitions to compare with
					for(int l=0;l<stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetNumChildren();l++){
						if( ((String)(stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(l).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue()).equals
								( (String)(stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(j).jjtGetChild(ASTMultilevelObjectLevelParentLevels.class).jjtGetChild(k)).jjtGetValue() ) ){
							consistent = true;
						}
					}
				}
				if(!consistent){
					throw new LevelHierarchyException("level hierarchy is inconsistent");
				}
			}
		}
	}
	
	/**
	 * checks if the level hierarchy contains loops
	 * @param sn is the root SQLMNode(sn) of the assigned statement
	 * @throws Exception 
	 */
	private void levelHierarchyLoopCheck(SQLMNode sn) throws LevelHierarchyException{
		SQLMNode stmt;
		if(sn instanceof ASTBulkCreateMultilevelObject ){
			stmt = (SQLMNode) sn.jjtGetChild(0);
		}else{
			stmt = sn;
		}
		if(stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class) != null){
			//run through the MultilevelObjectLevelDefinitions
			for(int j=0;j<stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetNumChildren();j++){
				String startAttrVal = (String)((SQLMNode)stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(j).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue();
				//run through MultilevelObjectLevelParentLevels of each MultilevelObjectLevelDefinition
				for(int k=0;k<stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(j).jjtGetChild(ASTMultilevelObjectLevelParentLevels.class).jjtGetNumChildren();k++){
					String currentAttrVal = (String)((SQLMNode)stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(j).jjtGetChild(ASTMultilevelObjectLevelParentLevels.class).jjtGetChild(k)).jjtGetValue();
					//run through the MultilevelObjectLevelDefinitions to compare with the loopSearch-method
					this.loopSearch(stmt, startAttrVal, currentAttrVal);
				}
			}
		}
	}
	
	/**
	 * recursive method which checks for loops inside of level-hierarchies
	 * @param stmt is the root SQLMNode of the assigned statement
	 * @param startAttrVal String which holds the name of the level to compare with
	 * @param currentAttrVal String which holds the name of the current level
	 * @throws Exception 
	 */
	private boolean loopSearch(SQLMNode stmt, String startAttrVal, String currentAttrVal) throws LevelHierarchyException{
		boolean found = true;
		boolean loop = false;
		while(found && (!loop)){
			found = false;
			for(int i=0;i<stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetNumChildren();i++){
				if(currentAttrVal.equals( (String)(stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue() )){
					found = true;
					//only one parent level-hierarchy exists
					if( stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelParentLevels.class).jjtGetNumChildren() < 2 ){
						if(stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelParentLevels.class).jjtGetNumChildren() == 1){
							currentAttrVal = (String)(stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelParentLevels.class).jjtGetChild(ASTMultilevelObjectLevelID.class)).jjtGetValue();
						}else{
							currentAttrVal = "";
						}
						if(currentAttrVal.equals(startAttrVal)){
							loop = true;
							break;
						}
					}else{ //more than one parent level-hierarchy exists
						for(int j=0;j<stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelParentLevels.class).jjtGetNumChildren();j++){
							currentAttrVal = (String)(stmt.jjtGetChild(ASTMultilevelObjectLevelHierarchy.class).jjtGetChild(i).jjtGetChild(ASTMultilevelObjectLevelParentLevels.class).jjtGetChild(j)).jjtGetValue();
							if(currentAttrVal.equals(startAttrVal)){
								loop = true;
								break;
							}else {
								if(this.loopSearch(stmt, startAttrVal, currentAttrVal)){
									loop = true;
									break;
								}
								found = false;
							}
						}
					}
				}
			}
		}
		if (loop){
			throw new LevelHierarchyException("level hierarchy contains a loop at level "+startAttrVal, 20002, stmt.jjtGetErrorLine());
		}
		return loop;
	}
	
}
