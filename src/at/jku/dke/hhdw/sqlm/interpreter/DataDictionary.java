package at.jku.dke.sqlm.interpreter;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import at.jku.dke.sqlm.parser.ASTDimensionHierarchyID;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeCoordinate;
import at.jku.dke.sqlm.parser.ASTMultilevelCubeID;
import at.jku.dke.sqlm.parser.ASTMultilevelFactID;
import at.jku.dke.sqlm.parser.ASTMultilevelObjectUnqualifiedID;
import at.jku.dke.sqlm.parser.SQLMNode;

/** 
*
*The DataDictionary class provides methods for getting information of the
*database which is necessary for translating the SQLM statements into
*PL/SQL code. The queried information is also stored locally for a 
*possible later use. To be sure that only one DataDictionary exists at
*the same time, and therefore holds all the information, the DataDictionary
*is implemented with the SingeltonPattern.
*
*/

public class DataDictionary {
	
	private static DataDictionary ref;
	
	private HashMap<String, HashMap<Integer, String>> mCubes;
	private HashMap<String, String> mCubeTypes;
	private HashMap<String, String> mRelTypes;
	private HashMap<String, String> conlevelTypes;
	private HashMap<String, String> dimensionId; 
	private HashMap<String, String> mFactValueTypes;
	private HashMap<String, String> queryviewTypes;
	private int tableNameAffix;
	private DataAccess dataAccess;
	private LinkedList<String> createdTables;
	//private LinkedList<String> updatedCubes;
	private LinkedList<CubeMeasureMapping> updatedCubes;
	
	
	/**
	*Initializes a newly created DataDictionary object so that all
	*local storages are initalized and empty
	*/
	public DataDictionary(){
		mCubes = new HashMap<String, HashMap<Integer, String>>();
		mCubeTypes = new HashMap<String, String>();
		mRelTypes = new HashMap<String, String>();
		conlevelTypes = new HashMap<String, String>();
		dimensionId = new HashMap<String, String>();
		mFactValueTypes = new HashMap<String, String>();
		queryviewTypes = new HashMap<String, String>();
		tableNameAffix = 0;
		createdTables = new LinkedList<String>();
		updatedCubes = new LinkedList<CubeMeasureMapping>();
	}
	
	/**
	*Returns the reference of the DataDictionary
	*/
	public static DataDictionary getDataDictionary() {
		if (ref == null) {
			ref = new DataDictionary();
		}
		return ref;
	}
	
	/**
	*Locally stores the sequence of a cube coordinate
	*@param cube_name Name of the Multilevel Cube from which the cube coordinate will be stored
	*@param sequence The sequence of the cube coordinate
	*/
	public void setSequence(String cube_name, HashMap<Integer, String> sequence){
		mCubes.put(cube_name, sequence);
	}
	
	/**
	*Returns the sequence of a cube coordinate of a specific Multilevel Cube
	*@param cube_name Name of the Multilevel Cube from which the cube coordinate will get returned
	*/
	public HashMap<Integer, String> getSequence(String cube_name){
		return mCubes.get(cube_name);
	}
	
	public String getValueOfSequence(String cube_name, Integer number){
		return mCubes.get(cube_name).get(number);
	}
	
	public Integer getNumberOfValuesOfSequence(String name){
		return mCubes.get(name).size();
	}
	
	public void deleteSequence(String name){
		mCubes.remove(name);
	}
	
	public boolean existsSequence(String name){
		return mCubes.containsKey(name);
	}	

	/**
	*Queries the sequence of a cube coordinate from the database
	*@param cube_name Name of the Multilevel Cube from which the cube coordinate will get returned
	*/
	public void getSequenceFromDatabase(String mcube_name, LinkedList<String> unorderedSequence) throws SQLException{
		String mcube_type = this.getMCubeType(mcube_name);
		String query = "SELECT LINE, TEXT FROM user_source us WHERE us.name = '"+mcube_type.toUpperCase()+"' AND us.type = 'TYPE'";
		dataAccess = DataAccess.getDataAccessMgr();
		LinkedList<ArrayList<Object>> source;
		try {
			source = dataAccess.executeQueryReturnList(query);
		} catch (SQLException e) {
			String reason = e.getMessage()+"\nInterpreter: Error at getting MlObject-sequence from DB";
			throw new SQLException(reason,e.getSQLState(),e.getErrorCode());
		}
		
		HashMap<Integer,String> temp = new HashMap<Integer, String>();
		int line;
		for (int i=0;i<unorderedSequence.size();i++){
			line = source.size();
			for(int j=0;j<source.size();j++){
				if(((String)source.get(j).get(1)).contains((CharSequence)unorderedSequence.get(i)) ){
					if(((BigDecimal)source.get(j).get(0)).intValue()<line ){
						line = ((BigDecimal)source.get(j).get(0)).intValue();
					}
				}
			}
			temp.put(line, unorderedSequence.get(i));
		}
		HashMap<Integer,String> sequence = new HashMap<Integer, String>();
		line = 1;
		for(int i=0;i<source.size();i++){
			if(temp.containsKey(i)){
				sequence.put(line,temp.get(i));
				line++;
			}
		}
		this.mCubes.put(mcube_name, sequence);
	}
	
	/**
	 * Returns the sequence of the cube coordinate of a specific Multilevel Cube from the database
	 * @param mcube_name Name of the Multilevel Cube
	 * @throws SQLException 
	 */
	public void getSequenceFromDatabase(String mcube_name) throws SQLException{
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT x.* FROM mcubes mc, TABLE (VALUE(mc).get_dimension_names()) x ");
		sb.append("WHERE mc.cname = '");
		sb.append(mcube_name);
		sb.append("'");
		dataAccess = DataAccess.getDataAccessMgr();
		LinkedList<ArrayList<Object>> source;
		try {
			source = dataAccess.executeQueryReturnList(sb.toString());
		} catch (SQLException e) {
			String reason = e.getMessage()+"\nInterpreter: Error at getting MObject-sequence from DB";
			throw new SQLException(reason,e.getSQLState(),e.getErrorCode());
		}
		HashMap<Integer,String> sequence = new HashMap<Integer, String>();
		for(int i=0;i<source.size();i++){
			String temp = source.get(i).toString();
			temp = (String) temp.subSequence(1, temp.length()-1);
			sequence.put(i+1, temp);
		}
		this.mCubes.put(mcube_name, sequence);
	}

	/**
	 * Returns the dynamically generated TYPE of an MCUBE from 
	 * the local storage or, if not stored, from the database
	 * @param mcube_name Name of the Multilevel Cube
	 * @throws SQLException 
	 */
	public String getMCubeType(String mcube_name) throws SQLException{
		String mcube_type;
		if(mCubeTypes.containsKey(mcube_name)){
			mcube_type = mCubeTypes.get(mcube_name);
		}else{
			String query = "SELECT MCUBE_#_TY FROM MCUBES mc WHERE mc.cname = '"+mcube_name+"'";
			dataAccess = DataAccess.getDataAccessMgr();
			mcube_type = dataAccess.getConcreteType(query);
			mCubeTypes.put(mcube_name, mcube_type);
		}
		return mcube_type;
	}
	
	/**
	 * Returns the dynamically generated TYPE of an MFACT from 
	 * the local storage or, if not stored, from the database
	 * @param mcube_name Name of the Multilevel Cube
	 * @throws SQLException 
	 */
	public String getMRelType(String mcube_name) throws SQLException{
		String mrel_type;
		if(mRelTypes.containsKey(mcube_name)){
			mrel_type = mRelTypes.get(mcube_name);
		}else{
			String query = "SELECT MREL_#_TY FROM MCUBES mc WHERE mc.cname = '"+mcube_name+"'";
			dataAccess = DataAccess.getDataAccessMgr();
			mrel_type = dataAccess.getConcreteType(query);
			mRelTypes.put(mcube_name, mrel_type);
		}
		return mrel_type;
	}
	
	/**
	 * Returns the dynamically generated TYPE of an MFACT_VALUE from 
	 * the local storage or, if not stored, from the database
	 * @param mcube_name Name of the Multilevel Cube
	 * @throws SQLException 
	 */
	public String getMFactValueType(String mcube_name) throws SQLException{
		String mfact_value_type;
		if(mFactValueTypes.containsKey(mcube_name)){
			mfact_value_type = mFactValueTypes.get(mcube_name);
		}else{
			String query = "SELECT MREL_#_VALUE_TY FROM MCUBES mc WHERE mc.cname = '"+mcube_name+"'";
			dataAccess = DataAccess.getDataAccessMgr();
			mfact_value_type = dataAccess.getConcreteType(query);
			mRelTypes.put(mcube_name, mfact_value_type);
		}
		return mfact_value_type;
	}
	
	/**
	 * Returns the dynamically generated TYPE of an MCUBE CONLEVEL 
	 * from the local storage or, if not stored, from the database
	 * @param mcube_name Name of the Multilevel Cube
	 * @throws SQLException 
	 */
	public String getConlevelType(String mcube_name) throws SQLException{
		String conlevel_type;
		if(conlevelTypes.containsKey(mcube_name)){
			conlevel_type = conlevelTypes.get(mcube_name);
		}else{
			String query = "SELECT CONLEVEL_#_TY FROM MCUBES mc WHERE mc.cname = '"+mcube_name+"'";
			dataAccess = DataAccess.getDataAccessMgr();
			conlevel_type = dataAccess.getConcreteType(query);
			conlevelTypes.put(mcube_name, conlevel_type);
		}
		return conlevel_type;
	}
	
	/**
	 * Returns the dynamically generated TYPE of an MCUBE QUERYVIEW 
	 * from the local storage or, if not stored, from the database
	 * @param mcube_name Name of the Multilevel Cube
	 * @throws SQLException 
	 */
	public String getQueryviewType(String mcube_name) throws SQLException{
		String queryview_type;
		if(queryviewTypes.containsKey(mcube_name)){
			queryview_type = queryviewTypes.get(mcube_name);
		}else{
			String query = "SELECT QUERYVIEW_#_TY FROM MCUBES mc WHERE mc.cname = '"+mcube_name+"'";
			dataAccess = DataAccess.getDataAccessMgr();
			queryview_type = dataAccess.getConcreteType(query);
			queryviewTypes.put(mcube_name, queryview_type);
		}
		return queryview_type;
	}

	/**
	 * Returns the dynamically generated DIMENSION ID from the database
	 * @param mcube_name Name of the Multilevel Cube
	 * @throws SQLException 
	 */
	public String getDimensionHierarchyId(String dimName) throws SQLException{
		String dimId;
		if(dimensionId.containsKey(dimName)){
			dimId = dimensionId.get(dimName);
		}else{
			dataAccess = DataAccess.getDataAccessMgr();
			String query = "SELECT dim.ID FROM DIMENSIONS dim WHERE dim.dname = '"+dimName+"'";
			dimId = dataAccess.getConcreteType(query);
			dimensionId.put(dimName, dimId);
		}
		return dimId;
	}
	
	/**
	 * Takes the dimension sequence of a specific mcube of the local storage if available
	 * else fetch it from the database
	 * @param sn Is the root SQLMNode(sn) which contains the cube coordinate
	 * @throws SQLException 
	 */
	public String getMCubeSequence(SQLMNode sn) throws SQLException{
		StringBuffer sequence = new StringBuffer();
		SQLMNode cubeCoordinate = sn.jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeCoordinate.class);
		SQLMNode mCubeId = sn.jjtGetChild(ASTMultilevelFactID.class).jjtGetChild(ASTMultilevelCubeID.class);
		//if the needed sequence is not stored locally, get the sequence from the database
		if(!this.existsSequence((String)mCubeId.jjtGetValue())){
			LinkedList<String> unorderedSequence = new LinkedList<String>();
			//run throw the values of the AST and save the dimension sequence
			for(int i=0;i<cubeCoordinate.jjtGetNumChildren();i++){
				unorderedSequence.add((String)(cubeCoordinate.jjtGetChild(i).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue());						
			}
			this.getSequenceFromDatabase((String)mCubeId.jjtGetValue());
		}
		for(int h=1;h<=this.getNumberOfValuesOfSequence((String)mCubeId.jjtGetValue()); h++){ //Werte der HashMap
			for(int i=0;i<cubeCoordinate.jjtGetNumChildren();i++){ //Werte des AST
				//abgleichen der Reihenfolge - wenn der richtige Wert für die n-Position gefunden ist, wird er eingefügt
				if(((String)(cubeCoordinate.jjtGetChild(i).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue()).equals((this.getSequence((String)mCubeId.jjtGetValue())).get(h))){
					sequence.append((String)(cubeCoordinate.jjtGetChild(i).jjtGetChild(ASTMultilevelObjectUnqualifiedID.class)).jjtGetValue());
					if(h != this.getNumberOfValuesOfSequence((String)mCubeId.jjtGetValue())){
						sequence.append("', '");
					}
				}
			}
		}
		return sequence.toString();
	}
	
	/**
	 * Generates a list which contains the right order of the analysed CubeCoordinate
	 */
	public ArrayList<Integer> getMCubeOrder(SQLMNode sn){
		ArrayList<Integer> order = new ArrayList<Integer>();
		String mcubeId = (String)(sn.jjtGetChild(0).jjtGetChild(1)).jjtGetValue();
		for(int h=1;h<=this.getNumberOfValuesOfSequence(mcubeId); h++){ //Werte der HashMap
			for(int i=0;i<sn.jjtGetChild(0).jjtGetChild(0).jjtGetNumChildren();i++){ //Werte des AST
				//abgleichen der Reihenfolge - wenn der richtige Wert für die n-Position gefunden ist, wird er eingefügt
				String comparingDimension = (String)(sn.jjtGetChild(0).jjtGetChild(0).jjtGetChild(i).jjtGetChild(1)).jjtGetValue();
				String searchedDimension = (this.getSequence((String)(sn.jjtGetChild(0).jjtGetChild(1)).jjtGetValue())).get(h);
				if(comparingDimension.equals(searchedDimension)){
					order.add(i);
				}
			}
		}
		return order;
	}
	
	/**
	 * Dynamically generates a table name for a SQLMQuery Result
	 * @throws SQLException 
	 */
	public String getTempTableName() throws SQLException{
		dataAccess = DataAccess.getDataAccessMgr();
		StringBuffer query = new StringBuffer();
		String tempTableName;
        boolean exists = true;
        query.append("temp_Output_");
        while(exists){
        	query.append(tableNameAffix);
        	exists = dataAccess.existsTable(query.toString().toUpperCase());
        	if(exists){
        		query.setLength(12);
        		tableNameAffix++;
        	}
        }
        tempTableName = query.toString();
        
        //store the created tables for later deletion
        this.createdTables.add(tempTableName);
        return tempTableName;
	}
	
	/**
	 * Method to delete all dynamic generated tables which 
	 * were needed for SQLMQuery Results
	 * @throws SQLException 
	 */
	public void deleteTempTables() throws SQLException{
		dataAccess = DataAccess.getDataAccessMgr();
		StringBuffer query = new StringBuffer();
		for(String tableName : createdTables){
			query.setLength(0);
			query.append("DROP TABLE ");
			query.append(tableName.toUpperCase());
			//dataAccess.setParallelConection(true);
			dataAccess.execute(query.toString());
		}
		createdTables.clear();
	}
	
	public void setUpdatedCube(String mcube_name, String measure_name){
		if(updatedCubes.size() == 0){
			CubeMeasureMapping cmm = new CubeMeasureMapping();
			cmm.setCubeName(mcube_name);
			cmm.addMeasure(measure_name);
			updatedCubes.add(cmm);
		}
		else{
			boolean cubeExists = false;
			for(int i=0;i<updatedCubes.size();i++){
				if(updatedCubes.get(i).getCubeName().equals(mcube_name)){
					cubeExists = true;
					if((updatedCubes.get(i).containsMeasure(measure_name)) == false){
						updatedCubes.get(i).addMeasure(measure_name);
					}
				}
				
			}
			if(cubeExists == false){
				CubeMeasureMapping cmm = new CubeMeasureMapping();
				cmm.setCubeName(mcube_name);
				cmm.addMeasure(measure_name);
				updatedCubes.add(cmm);
			}
		}
	}
	
	public LinkedList<CubeMeasureMapping> getAllUpdatedCubes(){
		return updatedCubes;
	}
	
}
