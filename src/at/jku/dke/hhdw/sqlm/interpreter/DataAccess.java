package at.jku.dke.sqlm.interpreter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;

import javax.sql.rowset.CachedRowSet;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.rowset.OracleCachedRowSet;

public class DataAccess {

	private static DataAccess ref;
	private Connection conn = null;
	private Statement stmt;
	private ResultSet rset;
	private Statement querystmt;
	private ResultSet queryrset;
	
	/**
	 * Private Constructor, necessary for Singleton pattern. 
	 */
	private DataAccess() throws SQLException{
		if(conn == null){
			this.openConn();
		}
	}
	
	/**
	 * Method to deliver the reference of the DataAccess object
	 */
	public static DataAccess getDataAccessMgr() throws SQLException{
		if (ref == null) {
			ref = new DataAccess();
		}
		return ref;
	}
	
	/**
	 * connect to the database
	 * connection string: 
	 * user: 
	 * password: 
	 * @return a Connection object representing the database connection, null, if connection has not been established
	 */
	private Connection getConnection() throws SQLException {
		
		Connection cn;
		//System.out.println("Connecting to Database ...");
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			//System.out.println(" Database Driver loaded");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		try {
			//getConnection for external access
			//cn = DriverManager.getConnection("jdbc:oracle:thin:@127.0.0.1:1521:orcl", "user", "password");
			//getConnection for StoredProcedure
			cn = DriverManager.getConnection("jdbc:default:connection:");
			//System.out.println(" Connection to Database established");
		} catch (SQLException e) {
			throw new SQLException(e+"\nInterpreter: Error at creating connection with database");
		}
		return cn;
	}
	
	/**
	 * Method to open the public connection "conn"
	 * @throws SQLException 
	 */
	public void openConn() throws SQLException{
		this.conn = this.getConnection();
	}
	
	/**
	 * Method to close the public connection "conn"
	 * @throws SQLException 
	 */
	public void closeConn() throws SQLException{
		if(this.conn != null){
			this.conn.close();
		}
	}
	
	/**
	 * Method to execute the passed query
	 * @throws SQLException 
	 */
	public void execute(String query) throws SQLException{
		try {
			stmt = conn.createStatement();
			stmt.execute(query);
			stmt.close();
		} catch (SQLException e) {
			this.closeConn();
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to execute the passed query and send back the result as ResultSet
	 * @throws SQLException 
	 */
	public ResultSet executeQueryReturnResultSet(String query)  throws SQLException{
		//System.out.println(query);
		//FilteredRowSet frs = new OracleFilteredRowSet();
		queryrset = null;
		querystmt = null;
		try {
			querystmt = conn.createStatement();
			((OracleConnection)conn).setCreateStatementAsRefCursor(true);
			queryrset = querystmt.executeQuery(query);
		} catch (SQLException e) {
			DataDictionary dataDictionary;
			dataDictionary = DataDictionary.getDataDictionary();
			dataDictionary.deleteTempTables();
			queryrset.close();
			querystmt.close();
			this.closeConn();
			e.printStackTrace();
		}
		return queryrset;
	}
	
	/**
	 * Method to execute the passed query and send back the result as ResultSet
	 * @throws SQLException 
	 */
	public CachedRowSet executeQueryReturnCachedRowSet(String query)  throws SQLException{
		//System.out.println(query);
		CachedRowSet crs = new OracleCachedRowSet();
		try {
			((OracleConnection)conn).setCreateStatementAsRefCursor(true);
			rset = stmt.executeQuery(query);
			crs.populate(rset);
			rset.close();
			stmt.close();
		} catch (SQLException e) {
			DataDictionary dataDictionary;
			dataDictionary = DataDictionary.getDataDictionary();
			dataDictionary.deleteTempTables();
			this.closeConn();
			e.printStackTrace();
		}
		return crs;
	}
	
	/**
	 * Method to execute the passed query and send back the result as LinkedList<ArrayList<Object>>
	 * @throws SQLException 
	 */
	public LinkedList<ArrayList<Object>> executeQueryReturnList(String query) throws SQLException{
		stmt = conn.createStatement();
        rset = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rset.getMetaData();
        int numCols = rsmd.getColumnCount();
        
        LinkedList<ArrayList<Object>> results = new LinkedList<ArrayList<Object>>();
        while(rset.next()){
            ArrayList<Object> al = new ArrayList<Object>(numCols);
            for(int i=1;i-1<numCols;i++){
            	al.add(rset.getObject(i));
            }
            results.add(al);
        }
        rset.close(); 
        stmt.close(); 
        return results;
	}
	
	/**
	 * Method to get the dynamic generated TYPE of the delivered MCUBE from the database
	 * @throws SQLException 
	 */
	public String getConcreteType(String query) throws SQLException{
		stmt = conn.createStatement();
	    rset = stmt.executeQuery(query);
	            
	    rset.next();
	    String mcube_type = rset.getString(1);
	        
	    rset.close(); 
	    stmt.close();
	    return mcube_type;
	}
	/**
	public String getDimensionId(String dimension_name) throws SQLException{
		if(this.conn == null){
			this.openConn();
		}
        try{
        	Statement st = conn.createStatement();
        	String query = "SELECT dim.ID FROM DIMENSIONS dim WHERE dim.dname = '"+dimension_name+"'";
            ResultSet rs = st.executeQuery(query);
            
            rs.next();
            String dimId = rs.getString(1);
            
            rs.close(); 
            st.close();
            return dimId;
        } catch (SQLException e){
            throw new SQLException(e+"\nInterpreter: Error at getting dimension id"); 
        }
	}
	**/
	
	/**
	 * Method to check if a table name is already existing inside the database
	 * @throws SQLException 
	 */
	public boolean existsTable(String table_name) throws SQLException{
		stmt = conn.createStatement();
        String query = "select table_name from user_tables where table_name='"+table_name+"'";
        rset = stmt.executeQuery(query);
            
        boolean exists = false;
        if(rset.next()){
            exists = true;
        } 
        rset.close(); 
        stmt.close();   
        return exists;
	}
	
	
	/**
	 * Method to create a table with the committed name and invisible column 
	 */
	public void createEmptyTable(String tablename) throws SQLException{
		stmt = conn.createStatement();
        String query = "CREATE TABLE "+tablename+" (EMPTY NUMBER)";
        stmt.executeQuery(query);
        stmt.close();
	}
	
	/**
	 * Method to get an empty ResultSet for the ref cursor
	 * @throws SQLException 
	 */
	public ResultSet getEmptyResultSet()  throws SQLException{
		try {
			String query = "SELECT error_name AS \"NO RESULT\" FROM ERRORS WHERE error_name IS NULL";
			querystmt = conn.createStatement();
			((OracleConnection)conn).setCreateStatementAsRefCursor(true);
			queryrset = querystmt.executeQuery(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return queryrset;
	}
	
}

