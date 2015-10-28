package at.jku.dke.sqlm.interpreter;

/*
 * DataTranslator
 * 
 */

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import at.jku.dke.sqlm.parser.*;
import at.jku.dke.sqlm.translator.*;


public class Translator {
	private DataAccess dataAccess;
	private DataDictionary dataDict;
	private Statement stmt;
	private QueryTranslator queryTranslator;
	private String translation;
	private ResultSet result;
	private SemanticChecker semCheck = new SemanticChecker();
	private boolean query;
	private boolean resultSetNeeded;
	private boolean debug = false;
	
	public Translator(){}
	
	public ResultSet run(SimpleNode sn) throws SQLException{
		resultSetNeeded = true;
		for(int i=0;i<sn.jjtGetNumChildren();i++) {
			query = false;
			SQLMNode currentstatement = (SQLMNode)sn.jjtGetChild(i);		
			if(currentstatement instanceof ASTCreateDimensionHierarchy){
				stmt = new CreateDimensionHierarchy(currentstatement);
			}else if(currentstatement instanceof ASTCreateMultilevelObject){
				try{
					semCheck.run(currentstatement);
				}catch(Exception e){
					e.printStackTrace();
				}
				stmt = new CreateMultilevelObject(currentstatement);
			}else if(currentstatement instanceof ASTCreateMultilevelCube){
				stmt = new CreateMultilevelCube(currentstatement);
			}else if(currentstatement instanceof ASTCreateMultilevelFact){
				stmt = new CreateMultilevelFact(currentstatement);
			}else if(currentstatement instanceof ASTAlterMultilevelObject){
				stmt = new AlterMultilevelObject(currentstatement);
			}else if(currentstatement instanceof ASTAlterMultilevelFact){
				stmt = new AlterMultilevelFact(currentstatement);
			}else if(currentstatement instanceof ASTDropDimensionHierarchy){
				stmt = new DropDimensionHierarchy(currentstatement);
			}else if(currentstatement instanceof ASTDropMultilevelObject){
				stmt = new DropMultilevelObject(currentstatement);
			}else if(currentstatement instanceof ASTDropMultilevelCube){
				stmt = new DropMultilevelCube(currentstatement);
			}else if(currentstatement instanceof ASTDropMultilevelFact){
				stmt = new DropMultilevelFact(currentstatement);
			}else if(currentstatement instanceof ASTUpdateMultilevelObject){
				stmt = new UpdateMultilevelObject(currentstatement);
			}else if(currentstatement instanceof ASTUpdateMultilevelFact){
				stmt = new UpdateMultilevelFact(currentstatement);
			}else if(currentstatement instanceof ASTBulkCreateMultilevelObject){
				try{
					semCheck.run(currentstatement);
				}catch(Exception e){
					e.printStackTrace();
				}
				stmt = new BulkCreateMultilevelObject(currentstatement);
			}else if(currentstatement instanceof ASTBulkUpdateMultilevelObject){ 
				stmt = new BulkUpdateMultilevelObject(currentstatement);
			}else if(currentstatement instanceof ASTBulkUpdateMultilevelFact){
				stmt = new BulkUpdateMultilevelFact(currentstatement);
			}else if(currentstatement instanceof ASTBulkCreateMultilevelFact){
				stmt = new BulkCreateMultilevelFact(currentstatement);
			}else if(currentstatement instanceof ASTSQLSelectStatement){
				queryTranslator = new QueryTranslator();
				result = queryTranslator.getQueryResultSet(currentstatement);
				resultSetNeeded = false;
				query = true;
				if(debug){
					this.ResultSetToString();
				}
			}
			if(!query){
				translation = stmt.translate();
				if(debug){
					System.out.println(translation);
					//last statement
					if(i+1 == sn.jjtGetNumChildren()){
						dataDict = DataDictionary.getDataDictionary();
						if(dataDict.getAllUpdatedCubes().size()>0){
							RefreshMeasureUnitCache refresh = new RefreshMeasureUnitCache();
							translation = refresh.translate();
							System.out.println(translation);
						}
					}
				}else{
					//execute the translated statement
					dataAccess = DataAccess.getDataAccessMgr();
					try{
						dataAccess.execute(translation);
						//last statement
						if(i+1 == sn.jjtGetNumChildren()){
							dataDict = DataDictionary.getDataDictionary();
							if(dataDict.getAllUpdatedCubes().size()>0){
								RefreshMeasureUnitCache refresh = new RefreshMeasureUnitCache();
								translation = refresh.translate();
								dataAccess = DataAccess.getDataAccessMgr();
								dataAccess.execute(translation);
							}
						}
					}catch(SQLException e){
						String reason = "\n"+e.getMessage()+"\nSQL-M: Error in"+stmt.getClass().getSimpleName()+"\nSQL-M: Error occurred before line "+currentstatement.jjtGetErrorLine();
						throw new SQLException(reason,e.getSQLState(),e.getErrorCode());
					}
				}
			}
		}
		//create blank ResultSet for API when no DQL statement was submitted		
		if(resultSetNeeded && debug == false){
			dataAccess = DataAccess.getDataAccessMgr();
			result = dataAccess.getEmptyResultSet();
		}
		
		//clear database from temporary data
		dataDict = DataDictionary.getDataDictionary();
		dataDict.deleteTempTables();
		
		return result;
	}
	
	//print query result for local testing
	private void ResultSetToString(){
		if(result != null){
			try {
				ResultSetMetaData rsMetaData = result.getMetaData();
			    int columnCount = rsMetaData.getColumnCount();
			    Object[] header = new Object[columnCount];
			    for (int i=1; i <= columnCount; ++i){
			        Object label = rsMetaData.getColumnLabel(i);
			        header[i-1] = label;
			        System.out.println(label.toString());
			    }
			    System.out.println("--------");
			    while (result.next()){
			        Object[] str = new Object[columnCount];
			        for (int i=1; i <= columnCount; i++){
			            Object obj = result.getObject(i);
			            str[i-1] = obj;
			            String output;
			            if(obj != null){output = obj.toString();} else{output = "NULL";}
			            System.out.println(output);
			        }
			        System.out.println("--------");
			    }
			}catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	
}
