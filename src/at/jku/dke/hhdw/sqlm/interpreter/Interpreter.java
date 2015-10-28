package at.jku.dke.sqlm.interpreter;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import at.jku.dke.sqlm.parser.*;

public class Interpreter {
	
	static boolean first=true;

	public static void main(String[] args) {
		
		System.out.println("Reading from standard input...");
	    System.out.print("Enter an M-SQL block:");
	    
	    new SQLMParser(System.in);
	    try {
	    	long startTime;
	    	long stopTime;
	    	long elapsedTime;
	    	ASTSQLMDocument document = SQLMParser.Start();
	    	document.dump("");
			System.out.println("Thank you. \n");
			
			startTime = System.currentTimeMillis();
			Optimizer opt = new Optimizer();
			opt.run(document);
			stopTime = System.currentTimeMillis();
		    elapsedTime = stopTime - startTime;
		    System.out.println("TIME FOR OPTIMIZATION: "+elapsedTime+"ms\n");
			document.dump("");
			
			System.out.println("----- PL/SQL -----");
			
			Translator trans = new Translator();
			startTime = System.currentTimeMillis();
			trans.run(document);
			stopTime = System.currentTimeMillis();
		    elapsedTime = stopTime - startTime;
		    System.out.println("TIME FOR TRANSLATION AND EXECUTION: "+elapsedTime+"ms\n");
			
			DataAccess dataAccess = DataAccess.getDataAccessMgr();
			dataAccess.closeConn();
			System.out.println("----- DONE -----");
	    }catch (Exception e){
			try {
				DataAccess dataAccess = DataAccess.getDataAccessMgr();
				dataAccess.closeConn();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		    System.out.println("Oops.");
		    System.out.println(e.getMessage());
	    } 
	}
	
	
	public static ResultSet sqlmQuery(oracle.sql.CLOB clob_input) throws Exception{
		String input = clob_input.getSubString(1, (int)clob_input.length());
		StringReader reader_input = new StringReader(input);
		if(first){
			new SQLMParser(reader_input);
			first = false;
		}else{
			SQLMParser.ReInit(reader_input);
		}
	    ASTSQLMDocument document = SQLMParser.Start();
	    Optimizer opt = new Optimizer();
		opt.run(document);
		Translator trans = new Translator();
		return trans.run(document);
	}
}
