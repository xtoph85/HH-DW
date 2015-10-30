package at.jku.dke.sqlm.interpreter;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import at.jku.dke.sqlm.parser.*;

public class Interpreter {
	public static boolean first=true;
	
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
