package at.jku.dke.sqlm.translator;

import java.sql.SQLException;

public interface StatementInterface {

	public String translate() throws SQLException;
}
