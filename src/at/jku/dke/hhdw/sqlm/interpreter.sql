
-- @author: Thomas Pecksteiner

DROP PACKAGE refcurpkg;
DROP FUNCTION sqlm_query;

CREATE OR REPLACE PROCEDURE sqlm_update(input CLOB)
  AS LANGUAGE JAVA  NAME 'at.jku.dke.sqlm.interpreter.Interpreter.sqlmUpdate(oracle.sql.CLOB)';

CREATE OR REPLACE PACKAGE refcurpkg AS
  type refcur_ty is ref cursor;
end;
/

CREATE OR REPLACE FUNCTION sqlm_query(input CLOB) RETURN refcurpkg.refcur_ty IS
  LANGUAGE JAVA NAME 'at.jku.dke.sqlm.interpreter.Interpreter.sqlmQuery(oracle.sql.CLOB) return java.sql.ResultSet';
/