package at.jku.dke.sqlm.translator;

import java.sql.SQLException;
import java.util.LinkedList;

import at.jku.dke.sqlm.interpreter.CubeMeasureMapping;
import at.jku.dke.sqlm.interpreter.DataDictionary;

public class RefreshMeasureUnitCache {

	private StringBuffer translation;
	private LinkedList<CubeMeasureMapping> cubes;
	private String objectType;
	private DataDictionary dataDict;
	
	public RefreshMeasureUnitCache(){
		dataDict = DataDictionary.getDataDictionary();
		cubes = dataDict.getAllUpdatedCubes();
		translation = new StringBuffer();
	}
	
	public String translate() throws SQLException{
		translation.append("\nDECLARE \n");
		translateVariableDeclaration();
		translation.append("BEGIN \n");
		translateExecution();
		translation.append("END;");
		return translation.toString();
	}
	
	public void translateVariableDeclaration() throws SQLException{
		dataDict = DataDictionary.getDataDictionary();
		for(int i=0;i<cubes.size();i++){
			objectType = dataDict.getMCubeType(cubes.get(i).getCubeName()).substring(6, 16);
			translation.append("  mc_");
			translation.append(objectType);
			translation.append(" mcube_");
			translation.append(objectType);
			translation.append("_ty;\n  mrel_");
			translation.append(objectType);
			translation.append(" mrel_");
			translation.append(objectType);
			translation.append("_trty;\n");
		}
	}
	
	public void translateExecution() throws SQLException{
		dataDict = DataDictionary.getDataDictionary();
		for(int i=0;i<cubes.size();i++){
			objectType = dataDict.getMCubeType(cubes.get(i).getCubeName()).substring(6, 16);
			for(int j=0;j<cubes.get(i).measureCount();j++){
				translation.append("  SELECT TREAT(VALUE(mc) AS mcube_");
				translation.append(objectType);
				translation.append("_ty) INTO mc_");
				translation.append(objectType);
				translation.append("\n  FROM mcubes mc\n  WHERE mc.cname = '");
				translation.append(cubes.get(i).getCubeName()); //mcube name
				translation.append("';\n");
				
				translation.append("\n  SELECT REF(mr) BULK COLLECT INTO mrel_");
				translation.append(objectType);
				translation.append("\n  FROM ");
				translation.append(objectType);
				translation.append(" mr;\n");
				
				translation.append("\n  mc_");
				translation.append(objectType);
				translation.append(".refresh_measure_unit_cache('");
				translation.append(cubes.get(i).getMeasure(j)); //mcube measure
				translation.append("', mrel_");
				translation.append(objectType);
				translation.append(");\n\n");
			}
		}
	}
	
}
	
	//Custome Object mit mcube name, mesaure names and 
	
	/**
	DECLARE
	    mc   mcube_c000000001_ty;
	    mrel mrel_c000000001_trty;
	BEGIN
	    SELECT TREAT(VALUE(mc) AS mcube_c000000001_ty) INTO mc
	    FROM   mcubes mc
	    WHERE  mc.cname = 'sales_cube';
	    
	    SELECT REF(mr) BULK COLLECT INTO mrel
	    FROM   c000000001 mr;
	    
	    dbms_output.put_line(mrel.COUNT);
	    mc.refresh_measure_unit_cache('revenue', mrel);
	END;
	**/