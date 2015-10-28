package at.jku.dke.sqlm.interpreter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import javax.sql.rowset.CachedRowSet;
import at.jku.dke.sqlm.parser.*;
import at.jku.dke.sqlm.translator.ClosedMCubeQuery;

/** 
*
*The QueryTranslater class provides methods for translating the input query
*into PL/SQL code. Then executes the code and provides the result.
*
*/

public class QueryTranslator {
	
	private DataDictionary dataDict;
	private DataAccess dataAccess;
	
	public QueryTranslator(){
		
	}
	
	
	/**
	 * Initiated the query translating process.
	 * @param sn is the root SQLMNode(sn) of the current closed Multilevel Cube Query
	 * @throws SQLException 
	 */
	public String rollupQuery(SQLMNode sn) throws SQLException {
		dataDict = DataDictionary.getDataDictionary();
		StringBuffer sb = new StringBuffer((String)sn.jjtGetValue());
		LinkedList<String> levels = new LinkedList<String> ();
		LinkedList<String> dimensions = new LinkedList<String>();
		String mcubeName;
		boolean subquery = false;
		boolean firstRollup = true;
		
		//RollupExpressions
		for(int i=0;i<sn.jjtGetNumChildren();i++){
			SQLMNode rollupExpression = sn.jjtGetChild(i);
			//get the Root MCube 
			//if(rollupExpression.jjtGetChild(1).jjtGetChild(0)instanceof ASTMultilevelCubeID){
			if(rollupExpression.jjtGetChild(ASTInputCubeBlock.class).jjtGetChild(0)instanceof ASTMultilevelCubeID){
				mcubeName = (String)(rollupExpression.jjtGetChild(1).jjtGetChild(0)).jjtGetValue();
			}else{ //SubQuery
				subquery = true;
				mcubeName = this.getRootMCube(rollupExpression.jjtGetChild(1).jjtGetChild(0));
			}
			
			/**
			//check if Qualified or UnQualifiedID
			boolean qualifiedId = true;
			for(int j=0;j<rollupExpression.jjtGetChild(0).jjtGetNumChildren();j++){ //Werte des AST
				if(rollupExpression.jjtGetChild(0).jjtGetChild(j).jjtGetNumChildren() == 1){
					qualifiedId = false;
				}
			}
			**/
			
			//ROLLUP dice cube coordinate only qualified
			//get the cube coordinate sequence of the mcube
			//if the sequence is not locally stored it must be queried from the database
			if(!dataDict.existsSequence(mcubeName)){
				LinkedList<String> unorderedSequence = new LinkedList<String>();
				for(int j=0;j<rollupExpression.jjtGetChild(0).jjtGetNumChildren();j++){
					unorderedSequence.add((String)(rollupExpression.jjtGetChild(0).jjtGetChild(j).jjtGetChild(0)).jjtGetValue());
				}
				dataDict.getSequenceFromDatabase(mcubeName, unorderedSequence);
			}
			
			//adjust the order
			for(int h=1;h<=dataDict.getNumberOfValuesOfSequence(mcubeName); h++){
				for(int j=0;j<rollupExpression.jjtGetChild(0).jjtGetNumChildren();j++){
					if(((String)(rollupExpression.jjtGetChild(0).jjtGetChild(j).jjtGetChild(0)).jjtGetValue()).equals((dataDict.getSequence(mcubeName)).get(h))){
						//save the rollup levels for later use, levels<DimHry,Level>
						levels.add((String)(rollupExpression.jjtGetChild(0).jjtGetChild(j).jjtGetChild(1)).jjtGetValue());
						//save the right dimension sequence
						dimensions.add((String)(rollupExpression.jjtGetChild(0).jjtGetChild(j).jjtGetChild(0)).jjtGetValue());
					}
				}
			}
			
			dataDict = DataDictionary.getDataDictionary();
			//look if MCubeId or SubQuery as InputCube and get the MCube
			//rollup MCube
			if(!subquery){
				String tableName = dataDict.getTempTableName();
				StringBuffer query = new StringBuffer();
				query.append("DECLARE\n  m_cube ");
				query.append(dataDict.getMCubeType(mcubeName));
				query.append(";");
				//test
				query.append("\n  measure_units measure_unit_tty;");
				//look if conversions are existing
				boolean conversion = false;
				if(rollupExpression.jjtGetChild(ASTRollupUnitConversionList.class) != null ){
					conversion = true;
					query.append("\n  measure_units measure_unit_tty;");
					//declare needed ref's for measure conversion
					for(int j=0;j<rollupExpression.jjtGetChild(2).jjtGetNumChildren();j++){
						if(rollupExpression.jjtGetChild(2).jjtGetChild(j).jjtGetChild(1).jjtGetChild(0) instanceof ASTMultilevelObjectQualifiedID){
							//declare dimension
							query.append("\n  dim");
							query.append(j);
							query.append(" dimension_ty;");
							//declare unit_ref 
							query.append("\n  unit");
							query.append(j);
							query.append("_ref REF mobject_ty;");
						}
						//declare needed cubeRef's
						query.append("\n  units");
						query.append(j);
						query.append("_cube_ref REF mcube_ty;");
					}
				}
				
				query.append("\nBEGIN\n  SELECT TREAT ( VALUE (mc) AS ");
				query.append(dataDict.getMCubeType(mcubeName));
				query.append(")\n  INTO m_cube \n  FROM mcubes mc \n  WHERE mc.cname = '");
				query.append(mcubeName);
				query.append("';");
				
				//insert the conversion-information into measure_units if they exist
				if(conversion){
					query.append(this.getConversionInformation(rollupExpression));
				}
				//create rollup function
				query.append("\n  m_cube.rollup('");
				query.append(tableName);
				query.append("', FALSE");
				//insert conversion-information if existing
				if(conversion){
					query.append(", measure_units");
				}
				//rollup levels
				query.append(", '");
				for(int j=0;j<levels.size();j++){
					query.append(levels.get(j));
					if(j+1<levels.size()){
						query.append("', '");
					}
				}
				query.append("');\nEND;\n");
				
				//execute rollup on the MCube
				dataAccess = DataAccess.getDataAccessMgr();
				System.out.println(query.toString());
				dataAccess.execute(query.toString());
				//insert table name with the information of the rollup-function
				sb.replace(sb.indexOf("ROLLUP"), (sb.indexOf("ROLLUP")+6), tableName);
				if(firstRollup){
					firstRollup = false;
				}else{
					sb.replace(sb.indexOf(","), sb.indexOf(",")+1,"UNION SELECT * FROM");
				}
			}else{ //SubQuery
				SQLMNode closedquery = rollupExpression.jjtGetChild(1).jjtGetChild(0);
				//subquery code
				StringBuffer sq = new StringBuffer();
				//get the information of the sub-query
				sq.append("DECLARE\n  m_cube ");
				sq.append(dataDict.getMCubeType(mcubeName));
				sq.append(";\n  mrel ");
				sq.append(dataDict.getMRelType(mcubeName));
				sq.append(";\n  dim_ref REF dimension_ty; \n  queryview ");
				sq.append(dataDict.getQueryviewType(mcubeName));
				sq.append(";\n");
				if(dimensions.size()>0){
					for(int j=0;j<dimensions.size();j++){
						sq.append("  pred_");
						sq.append(j);
						sq.append(" slice_predicate_ty;\n");
					}
				}
				//SliceConversionExpression refs
				//declare AttributeUnit refs
				int count = this.getCountOfSimultaneousNeededAttributeMObjectRefs(closedquery);
				if(count>0){
					for(int c=0;c<count;c++){
						//declare dimensions
						sq.append("\n  attr_dim");
						sq.append(c);
						sq.append(" dimension_ty;\n");
						//declare unit_refs
						sq.append("  attr_unit");
						sq.append(c);
						sq.append("_ref REF mobject_ty;\n");
					}
				}
				
				//declare m-object value refs
				count = this.getCountOfSimultaneousNeededValueMObjectRefs(closedquery);
				if(count>0){
					for(int c=0;c<count;c++){
						//declare dimensions
						sq.append("\n  value_dim");
						sq.append(c);
						sq.append(" dimension_ty;");
						//declare unit_refs
						sq.append("  value_unit");
						sq.append(c);
						sq.append("_ref REF mobject_ty;\n");
					}
				}
				
				//declare AttributeUnitConversionRule refs
				count = this.getCountOfSimultaneousNeededMCubeRefs(closedquery);
				if(count>0){
					for(int c=0;c<count;c++){
						//declare needed cube refs
						sq.append("\n  attr_units");
						sq.append(c);
						sq.append("_cube_ref REF mcube_ty;\n");
					}
				}
				
				//declare RollupUnitConversion refs
				boolean conversion = false;
				if(rollupExpression.jjtGetChild(ASTRollupUnitConversionList.class) != null){
					conversion = true;
					sq.append("  measure_units measure_unit_tty;\n");
					//declare needed ref's for measure conversion
					for(int j=0;j<rollupExpression.jjtGetChild(2).jjtGetNumChildren();j++){
						if(rollupExpression.jjtGetChild(2).jjtGetChild(j).jjtGetChild(1).jjtGetChild(0) instanceof ASTMultilevelObjectQualifiedID){
							//declare dimension
							sq.append("\n  dim");
							sq.append(j);
							sq.append(" dimension_ty;");
							//declare unit_ref 
							sq.append("\n  unit");
							sq.append(j);
							sq.append("_ref REF mobject_ty;");
						}
						//declare needed cube refs
						sq.append("\n  units");
						sq.append(j);
						sq.append("_cube_ref REF mcube_ty;\n");
					}
				}
				
				sq.append("BEGIN\n  SELECT TREAT(VALUE(mc) AS ");
				sq.append(dataDict.getMCubeType(mcubeName));
				sq.append(") INTO m_cube\n  FROM mcubes mc\n  WHERE  mc.cname = '");
				sq.append(mcubeName);
				sq.append("';\n  queryview := m_cube.new_queryview;\n");
				
				//get Information of the SubQueries
				ClosedMCubeQuery query = new ClosedMCubeQuery(rollupExpression.jjtGetChild(1).jjtGetChild(0), dimensions, mcubeName);
				sq.append(query.getQueryCode());
				
				//insert the conversion-information into measure_units if they exist
				if(conversion){
					sq.append(this.getConversionInformation(rollupExpression));
				}
				
				String tableName = dataDict.getTempTableName();
				//Rollup function
				sq.append("\n  queryview.rollup('");
				sq.append(tableName);
				sq.append("', FALSE");
				//insert conversion-inforamation if existing
				if(conversion){
					sq.append(", measure_units");
				}
				//Rollup Levels
				sq.append(", '");
				for(int j=0;j<levels.size();j++){
					sq.append(levels.get(j));
					if(j+1<levels.size()){
						sq.append("', '");
					}
				}
				sq.append("');\nEND;\n");
				//execute rollup on the Queryview
				dataAccess = DataAccess.getDataAccessMgr();
				System.out.println(sq.toString());
				dataAccess.execute(sq.toString());
				//insert tablename with the query result of the rollup-function
				sb.replace(sb.indexOf("ROLLUP"), (sb.indexOf("ROLLUP")+6), tableName);
				if(firstRollup){
					firstRollup = false;
				}else{
					sb.replace(sb.indexOf(","), sb.indexOf(",")+1,"UNION SELECT * FROM");
				}
			}
			//clear saved levels from last rollup block
			levels.clear();
		}
		sb.deleteCharAt(sb.indexOf(";"));
		return sb.toString();
	}
	
	private String getRootMCube(SQLMNode sn){
		String rootMCube;
		//check for SubQuery and get MCube
		if(sn.jjtGetChild(1)instanceof ASTDiceBlock){ //DiceBlock
			if(sn.jjtGetChild(1).jjtGetChild(1).jjtGetChild(0)instanceof ASTMultilevelCubeID){
				rootMCube = (String)(sn.jjtGetChild(1).jjtGetChild(1).jjtGetChild(0)).jjtGetValue();
			}else{ //ClosedMCubeSubQuery
				rootMCube = this.getRootMCube(sn.jjtGetChild(1).jjtGetChild(1).jjtGetChild(0));
			}
		}else{ //CubeBlock
			if(sn.jjtGetChild(1).jjtGetChild(0)instanceof ASTMultilevelCubeID){
				rootMCube = (String)(sn.jjtGetChild(1).jjtGetChild(0)).jjtGetValue();
			}else{ //ClosedMCubeSubQuery
				rootMCube = this.getRootMCube(sn.jjtGetChild(1).jjtGetChild(0));
			}
		}
		return rootMCube;
	}
	
	private Integer getCountOfSimultaneousNeededAttributeMObjectRefs(SQLMNode sn){
		int count = 0;
		int temp_count = 0;
		if(sn.jjtGetChild(ASTSliceBlock.class) != null){
			for(int i=0;i<sn.jjtGetChild(ASTSliceBlock.class).jjtGetNumChildren();i++){
				if(sn.jjtGetChild(ASTSliceBlock.class).jjtGetChild(i) instanceof ASTSliceConversionExpression){
					if(sn.jjtGetChild(ASTSliceBlock.class).jjtGetChild(i).jjtGetChild(ASTAttributeUnit.class).jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null ){
						temp_count++;
					}
				}
			}
		}
		if(temp_count>count){
			count = temp_count;
		}
		if(sn.jjtGetChild(1)instanceof ASTDiceBlock){
			if(sn.jjtGetChild(1).jjtGetChild(1).jjtGetChild(0)instanceof ASTClosedMCubeQuery){
				temp_count = this.getCountOfSimultaneousNeededAttributeMObjectRefs(sn.jjtGetChild(1).jjtGetChild(1).jjtGetChild(0));
			}
		}else{
			if(sn.jjtGetChild(1).jjtGetChild(0)instanceof ASTClosedMCubeQuery){
				temp_count = this.getCountOfSimultaneousNeededAttributeMObjectRefs(sn.jjtGetChild(1).jjtGetChild(0));
			}
		}
		if(temp_count>count){
			count = temp_count;
		}
		return count;
	}
	
	private Integer getCountOfSimultaneousNeededMCubeRefs(SQLMNode sn){
		int count = 0;
		int temp_count = 0;
		if(sn.jjtGetChild(ASTSliceBlock.class) != null){
			for(int i=0;i<sn.jjtGetChild(ASTSliceBlock.class).jjtGetNumChildren();i++){
				if(sn.jjtGetChild(ASTSliceBlock.class).jjtGetChild(i) instanceof ASTSliceConversionExpression){
					temp_count++;
				}
			}
		}
		if(temp_count>count){
			count = temp_count;
		}
		if(sn.jjtGetChild(1)instanceof ASTDiceBlock){
			if(sn.jjtGetChild(1).jjtGetChild(1).jjtGetChild(0)instanceof ASTClosedMCubeQuery){
				temp_count = this.getCountOfSimultaneousNeededMCubeRefs(sn.jjtGetChild(1).jjtGetChild(1).jjtGetChild(0));
			}
		}else{
			if(sn.jjtGetChild(1).jjtGetChild(0)instanceof ASTClosedMCubeQuery){
				temp_count = this.getCountOfSimultaneousNeededMCubeRefs(sn.jjtGetChild(1).jjtGetChild(0));
			}
		}
		if(temp_count>count){
			count = temp_count;
		}
		return count;
	}
	
	private Integer getCountOfSimultaneousNeededValueMObjectRefs(SQLMNode sn){
		int count = 0;
		int temp_count = 0;
		if(sn.jjtGetChild(ASTSliceBlock.class) != null){
			for(int i=0;i<sn.jjtGetChild(ASTSliceBlock.class).jjtGetNumChildren();i++){
				if(sn.jjtGetChild(ASTSliceBlock.class).jjtGetChild(i).jjtGetChild(ASTMultilevelObjectQualifiedID.class) != null){
					temp_count++;
				}
			}
		}
		if(temp_count>count){
			count = temp_count;
		}
		if(sn.jjtGetChild(1)instanceof ASTDiceBlock){
			if(sn.jjtGetChild(1).jjtGetChild(1).jjtGetChild(0)instanceof ASTClosedMCubeQuery){
				temp_count = this.getCountOfSimultaneousNeededValueMObjectRefs(sn.jjtGetChild(1).jjtGetChild(1).jjtGetChild(0));
			}
		}else{
			if(sn.jjtGetChild(1).jjtGetChild(0)instanceof ASTClosedMCubeQuery){
				temp_count = this.getCountOfSimultaneousNeededValueMObjectRefs(sn.jjtGetChild(1).jjtGetChild(0));
			}
		}
		if(temp_count>count){
			count = temp_count;
		}
		return count;
	}
	
	private String getConversionInformation(SQLMNode rollupExpression){
		StringBuffer sb = new StringBuffer();
		SQLMNode conversionList = rollupExpression.jjtGetChild(2);
		for(int j=0;j<conversionList.jjtGetNumChildren();j++){
			if(conversionList.jjtGetChild(j).jjtGetChild(1).jjtGetChild(0) instanceof ASTMultilevelObjectQualifiedID){
				//get dimension ref
				sb.append("\n  SELECT VALUE(d) INTO dim");
				sb.append(j);
				sb.append(" FROM dimensions d  WHERE  d.dname = '");
				sb.append( (String)(conversionList.jjtGetChild(j).jjtGetChild(1).jjtGetChild(0).jjtGetChild(1)).jjtGetValue() );
				sb.append("';");
				//get m-object ref
				sb.append("\n  unit");
				sb.append(j);
				sb.append("_ref := dim");
				sb.append(j);
				sb.append(".get_mobject_ref('");
				sb.append( (String)(conversionList.jjtGetChild(j).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0)).jjtGetValue() );
				sb.append("');");
			}
			//get m-cube ref
			sb.append("\n  SELECT REF(mc) INTO units");
			sb.append(j);
			sb.append("_cube_ref FROM mcubes mc WHERE  mc.cname = '");
			sb.append( (String)(conversionList.jjtGetChild(j).jjtGetChild(2).jjtGetChild(0)).jjtGetValue() );
			sb.append("';");
		}
		sb.append("\n  measure_units := measure_unit_tty(");
		//run through each RollupUnitConversion
		for(int j=0;j<conversionList.jjtGetNumChildren();j++){
			sb.append("measure_unit_ty('");
			//run through for each MultilevelFactMeasure to convert
			for(int k=0;k<conversionList.jjtGetChild(j).jjtGetChild(0).jjtGetNumChildren();k++){
				//insert measure-name
				sb.append( (String)(conversionList.jjtGetChild(j).jjtGetChild(0).jjtGetChild(k)).jjtGetValue() );
				//insert measure-unit
				if(conversionList.jjtGetChild(j).jjtGetChild(1).jjtGetChild(0) instanceof ASTNumberValue){
					sb.append("', ANYDATA.convertNumber(");
				}else{
					if(conversionList.jjtGetChild(j).jjtGetChild(1).jjtGetChild(0) instanceof ASTMultilevelObjectQualifiedID){
						sb.append("', ANYDATA.convertRef(unit");
						sb.append(j);
						sb.append("_ref");
					}else{
						sb.append("', ANYDATA.convertVarchar2(");
					}
				}
				if(conversionList.jjtGetChild(j).jjtGetChild(1).jjtGetChild(ASTMultilevelObjectQualifiedID.class) == null ){
					sb.append( (String)(rollupExpression.jjtGetChild(2).jjtGetChild(j).jjtGetChild(1).jjtGetChild(0)).jjtGetValue() );
				}
				sb.append("), ");
				//insert units cube
				sb.append("ANYDATA.convertRef(units");
				sb.append(j);
				sb.append("_cube_ref)");
				if(k+1<conversionList.jjtGetChild(j).jjtGetChild(0).jjtGetNumChildren()){
					sb.append("), measure_unit_ty('");
				}
			}
			if(j+1<conversionList.jjtGetNumChildren()){
				sb.append("), ");
			}
		}
		sb.append("));");
		return sb.toString();
	}
	/**
	private void getSliceDimensions(SQLMNode sn, HashMap<String, Integer> sliceDim){
		if(sn.jjtGetChild(ASTSliceBlock.class) != null){
			for(int i=0;i<sn.jjtGetChild(ASTSliceBlock.class).jjtGetNumChildren();i++){
				String dimId = (String)(sn.jjtGetChild(ASTSliceBlock.class).jjtGetChild(i).
						jjtGetChild(ASTSliceExpressionPath.class).jjtGetChild(ASTDimensionHierarchyID.class)).jjtGetValue();
				if(!sliceDim.containsKey(dimId)){
					sliceDim.put(dimId, sliceDim.size());
				}
			}
		}
		//check for SubQuery and get MCube
		if(sn.jjtGetChild(ASTDiceBlock.class)!= null){
			SQLMNode diceBlock = sn.jjtGetChild(ASTDiceBlock.class);
			if(diceBlock.jjtGetChild(ASTInputCubeBlock.class).jjtGetChild(ASTClosedMCubeQuery.class) != null){
				this.getSliceDimensions(sn.jjtGetChild(1).jjtGetChild(1).jjtGetChild(0), sliceDim);
			}
		}else{ //CubeBlock
			SQLMNode cubeBlock = sn.jjtGetChild(ASTCubeBlock.class);
			if(cubeBlock.jjtGetChild(ASTClosedMCubeQuery.class) != null){
				this.getSliceDimensions(sn.jjtGetChild(1).jjtGetChild(0), sliceDim);
			}
		}
	}
	**/
	public ResultSet getQueryResultSet(SQLMNode queryRootNode) throws SQLException {
		dataAccess = DataAccess.getDataAccessMgr();
		ResultSet result = null;
		try{
			result = dataAccess.executeQueryReturnResultSet(this.rollupQuery(queryRootNode));
		}catch(Exception e){
			throw new SQLException(e.toString());
		}
		//handle the deletion of the temporary tables
		dataDict = DataDictionary.getDataDictionary();
		dataDict.deleteTempTables();
		
		return result;
	}
	
	public CachedRowSet getQueryCachedRowSet(SQLMNode queryRootNode) throws SQLException {
		dataAccess = DataAccess.getDataAccessMgr();
		//FilteredRowSet result = null;
		CachedRowSet result = null;
		//System.out.println(this.rollupQuery(queryRootNode));
		try{
			result = dataAccess.executeQueryReturnCachedRowSet(this.rollupQuery(queryRootNode));
		}catch(Exception e){
			throw new SQLException(e.toString());
		}
		//handle the deletion of the temporary tables
		dataDict = DataDictionary.getDataDictionary();
		dataDict.deleteTempTables();
		return result;
	}
	
}

	