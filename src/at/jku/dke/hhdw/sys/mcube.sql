/** 
 * The Hetero-Homogeneous Data Warehouse Project (HH-DW)
 * Copyright (C) 2010-2012 Department of Business Informatics -- Data & Knowledge Engineering
 * Johannes Kepler University Linz, Altenberger Str. 69, A-4040 Linz, Austria
 **/
/**
 * This file is part of HH-DW.
 *
 * HH-DW is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * HH-DW is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with HH-DW. If not, see <http://www.gnu.org/licenses/>.
 **/

--------------------------------------------------------------------------------
--                                  RESET                                     --
--------------------------------------------------------------------------------

DROP PACKAGE mcube;
DROP PACKAGE mcube_ddl;

DROP TYPE mcube_ty FORCE;
DROP TYPE mrel_ty FORCE;

DROP TYPE slice_predicate_ty FORCE;
DROP TYPE boolean_expression_tty FORCE;
DROP TYPE boolean_expression_ty  FORCE;

DROP TYPE measure_unit_tty FORCE;
DROP TYPE measure_unit_ty FORCE;

DROP TYPE elem_tty FORCE;
DROP TYPE elem_ty FORCE;
DROP TYPE varchar2_elem_ty FORCE;
DROP TYPE number_elem_ty FORCE;

DROP TYPE set_union_aggr_ty FORCE;
DROP FUNCTION set_union;

CREATE OR REPLACE TYPE elem_ty AS OBJECT(
    ID RAW(16),
    CONSTRUCTOR FUNCTION elem_ty(id RAW) RETURN SELF AS RESULT,
    CONSTRUCTOR FUNCTION elem_ty RETURN SELF AS RESULT,
    MEMBER FUNCTION equals(other elem_ty) RETURN INTEGER
) NOT FINAL;
/

CREATE OR REPLACE TYPE BODY elem_ty IS
    CONSTRUCTOR FUNCTION elem_ty(id RAW) RETURN SELF AS RESULT IS
        
    BEGIN
        SELF.id := id;
        
        RETURN;
    END;
    
    CONSTRUCTOR FUNCTION elem_ty RETURN SELF AS RESULT IS
        
    BEGIN
        SELF.id := SYS_GUID();
        
        RETURN;
    END;
    
    MEMBER FUNCTION equals(other elem_ty) RETURN INTEGER IS
        equal BOOLEAN;
    BEGIN
        equal := other.id  = SELF.id;
        
        IF equal THEN
            RETURN 1;
        ELSE
            RETURN -1;
        END IF;
        
        RETURN -1;
    END;
END;
/

CREATE OR REPLACE TYPE elem_tty AS TABLE OF elem_ty;
/

CREATE OR REPLACE TYPE varchar2_elem_ty UNDER elem_ty (
    val VARCHAR2(4000),
    CONSTRUCTOR FUNCTION varchar2_elem_ty(id RAW, val VARCHAR2) RETURN SELF AS RESULT,
    OVERRIDING MEMBER FUNCTION equals(other elem_ty) RETURN INTEGER
);
/

CREATE OR REPLACE TYPE BODY varchar2_elem_ty IS
    CONSTRUCTOR FUNCTION varchar2_elem_ty(id RAW, val VARCHAR2) RETURN SELF AS RESULT IS
        
    BEGIN
        SELF.id := id;
        SELF.val := val;
        
        RETURN;
    END;
    
    OVERRIDING MEMBER FUNCTION equals(other elem_ty) RETURN INTEGER IS
        equal BOOLEAN;
        
        other_varchar2_elem varchar2_elem_ty := TREAT(other AS varchar2_elem_ty);
    BEGIN
        IF other IS OF (varchar2_elem_ty) THEN
            equal := other_varchar2_elem.id  = SELF.id;
        
            IF equal THEN
                RETURN 1;
            ELSE
                RETURN -1;
            END IF;
        END IF;
        
        RETURN -1;
    END;
END;
/

CREATE OR REPLACE TYPE number_elem_ty UNDER elem_ty (
    val NUMBER,
    CONSTRUCTOR FUNCTION number_elem_ty(id RAW, val NUMBER) RETURN SELF AS RESULT,
    OVERRIDING MEMBER FUNCTION equals(other elem_ty) RETURN INTEGER
);
/

CREATE OR REPLACE TYPE BODY number_elem_ty IS
    CONSTRUCTOR FUNCTION number_elem_ty(id RAW, val NUMBER) RETURN SELF AS RESULT IS
        
    BEGIN
        SELF.id := id;
        SELF.val := val;
        
        RETURN;
    END;
    
    OVERRIDING MEMBER FUNCTION equals(other elem_ty) RETURN INTEGER IS
        equal BOOLEAN;
        
        other_number_elem number_elem_ty := TREAT(other AS number_elem_ty);
    BEGIN
        IF other IS OF (number_elem_ty) THEN
            equal := other_number_elem.id = SELF.id;
            
            IF equal THEN
                RETURN 1;
            ELSE
                RETURN -1;
            END IF;
        END IF;
        
        RETURN -1;
    END;
END;
/

CREATE OR REPLACE TYPE set_union_aggr_ty AS OBJECT (
    collection elem_tty,
    
    STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT set_union_aggr_ty) 
        RETURN NUMBER,
    
    MEMBER FUNCTION ODCIAggregateIterate(SELF IN OUT set_union_aggr_ty, 
                                         value IN elem_tty) 
        RETURN NUMBER,
    
    MEMBER FUNCTION ODCIAggregateTerminate(SELF IN set_union_aggr_ty,
                                           returnValue OUT elem_tty,
                                           flags IN NUMBER)
        RETURN NUMBER,
    
    MEMBER FUNCTION ODCIAggregateMerge(SELF IN OUT set_union_aggr_ty, 
                                       ctx2 IN set_union_aggr_ty)
        RETURN NUMBER
);
/

CREATE OR REPLACE TYPE BODY set_union_aggr_ty IS 

    STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT set_union_aggr_ty) 
        RETURN NUMBER IS 
    BEGIN
        sctx := set_union_aggr_ty(elem_tty());
        
        RETURN ODCIConst.Success;
    END;
    
    MEMBER FUNCTION ODCIAggregateIterate(SELF IN OUT set_union_aggr_ty, 
                                         value IN elem_tty)
        RETURN NUMBER IS
        
        new_collection elem_tty := SELF.collection;
        
        elem elem_ty;
        
        i INTEGER;
        j INTEGER;
        found BOOLEAN;
    BEGIN
        i := value.FIRST;
        WHILE i IS NOT NULL LOOP
            j := new_collection.FIRST;
            found := FALSE;
            WHILE j IS NOT NULL AND NOT found LOOP
                found := new_collection(j).equals(value(i)) > 0;
                
                j := new_collection.NEXT(j);
            END LOOP;
            
            IF NOT found THEN
                new_collection.EXTEND;
                new_collection(new_collection.LAST) := value(i);
            END IF;
            
            i := value.NEXT(i);
        END LOOP;
        
        SELF.collection := new_collection;
         
        RETURN ODCIConst.Success;
    END;
    
    MEMBER FUNCTION ODCIAggregateTerminate(SELF IN set_union_aggr_ty,
                                           returnValue OUT elem_tty,
                                           flags IN NUMBER) RETURN NUMBER IS
    
    BEGIN
        returnValue := SELF.collection;
        
        RETURN ODCIConst.Success;
    END;
    
    MEMBER FUNCTION ODCIAggregateMerge(SELF IN OUT set_union_aggr_ty, 
                                       ctx2 IN set_union_aggr_ty) RETURN NUMBER IS
        
    BEGIN
        RETURN SELF.ODCIAggregateIterate(ctx2.collection);
    END;
END;
/

CREATE OR REPLACE FUNCTION set_union(input elem_tty) RETURN elem_tty 
PARALLEL_ENABLE AGGREGATE USING set_union_aggr_ty;
/

/**
 * This object type represents a boolean expression.
 */
CREATE OR REPLACE TYPE boolean_expression_ty AS OBJECT (
    attribute_name      VARCHAR2(30),  -- name of the attribute
    attribute_unit      ANYDATA,       -- unit of the attribute
    conversion_rule     ANYDATA,       -- how to convert the unit?
    expression_operator VARCHAR2(30),  -- sql operator for comparison
    attribute_value     ANYDATA        -- comparison value
);
/

CREATE OR REPLACE TYPE boolean_expression_tty AS TABLE OF boolean_expression_ty;
/

/**
 * A slice predicate is a conjunction of boolean expressions involving multiple
 * attributes on the same level.
 */
CREATE OR REPLACE TYPE slice_predicate_ty AS OBJECT(
    expressions boolean_expression_tty,
    dim REF dimension_ty,                
    lvl VARCHAR2(30),
    CONSTRUCTOR FUNCTION slice_predicate_ty(dim REF dimension_ty, lvl VARCHAR2) 
        RETURN SELF AS RESULT,
    
    /**
     * This procedure adds a new expression to the expressions list.
     * @param attribute_name the name of the attribute whose value is to be compared.
     * @param expression_operator the operator, e.g., <, >, =, <=, >=, LIKE
     * @param lvl the value to be compared with the attribute value
     */
    MEMBER PROCEDURE add_expression(attribute_name VARCHAR2, 
                                    expression_operator VARCHAR2, 
                                    attribute_value ANYDATA),
    MEMBER PROCEDURE add_expression(attribute_name  VARCHAR2,
                                    attribute_unit  ANYDATA,
                                    conversion_rule ANYDATA,
                                    expression_operator VARCHAR2, 
                                    attribute_value ANYDATA),
    MEMBER FUNCTION satisfies(obj mobject_ty) RETURN BOOLEAN,
    MEMBER FUNCTION get_satisfying_objects RETURN mobject_trty
);
/

CREATE OR REPLACE TYPE BODY slice_predicate_ty AS
    CONSTRUCTOR FUNCTION slice_predicate_ty(dim REF dimension_ty, lvl VARCHAR2)
        RETURN SELF AS RESULT IS
    BEGIN
        SELF.dim := dim;
        SELF.lvl := lvl;
        SELF.expressions := boolean_expression_tty();
        
        RETURN;
    END;
    
    MEMBER PROCEDURE add_expression(attribute_name VARCHAR2, 
                                    expression_operator VARCHAR2, 
                                    attribute_value ANYDATA) IS
    BEGIN
        SELF.expressions.extend;
        SELF.expressions(SELF.expressions.LAST) := 
            boolean_expression_ty(attribute_name,
                                  NULL,
                                  NULL,
                                  expression_operator,
                                  attribute_value);
    END;
    
    MEMBER PROCEDURE add_expression(attribute_name VARCHAR2,
                                    attribute_unit ANYDATA,
                                    conversion_rule ANYDATA,
                                    expression_operator VARCHAR2, 
                                    attribute_value ANYDATA) IS
    BEGIN
        SELF.expressions.extend;
        SELF.expressions(SELF.expressions.LAST) := 
            boolean_expression_ty(attribute_name,
                                  attribute_unit,
                                  conversion_rule,
                                  expression_operator,
                                  attribute_value);
    END;
    
    MEMBER FUNCTION get_satisfying_objects RETURN mobject_trty IS
        -- types
        TYPE map_string_string IS TABLE OF VARCHAR2(30) INDEX BY VARCHAR2(30);
        
        -- return value
        mobjects mobject_trty;
        
        -- tmp variables
        dim dimension_ty;
        attribute_descr attribute_ty;
        table_name VARCHAR2(30);
        table_name_next VARCHAR2(30);
        table_names map_string_string;
        
        -- dynamic sql strings
        sql_statement VARCHAR2(10000);
        select_values VARCHAR2(100);
        from_tables   VARCHAR2(5000);
        where_clause  VARCHAR2(5000);      
        
        i INTEGER;
        
        -- dbms_sql cursor
        sql_cursor INTEGER;
        rows_processed INTEGER;
    BEGIN
        -- get the dimension
        utl_ref.select_object(SELF.dim, dim);
        
        -- open a cursor
        sql_cursor := dbms_sql.open_cursor;
        
        --  
        i := SELF.expressions.FIRST;
        WHILE i IS NOT NULL LOOP
            -- get the attribute description (for the table name mainly).
            attribute_descr :=
                dim.get_attribute_description(SELF.expressions(i).attribute_name);
            
            table_name := UPPER(attribute_descr.table_name);
            
            IF UPPER(attribute_descr.attribute_level) = UPPER(SELF.lvl) THEN
                IF i > 1 THEN
                    IF NOT table_names.EXISTS(table_name) THEN
                        from_tables := from_tables || ', ';
                    END IF;
                    
                    where_clause := where_clause || ' AND ';
                END IF;
                
                IF NOT table_names.EXISTS(table_name) THEN
                    from_tables := from_tables || table_name;
                    
                    table_names(UPPER(table_name)) := table_name;
                END IF;
                
                where_clause := where_clause ||
                    table_name || '.' || SELF.expressions(i).attribute_name || 
                    ' ' || SELF.expressions(i).expression_operator || ' :' || i;
            ELSE
                -- TODO: Throw an error!
                NULL;
            END IF;
            
            i := SELF.expressions.NEXT(i);
        END LOOP;
        
        IF table_names.COUNT > 0 THEN
            -- get the first table
            table_name := table_names.FIRST;
            
            -- read the object names from an arbitrary table
            select_values := table_name || '.obj';
            
            -- loop through the tables to create the join conditions
            WHILE table_name IS NOT NULL LOOP
                table_name_next := table_names.NEXT(table_name);
                
                IF table_name_next IS NOT NULL THEN
                    where_clause := where_clause ||
                        ' AND ' || table_name || '.oname = ';
                    
                    where_clause := where_clause ||
                        table_name_next || '.oname';
                END IF;
                
                table_name := table_name_next;
            END LOOP;
            
            -- complete the query code
            sql_statement := 
                'BEGIN' || chr(10) ||
                '    SELECT ' || select_values || ' BULK COLLECT INTO :mobjects' || chr(10) ||
                '    FROM   ' || from_tables || chr(10) ||
                '    WHERE  ' || where_clause || ';' || chr(10) ||
                'END;';
                    
            -- parse the dynamic statement
            dbms_sql.parse(sql_cursor, sql_statement, dbms_sql.native);
            
            -- bind the variables.
            i := SELF.expressions.FIRST;
            WHILE i IS NOT NULL LOOP           
                CASE SELF.expressions(i).attribute_value.getTypeName
                    WHEN 'SYS.NUMBER' THEN
                        dbms_sql.bind_variable(sql_cursor, ':' || i, SELF.expressions(i).attribute_value.accessNumber());
                    WHEN 'SYS.VARCHAR2' THEN
                        dbms_sql.bind_variable(sql_cursor, ':' || i, SELF.expressions(i).attribute_value.accessVarchar2(), 4000);
                    ELSE
                       -- TODO: throw an error!
                       NULL;
                END CASE;
                
                i := SELF.expressions.NEXT(i);
            END LOOP;
            
            dbms_sql.bind_variable(sql_cursor, ':mobjects', mobjects);
            
            -- execute the statement
            rows_processed := dbms_sql.execute(sql_cursor);
            
            -- obtain the results
            IF rows_processed > 0 THEN
                dbms_sql.variable_value(sql_cursor, ':mobjects', mobjects);
            END IF;
        
        END IF;
        
        -- close the cursor
        dbms_sql.close_cursor(sql_cursor);
        
        RETURN mobjects;
    END;
    
    MEMBER FUNCTION satisfies(obj mobject_ty) RETURN BOOLEAN IS
        satisfied BOOLEAN;
        satisfied1 INTEGER;
        
        attribute_descr attribute_ty;
        attribute_value ANYDATA;
        
        compare_values VARCHAR2(500);
        
        i INTEGER;
    BEGIN
        -- by default, return FALSE.
        satisfied := FALSE;
        
        -- continue only if the level of this predicate corresponds to the
        -- top-level of the m-object that is to be checked and if the m-object
        -- belongs to the right dimension.
        IF SELF.lvl = obj.top_level AND obj.dim = SELF.dim THEN
            satisfied := TRUE;
            
            -- check each boolean expression in the expression list.
            i := SELF.expressions.FIRST;
            WHILE satisfied AND i IS NOT NULL LOOP
                -- get the attribute value
                attribute_value := obj.get_attribute(SELF.expressions(i).attribute_name);
                
                -- continue only if the m-object asserts a value for the attribute
                -- and if the data types are equal.
                IF attribute_value IS NOT NULL AND
                   attribute_value.getTypeName = 
                       SELF.expressions(i).attribute_value.getTypeName THEN
                    
                    compare_values := 
                        'SELECT COUNT(*)' || chr(10) ||
                        'FROM   (SELECT :1 AS value FROM dual) op1, (SELECT :2 AS value FROM dual) op2' || chr(10) ||
                        'WHERE  op1.value ' || SELF.expressions(i).expression_operator || ' op2.value';
                    
                    CASE attribute_value.getTypeName
                        WHEN 'SYS.NUMBER' THEN
                            EXECUTE IMMEDIATE compare_values
                                INTO  satisfied1
                                USING attribute_value.accessNumber, 
                                      SELF.expressions(i).attribute_value.accessNumber;
                        
                        WHEN 'SYS.VARCHAR2' THEN
                            EXECUTE IMMEDIATE compare_values
                                INTO  satisfied1
                                USING attribute_value.accessVarchar2, 
                                      SELF.expressions(i).attribute_value.accessVarchar2;
                        
                        ELSE
                            satisfied1 := 0;
                    END CASE;
                    
                    satisfied := satisfied1 > 0;
                ELSE
                    satisfied := FALSE;
                END IF;
                
                i := SELF.expressions.NEXT(i);
            END LOOP;
        END IF;
        
        RETURN satisfied;
    END;
END;
/

CREATE OR REPLACE TYPE mcube_ty AS OBJECT(
    cname VARCHAR2(30),
    
    -- system administration variables
    mrel_table            VARCHAR2(30),  -- name of the m-relationship table
    id                    VARCHAR2(10),  -- unique surrogate key
    mrel_id_seq           VARCHAR2(30),  -- name of the sequence package for the
                                         -- surrogate keys.
    
    mcube_#_ty            VARCHAR2(30),
    mrel_#_ty             VARCHAR2(30),
    mrel_#_trty           VARCHAR2(30),
    coordinate_#_ty       VARCHAR2(30),
    coordinate_#_tty      VARCHAR2(30),
    conlevel_#_ty         VARCHAR2(30),
    conlevel_#_tty        VARCHAR2(30),
    measure_#_ty          VARCHAR2(30),
    measure_#_tty         VARCHAR2(30),
    measure_meta_#_ty     VARCHAR2(30),
    measure_meta_#_tty    VARCHAR2(30),
    measure_table_#_ty    VARCHAR2(30),
    measure_table_#_tty   VARCHAR2(30),
    conlvl_ancestor_#_ty  VARCHAR2(30),
    conlvl_ancestor_#_tty VARCHAR2(30),
    measure_#_collections VARCHAR2(30),
    queryview_#_ty        VARCHAR2(30),
    expr_#_ty             VARCHAR2(30),
    expr_#_tty            VARCHAR2(30),
    dice_expr_#_ty        VARCHAR2(30),
    slice_expr_#_ty       VARCHAR2(30),
    project_expr_#_ty     VARCHAR2(30),
    mrel_#_value_ty       VARCHAR2(30),
    mrel_#_value_tty      VARCHAR2(30),
        
    -- administration flags
    enforce_consistency INTEGER, -- should consistency be enforced?
    
    -- cache flags
    enable_measure_unit_cache INTEGER, -- should the units be cached in the measure tables?
    
    MEMBER PROCEDURE persist,
    MEMBER PROCEDURE delete_mcube,
    
    -- data dictionary functions
    MEMBER FUNCTION get_dimension_names RETURN names_tty,
    MEMBER FUNCTION get_dimension_ids RETURN names_tty,
    
    -- consistency checks
    MEMBER FUNCTION unique_measure_induction(measure_name VARCHAR2)
        RETURN BOOLEAN
) NOT FINAL NOT INSTANTIABLE;
/

CREATE OR REPLACE TYPE mrel_ty AS OBJECT(
    mcube REF mcube_ty,
    
    -- a unique surrogate id and URI
    id  VARCHAR2(10),
    
    specializes INTEGER,
    
      -- TO BE OVERRIDDEN!
    MEMBER FUNCTION does_specialize RETURN BOOLEAN,
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE delete_mrel
) NOT FINAL NOT INSTANTIABLE;
/

CREATE OR REPLACE TYPE measure_unit_ty AS OBJECT (
    measure_name    VARCHAR2(30),
    measure_unit    ANYDATA,
    conversion_rule ANYDATA
);
/

CREATE OR REPLACE TYPE measure_unit_tty AS TABLE OF measure_unit_ty;
/


CREATE OR REPLACE PACKAGE mcube AS    
    FUNCTION create_mcube(cname VARCHAR2, 
                          dimensions names_tty,
                          root_coordinate names_tty)
        RETURN REF mcube_ty;
    
    
    PROCEDURE delete_mcube(cname VARCHAR2);
END;
/

-- This package is used to dynamically create/specialize a few types that are
-- used by other packages.
CREATE OR REPLACE PACKAGE mcube_ddl AS       
    /*** TABLES ***/
    PROCEDURE create_mcubes_table;
    
    PROCEDURE create_mrel_table(mrel_#_ty  VARCHAR2,
                                mrel_table VARCHAR2,
                                mcube_id   VARCHAR2,
                                dim_ids    names_tty);
    
    /*** HEADERS (AND TYPES WITH NO BODY) ***/
    PROCEDURE create_coordinate_header(coordinate_#_ty  VARCHAR2,
                                       coordinate_#_tty VARCHAR2,
                                       dimensions       names_tty,
                                       dim_ids          names_tty,
                                       mobject_#_ty     names_tty);
    
    PROCEDURE create_conlevel_header(conlevel_#_ty   VARCHAR2,
                                     conlevel_#_tty  VARCHAR2,
                                     dimensions      names_tty,
                                     dim_ids         names_tty);
    
    PROCEDURE create_measure_header(measure_#_ty  VARCHAR2,
                                    measure_#_tty VARCHAR2,
                                    conlevel_#_ty VARCHAR2);
    
    PROCEDURE create_measure_collect_header(measure_#_collections VARCHAR2,
                                            measure_#_ty          VARCHAR2,
                                            measure_#_tty         VARCHAR2,
                                            conlevel_#_ty         VARCHAR2);
    
    PROCEDURE create_measure_table_header(measure_table_#_ty  VARCHAR2,
                                          measure_table_#_tty VARCHAR2,
                                          conlevel_#_ty       VARCHAR2);
    
    PROCEDURE create_measure_meta_header(measure_meta_#_ty  VARCHAR2,
                                         measure_meta_#_tty VARCHAR2,
                                         conlevel_#_ty      VARCHAR2);
    
    PROCEDURE create_conlvl_ancestor_header(conlvl_ancestor_#_ty  VARCHAR2,
                                            conlvl_ancestor_#_tty VARCHAR2,
                                            mrel_#_ty             VARCHAR2,
                                            conlevel_#_ty         VARCHAR2);
    
    PROCEDURE create_mrel_header(mrel_#_ty             VARCHAR2,
                                 mrel_#_trty           VARCHAR2,
                                 dimensions            names_tty,
                                 dim_ids               names_tty,
                                 coordinate_#_ty       VARCHAR2,
                                 conlevel_#_ty         VARCHAR2,
                                 conlvl_ancestor_#_tty VARCHAR2,
                                 measure_#_ty          VARCHAR2,
                                 measure_#_tty         VARCHAR2,
                                 measure_table_#_tty   VARCHAR2,
                                 measure_meta_#_tty    VARCHAR2);
    
    PROCEDURE create_mrel_value_header(mrel_#_value_ty  VARCHAR2,
                                       mrel_#_value_tty VARCHAR2,
                                       dimensions        names_tty,
                                       dim_ids           names_tty);
    
    PROCEDURE create_mcube_header(cname            VARCHAR2,
                                  mcube_#_ty       VARCHAR2,
                                  mrel_#_ty        VARCHAR2,
                                  mrel_#_trty      VARCHAR2,
                                  mrel_#_value_tty VARCHAR2,
                                  conlevel_#_ty    VARCHAR2,
                                  coordinate_#_ty  VARCHAR2,
                                  coordinate_#_tty VARCHAR2,
                                  measure_#_ty     VARCHAR2,
                                  measure_#_tty    VARCHAR2,
                                  queryview_#_ty   VARCHAR2,
                                  dimensions       names_tty,
                                  dim_ids          names_tty,
                                  dimension_#_ty   names_tty,
                                  mobject_#_ty     names_tty);
    
    PROCEDURE create_expressions_header(expr_#_ty         VARCHAR2,
                                        expr_#_tty        VARCHAR2,
                                        dice_expr_#_ty    VARCHAR2,
                                        slice_expr_#_ty   VARCHAR2,
                                        project_expr_#_ty VARCHAR2,
                                        queryview_#_ty    VARCHAR2,
                                        coordinate_#_ty   VARCHAR2,
                                        dimensions        names_tty,
                                        dim_ids           names_tty);
    
    PROCEDURE create_queryview_header(mcube_#_ty        VARCHAR2,
                                      mrel_#_trty       VARCHAR2,
                                      conlevel_#_ty     VARCHAR2,
                                      coordinate_#_ty   VARCHAR2,
                                      queryview_#_ty    VARCHAR2,
                                      expr_#_tty        VARCHAR2,
                                      dice_expr_#_ty    VARCHAR2,
                                      slice_expr_#_ty   VARCHAR2,
                                      project_expr_#_ty VARCHAR2,
                                      dimensions      names_tty,
                                      dim_ids names_tty);
    
    PROCEDURE create_measure_collect_body(measure_#_collections VARCHAR2,
                                          measure_#_ty         VARCHAR2,
                                          measure_#_tty        VARCHAR2,
                                          conlevel_#_ty        VARCHAR2);
    
    PROCEDURE create_coordinate_body(coordinate_#_ty VARCHAR2,
                                     dimensions      names_tty,
                                     dim_ids names_tty,
                                     mobject_#_ty    names_tty,
                                     mobject_tables  names_tty);
    
    PROCEDURE create_conlevel_body(conlevel_#_ty   VARCHAR2,
                                   dimensions      names_tty,
                                   dim_ids names_tty);
    
    PROCEDURE create_expressions_body(expr_#_ty         VARCHAR2,
                                      expr_#_tty        VARCHAR2,
                                      dice_expr_#_ty    VARCHAR2,
                                      slice_expr_#_ty   VARCHAR2,
                                      project_expr_#_ty VARCHAR2,
                                      queryview_#_ty    VARCHAR2,
                                      coordinate_#_ty   VARCHAR2,
                                      dimensions        names_tty,
                                      dim_ids           names_tty);
                                      
    PROCEDURE create_queryview_body(mcube_#_ty        VARCHAR2,
                                    mrel_#_trty       VARCHAR2,
                                    mrel_table        VARCHAR2,
                                    conlevel_#_ty     VARCHAR2,
                                    coordinate_#_ty   VARCHAR2,
                                    measure_#_ty      VARCHAR2,
                                    measure_#_tty     VARCHAR2,
                                    queryview_#_ty    VARCHAR2,
                                    expr_#_tty        VARCHAR2,
                                    dice_expr_#_ty    VARCHAR2,
                                    slice_expr_#_ty   VARCHAR2,
                                    project_expr_#_ty VARCHAR2,
                                    dimensions        names_tty,
                                    dim_ids           names_tty,
                                    mobject_tables    names_tty);
    
    PROCEDURE create_mcube_body(cname                 VARCHAR2,
                                mcube_#_ty            VARCHAR2,
                                mrel_#_ty             VARCHAR2,
                                mrel_#_trty           VARCHAR2,
                                mrel_table            VARCHAR2,
                                mrel_id_seq           VARCHAR2,
                                coordinate_#_ty       VARCHAR2,
                                coordinate_#_tty      VARCHAR2,
                                conlevel_#_ty         VARCHAR2,
                                conlevel_#_tty        VARCHAR2,
                                measure_#_ty          VARCHAR2,
                                measure_#_tty         VARCHAR2,
                                measure_meta_#_ty     VARCHAR2,
                                measure_meta_#_tty    VARCHAR2,
                                measure_table_#_ty    VARCHAR2,
                                measure_table_#_tty   VARCHAR2,
                                measure_#_collections VARCHAR2,
                                conlvl_ancestor_#_ty  VARCHAR2,
                                conlvl_ancestor_#_tty VARCHAR2,
                                queryview_#_ty        VARCHAR2,
                                expr_#_ty             VARCHAR2,
                                expr_#_tty            VARCHAR2,
                                dice_expr_#_ty        VARCHAR2,
                                slice_expr_#_ty       VARCHAR2,
                                project_expr_#_ty     VARCHAR2,
                                mrel_#_value_ty       VARCHAR2,
                                mrel_#_value_tty      VARCHAR2,
                                dimensions            names_tty,
                                dim_ids               names_tty,
                                dimension_#_ty        names_tty,
                                mobject_#_ty          names_tty,
                                mobject_tables        names_tty);
    
    PROCEDURE create_mrel_body(mrel_#_ty             VARCHAR2,
                               mrel_#_trty           VARCHAR2,
                               dimensions            names_tty,
                               dim_ids               names_tty,
                               coordinate_#_ty       VARCHAR2,
                               conlevel_#_ty         VARCHAR2,
                               conlevel_#_tty        VARCHAR2,
                               conlvl_ancestor_#_ty  VARCHAR2,
                               conlvl_ancestor_#_tty VARCHAR2,
                               measure_#_ty          VARCHAR2,
                               measure_#_tty         VARCHAR2,
                               measure_table_#_ty    VARCHAR2,
                               measure_table_#_tty   VARCHAR2,
                               measure_meta_#_ty     VARCHAR2,
                               measure_meta_#_tty    VARCHAR2,
                               measure_#_collections VARCHAR2,
                               cname                 VARCHAR2,
                               mrel_table            VARCHAR2,
                               mobject_tables        names_tty);
END;
/


CREATE OR REPLACE PACKAGE BODY mcube AS
    
    FUNCTION create_mcube(cname VARCHAR2,
                          dimensions names_tty, 
                          root_coordinate names_tty) 
            RETURN REF mcube_ty IS
        
        id VARCHAR2(10);
        
        -- m-cube related types that are determined in this function
        mrel_table            VARCHAR2(30);       
        mcube_#_ty            VARCHAR2(30);
        mrel_#_ty             VARCHAR2(30);
        mrel_#_trty           VARCHAR2(30);
        coordinate_#_ty       VARCHAR2(30);
        coordinate_#_tty      VARCHAR2(30);
        conlevel_#_ty         VARCHAR2(30);
        conlevel_#_tty        VARCHAR2(30);
        measure_#_ty          VARCHAR2(30);
        measure_#_tty         VARCHAR2(30);
        measure_meta_#_ty     VARCHAR2(30);
        measure_meta_#_tty    VARCHAR2(30);
        measure_table_#_ty    VARCHAR2(30);
        measure_table_#_tty   VARCHAR2(30);
        measure_#_collections VARCHAR2(30);
        conlvl_ancestor_#_ty  VARCHAR2(30);
        conlvl_ancestor_#_tty VARCHAR2(30);
        queryview_#_ty        VARCHAR2(30);
        expr_#_ty             VARCHAR2(30);
        expr_#_tty            VARCHAR2(30);
        dice_expr_#_ty        VARCHAR2(30);
        slice_expr_#_ty       VARCHAR2(30);
        project_expr_#_ty     VARCHAR2(30);
        mrel_#_value_ty       VARCHAR2(30);
        mrel_#_value_tty      VARCHAR2(30);
        
        mrel_id_seq_pkg       VARCHAR2(30);
        mrel_id_seq_seq       VARCHAR2(30);
        
        -- dimension-related types determined outside of this function
        dim_ids names_tty := names_tty();
        dimension_#_ty  names_tty := names_tty();
        mobject_#_ty    names_tty := names_tty();
        mobject_tables  names_tty := names_tty();
        
        new_cube mcube_ty;
        new_cube_ref REF mcube_ty;
        
        -- dbms_sql
        sql_cursor INTEGER;
        rows_processed INTEGER;
        
        -- cursor variables
        i INTEGER;
        dim dimension_ty;
        
        has_root_coordinate BOOLEAN 
            := root_coordinate IS NOT NULL AND
               root_coordinate.COUNT = dimensions.COUNT;
        
        create_mcube                   VARCHAR2(10000);
        create_mcube_dimensions_var    VARCHAR2(1500);
        create_mcube_dimensions_var1   VARCHAR2(1500);
        create_mcube_dimensions_select VARCHAR2(1500);
        create_mcube_coordinate_var    VARCHAR2(1500);
        create_mcube_coordinate_var1   VARCHAR2(1500);
        create_mcube_coordinate_select VARCHAR2(1500);
    BEGIN
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            -- extend the collections
            dim_ids.EXTEND;
            dimension_#_ty.EXTEND;
            mobject_#_ty.EXTEND;
            mobject_tables.EXTEND;
            
            -- determine the dimension's id, type and m-object type.
            EXECUTE IMMEDIATE
                'SELECT d.id, d.dimension_#_ty, d.mobject_#_ty, d.mobject_table' || chr(10) ||
                'FROM   dimensions d' || chr(10) ||
                'WHERE  d.dname = :1'
            INTO dim_ids(dim_ids.LAST), 
                 dimension_#_ty(dimension_#_ty.LAST), 
                 mobject_#_ty(mobject_#_ty.LAST),
                 mobject_tables(mobject_tables.LAST)
            USING dimensions(i);
            
            -- complete the dynamic string for the m-cube creation
            IF i > 1 THEN
                create_mcube_coordinate_var1 := create_mcube_coordinate_var1 || ', ';
            END IF;
            
            create_mcube_dimensions_var := create_mcube_dimensions_var ||
                '    ' || dimensions(i) || ' REF ' || dimension_#_ty(i) || ';' || chr(10);
            
            create_mcube_dimensions_var1 := create_mcube_dimensions_var1 ||
                ', ' || dimensions(i);
            
            create_mcube_dimensions_select := create_mcube_dimensions_select ||
                '    SELECT TREAT(REF(d) AS REF ' || dimension_#_ty(i) || ') INTO ' || dimensions(i) || chr(10) ||
                '    FROM   dimensions d' || chr(10) ||
                '    WHERE  d.dname = :' || dimensions(i) || ';' || chr(10) ||
                '    ' || chr(10);
            
            create_mcube_coordinate_var := create_mcube_coordinate_var ||
                '    ' || dim_ids(i) || '_obj REF ' || mobject_#_ty(i) || ';' || chr(10);
            
            create_mcube_coordinate_var1 := create_mcube_coordinate_var1 ||
                dim_ids(i) || '_obj, :' || dim_ids(i) || '_oname';
            
            create_mcube_coordinate_select := create_mcube_coordinate_select ||
                '    SELECT TREAT(REF(o) AS REF ' || mobject_#_ty(i) || ') INTO ' || dim_ids(i) || '_obj' || chr(10) ||
                '    FROM   ' || mobject_tables(i) || ' o' || chr(10) ||
                '    WHERE  o.oname = :' || dim_ids(i) || '_oname;' || chr(10) ||
                '    ' || chr(10);
                        
            i := dimensions.NEXT(i);
        END LOOP;
        
        -- create the mcubes table if it does not already exist.
        mcube_ddl.create_mcubes_table();
                
        EXECUTE IMMEDIATE
            'SELECT ''c'' || mcubes_seq_pkg.NEXT_VAL() FROM dual'
            INTO id; 
            
        -- determine the names of the dynamically created types.
        mrel_table := id;
        mcube_#_ty := 'mcube_' || id || '_ty';
        mrel_#_ty := 'mrel_' || id || '_ty';
        mrel_#_trty := 'mrel_' || id || '_trty';
        coordinate_#_ty := 'coordinate_' || id || '_ty';
        coordinate_#_tty := 'coordinate_' || id || '_tty';
        conlevel_#_ty := 'conlevel_' || id || '_ty';
        conlevel_#_tty := 'conlevel_' || id || '_tty';
        measure_#_ty := 'measure_' || id || '_ty';
        measure_#_tty := 'measure_' || id || '_tty';
        measure_meta_#_ty := 'measure_meta_' || id || '_ty';
        measure_meta_#_tty := 'measure_meta_' || id || '_tty';
        measure_table_#_ty := 'measure_table_' || id || '_ty';
        measure_table_#_tty := 'measure_table_' || id || '_tty';
        measure_#_collections := 'measure_' || id || '_collections';
        conlvl_ancestor_#_ty := 'conlvl_ancestor_' || id || '_ty';
        conlvl_ancestor_#_tty := 'conlvl_ancestor_' || id || '_tty';
        queryview_#_ty := 'queryview_' || id || '_ty';
        expr_#_ty := 'expr_' || id || '_ty';
        expr_#_tty := 'expr_' || id || '_tty';
        dice_expr_#_ty := 'dice_expr_' || id || '_ty';
        slice_expr_#_ty := 'slice_expr_' || id || '_ty';
        project_expr_#_ty := 'project_expr_' || id || '_ty';
        mrel_#_value_ty := 'mrel_' || id || '_value_ty';
        mrel_#_value_tty := 'mrel_' || id || '_value_tty';
        
        mrel_id_seq_pkg := id || '_seq_pkg';
        mrel_id_seq_seq := id || '_seq_seq';
        
        dimension.create_sequence(mrel_id_seq_pkg, mrel_id_seq_seq);
        
        -- dynamically specialize the object types;
        -- create tables, triggers, and packages.        
        mcube_ddl.create_coordinate_header(coordinate_#_ty,
                                           coordinate_#_tty,
                                           dimensions,
                                           dim_ids,
                                           mobject_#_ty);
        
        mcube_ddl.create_conlevel_header(conlevel_#_ty,
                                         conlevel_#_tty,
                                         dimensions,
                                         dim_ids);
        
        mcube_ddl.create_measure_header(measure_#_ty,
                                        measure_#_tty,
                                        conlevel_#_ty);
        
        mcube_ddl.create_measure_collect_header(measure_#_collections,
                                                measure_#_ty,
                                                measure_#_tty,
                                                conlevel_#_ty);
        
        mcube_ddl.create_measure_table_header(measure_table_#_ty,
                                              measure_table_#_tty,
                                              conlevel_#_ty);
        
        mcube_ddl.create_measure_meta_header(measure_meta_#_ty,
                                             measure_meta_#_tty,
                                             conlevel_#_ty);
        
        mcube_ddl.create_conlvl_ancestor_header(conlvl_ancestor_#_ty,
                                                conlvl_ancestor_#_tty,
                                                mrel_#_ty,
                                                conlevel_#_ty);
        
        mcube_ddl.create_mrel_value_header(mrel_#_value_ty,
                                           mrel_#_value_tty,
                                           dimensions,
                                           dim_ids);
        
        mcube_ddl.create_mrel_header(mrel_#_ty,
                                     mrel_#_trty,
                                     dimensions,
                                     dim_ids,
                                     coordinate_#_ty,
                                     conlevel_#_ty,
                                     conlvl_ancestor_#_tty,
                                     measure_#_ty,
                                     measure_#_tty,
                                     measure_table_#_tty,
                                     measure_meta_#_tty);
        
        mcube_ddl.create_mrel_table(mrel_#_ty,
                                    mrel_table,
                                    id,
                                    dim_ids);
        
        mcube_ddl.create_expressions_header(expr_#_ty,
                                            expr_#_tty,
                                            dice_expr_#_ty,
                                            slice_expr_#_ty,
                                            project_expr_#_ty,
                                            queryview_#_ty,
                                            coordinate_#_ty,
                                            dimensions,
                                            dim_ids);
        
        mcube_ddl.create_queryview_header(mcube_#_ty,
                                          mrel_#_trty,
                                          conlevel_#_ty,
                                          coordinate_#_ty,
                                          queryview_#_ty,
                                          expr_#_tty,
                                          dice_expr_#_ty,
                                          slice_expr_#_ty,
                                          project_expr_#_ty,
                                          dimensions,
                                          dim_ids);
                                          
        mcube_ddl.create_mcube_header(cname, 
                                      mcube_#_ty,
                                      mrel_#_ty,
                                      mrel_#_trty,
                                      mrel_#_value_tty,
                                      conlevel_#_ty,
                                      coordinate_#_ty,
                                      coordinate_#_tty,
                                      measure_#_ty,
                                      measure_#_tty,
                                      queryview_#_ty,
                                      dimensions,
                                      dim_ids,
                                      dimension_#_ty,
                                      mobject_#_ty);
        
        mcube_ddl.create_coordinate_body(coordinate_#_ty,
                                         dimensions,
                                         dim_ids,
                                         mobject_#_ty,
                                         mobject_tables);
        
        mcube_ddl.create_conlevel_body(conlevel_#_ty,
                                       dimensions,
                                       dim_ids);
        
        mcube_ddl.create_measure_collect_body(measure_#_collections,
                                              measure_#_ty,
                                              measure_#_tty,
                                              conlevel_#_ty);
        
        
        mcube_ddl.create_expressions_body(expr_#_ty,
                                          expr_#_tty,
                                          dice_expr_#_ty,
                                          slice_expr_#_ty,
                                          project_expr_#_ty,
                                          queryview_#_ty,
                                          coordinate_#_ty,
                                          dimensions,
                                          dim_ids);
                                            
        mcube_ddl.create_queryview_body(mcube_#_ty,
                                        mrel_#_trty,
                                        mrel_table,
                                        conlevel_#_ty,
                                        coordinate_#_ty,
                                        measure_#_ty,
                                        measure_#_tty,
                                        queryview_#_ty,
                                        expr_#_tty,
                                        dice_expr_#_ty,
                                        slice_expr_#_ty,
                                        project_expr_#_ty,
                                        dimensions,
                                        dim_ids,
                                        mobject_tables);
                                          
        mcube_ddl.create_mcube_body(cname,
                                    mcube_#_ty,
                                    mrel_#_ty,
                                    mrel_#_trty,
                                    mrel_table,
                                    mrel_id_seq_pkg,
                                    coordinate_#_ty,
                                    coordinate_#_tty,
                                    conlevel_#_ty,
                                    conlevel_#_tty,
                                    measure_#_ty,
                                    measure_#_tty,
                                    measure_meta_#_ty,
                                    measure_meta_#_tty,
                                    measure_table_#_ty,
                                    measure_table_#_tty,
                                    measure_#_collections,
                                    conlvl_ancestor_#_ty,
                                    conlvl_ancestor_#_tty,
                                    queryview_#_ty,
                                    expr_#_ty,
                                    expr_#_tty,
                                    dice_expr_#_ty,
                                    slice_expr_#_ty,
                                    project_expr_#_ty,
                                    mrel_#_value_ty,
                                    mrel_#_value_tty,
                                    dimensions,
                                    dim_ids,
                                    dimension_#_ty,
                                    mobject_#_ty,
                                    mobject_tables);
        
        mcube_ddl.create_mrel_body(mrel_#_ty,
                                   mrel_#_trty,
                                   dimensions,
                                   dim_ids,
                                   coordinate_#_ty,
                                   conlevel_#_ty,
                                   conlevel_#_tty,
                                   conlvl_ancestor_#_ty,
                                   conlvl_ancestor_#_tty,
                                   measure_#_ty,
                                   measure_#_tty,
                                   measure_table_#_ty,
                                   measure_table_#_tty,
                                   measure_meta_#_ty,
                                   measure_meta_#_tty,
                                   measure_#_collections,
                                   cname,
                                   mrel_table,
                                   mobject_tables);
        
        -- dynamically build the code to create the m-cube.
        create_mcube := 
            'DECLARE' || chr(10) ||
            '    new_cube ' || mcube_#_ty || ';' || chr(10) ||
                 create_mcube_dimensions_var ||
                 create_mcube_coordinate_var ||
            'BEGIN' || chr(10) ||
                 create_mcube_dimensions_select ||
            '    -- create the new mcube' || chr(10) ||
            '    new_cube := ' || mcube_#_ty || '(:cname, :id' || create_mcube_dimensions_var1 || ');' || chr(10) ||
            '    ' || chr(10);
        
        IF has_root_coordinate THEN
            create_mcube := create_mcube || 
                '    -- set the new m-cube''s root-coordinate' || chr(10) ||
                     create_mcube_coordinate_select ||   
                '    new_cube.root_coordinate := ' || coordinate_#_ty || '(' || create_mcube_coordinate_var1  || ');' || chr(10) ||
                '    ' || chr(10);
        END IF;
            
        create_mcube := create_mcube ||
            '    -- insert the dimension into the dimensions table' || chr(10) ||
            '    INSERT INTO mcubes VALUES(new_cube);' || chr(10) ||
            '    ' || chr(10) ||
            '    --:new_cube := new_cube;' || chr(10) ||
            'END;';
        
        ---- use dynamic sql to create the m-cube
        sql_cursor := dbms_sql.open_cursor;
        
        dbms_sql.parse(sql_cursor,
                       create_mcube,
                       dbms_sql.NATIVE);
        
        -- 10g compatibility: do not retrieve newly created m-cube
        --dbms_sql.bind_variable(sql_cursor, 'new_cube', new_cube);
        dbms_sql.bind_variable(sql_cursor, 'cname', cname);
        dbms_sql.bind_variable(sql_cursor, 'id', id);
        
        -- bind the names of the dimensions and the root_coordinate
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            dbms_sql.bind_variable(sql_cursor, dimensions(i), dimensions(i));
            
            i := dimensions.NEXT(i);
        END LOOP;
        
        -- bind the names of the m-objects of the 
        -- root-coordinate (if specified)
        IF has_root_coordinate THEN 
            i := root_coordinate.FIRST;
            WHILE i IS NOT NULL LOOP
                dbms_sql.bind_variable(sql_cursor, 
                                       dim_ids(i) || '_oname',
                                       root_coordinate(i));
                
                i := root_coordinate.NEXT(i);
            END LOOP;
        END IF;
        
        -- execute the statements
        rows_processed := dbms_sql.execute(sql_cursor);
        
        -- retrieve the new m-cube object
        --IF rows_processed > 0 THEN
        --    dbms_sql.variable_value(sql_cursor, 'new_cube', new_cube);
        --END IF;
        
        -- close the cursor
        dbms_sql.close_cursor(sql_cursor);
        
        -- get the reference of the new dimension
        -- TODO: Perhaps make a function 
        --       mcube_ty.get_reference RETURN REF mcube_ty,
        --       so that dynamic SQL is eliminated here.
        EXECUTE IMMEDIATE 'SELECT REF(mc) FROM mcubes mc WHERE mc.cname = :1'
            INTO new_cube_ref
            USING cname;
        
        RETURN new_cube_ref;
    END;
    
    PROCEDURE delete_mcube(cname VARCHAR2) IS        
        measure_tables names_tty;
                
        mcube mcube_ty;
        
        mrel_table            VARCHAR2(30);
        mrel_id_seq           VARCHAR2(30);
        mcube_#_ty            VARCHAR2(30);
        mrel_#_ty             VARCHAR2(30);
        mrel_#_trty           VARCHAR2(30);
        coordinate_#_ty       VARCHAR2(30);
        coordinate_#_tty      VARCHAR2(30);
        conlevel_#_ty         VARCHAR2(30);
        conlevel_#_tty        VARCHAR2(30);
        measure_#_ty          VARCHAR2(30);
        measure_#_tty         VARCHAR2(30);
        measure_meta_#_ty     VARCHAR2(30);
        measure_meta_#_tty    VARCHAR2(30);
        measure_table_#_ty    VARCHAR2(30);
        measure_table_#_tty   VARCHAR2(30);
        conlvl_ancestor_#_ty  VARCHAR2(30);
        conlvl_ancestor_#_tty VARCHAR2(30);
        measure_#_collections VARCHAR2(30);
        queryview_#_ty        VARCHAR2(30);
        expr_#_ty             VARCHAR2(30);
        expr_#_tty            VARCHAR2(30);
        dice_expr_#_ty        VARCHAR2(30);
        slice_expr_#_ty       VARCHAR2(30);
        project_expr_#_ty     VARCHAR2(30);
        mrel_#_value_ty       VARCHAR2(30);
        mrel_#_value_tty      VARCHAR2(30);
        
        i INTEGER;
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT mrel_table, mrel_id_seq, mcube_#_ty, mrel_#_ty, mrel_#_ty, mrel_#_trty,' || chr(10) ||
            '       coordinate_#_ty, coordinate_#_tty, conlevel_#_ty, conlevel_#_tty,' || chr(10) ||
            '       measure_#_ty, measure_#_tty, measure_meta_#_ty, measure_meta_#_tty,' || chr(10) ||
            '       measure_table_#_ty, measure_table_#_tty, conlvl_ancestor_#_ty,' || chr(10) ||
            '       conlvl_ancestor_#_tty, measure_#_collections, queryview_#_ty,' || chr(10) ||
            '       expr_#_ty, expr_#_tty, dice_expr_#_ty, slice_expr_#_ty,' || chr(10) ||
            '       project_expr_#_ty, mrel_#_value_ty, mrel_#_value_tty' || chr(10) ||
            'FROM   mcubes' || chr(10) ||
            'WHERE  cname = :1' || chr(10)
        INTO mrel_table, mrel_id_seq, mcube_#_ty, mrel_#_ty, mrel_#_ty, mrel_#_trty,
             coordinate_#_ty, coordinate_#_tty, conlevel_#_ty, conlevel_#_tty,
             measure_#_ty, measure_#_tty, measure_meta_#_ty, measure_meta_#_tty,
             measure_table_#_ty, measure_table_#_tty, conlvl_ancestor_#_ty,
             conlvl_ancestor_#_tty, measure_#_collections, queryview_#_ty,
             expr_#_ty, expr_#_tty, dice_expr_#_ty, slice_expr_#_ty,
             project_expr_#_ty, mrel_#_value_ty, mrel_#_value_tty
        USING cname;
        
        -- delete the m-cube from the mcubes tabke
        EXECUTE IMMEDIATE
            'DELETE FROM mcubes WHERE cname = :1'
        USING cname;
        
        EXECUTE IMMEDIATE
            'SELECT DISTINCT t.table_name' || chr(10) ||
            'FROM   ' || mrel_table || ' mr, TABLE(mr.measure_tables) t' || chr(10)
        BULK COLLECT INTO measure_tables;
        
        i := measure_tables.FIRST;
        WHILE i IS NOT NULL LOOP
            EXECUTE IMMEDIATE
                'DROP TABLE ' || measure_tables(i);
            
            i := measure_tables.NEXT(i);
        END LOOP;
        
        EXECUTE IMMEDIATE
            'DROP TABLE ' || mrel_table;
        
        EXECUTE IMMEDIATE
            'DROP TYPE ' || mcube_#_ty || ' VALIDATE';
        
        EXECUTE IMMEDIATE
            'DROP TYPE ' || mrel_#_ty  || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || mrel_#_trty  || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || coordinate_#_ty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || coordinate_#_tty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || conlevel_#_ty  || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || conlevel_#_tty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || measure_#_ty  || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || measure_#_tty  || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || measure_meta_#_ty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || measure_meta_#_tty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || measure_table_#_ty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || measure_table_#_tty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || conlvl_ancestor_#_ty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || conlvl_ancestor_#_tty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP PACKAGE ' || measure_#_collections;

        EXECUTE IMMEDIATE
            'DROP TYPE ' || queryview_#_ty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || expr_#_ty  || ' FORCE';
        
        EXECUTE IMMEDIATE
            'DROP TYPE ' || expr_#_tty  || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || dice_expr_#_ty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || slice_expr_#_ty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || project_expr_#_ty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || mrel_#_value_ty || ' FORCE';

        EXECUTE IMMEDIATE
            'DROP TYPE ' || mrel_#_value_tty || ' FORCE';
        
        EXECUTE IMMEDIATE
            'BEGIN' || chr(10) ||
            '    ' || mrel_id_seq || '.delete_sequence;' || chr(10) ||
            'END;' || chr(10);
        
        EXECUTE IMMEDIATE
            'DROP PACKAGE ' || mrel_id_seq;
    END;
END;
/

CREATE OR REPLACE PACKAGE BODY mcube_ddl AS      
    /*** TABLES ***/
    PROCEDURE create_mcubes_table IS
        cnt INTEGER;
    BEGIN
        -- check if the table already exists
        SELECT COUNT(*) INTO cnt
        FROM   user_tab_columns utc
        WHERE  utc.table_name = 'MCUBES';
        
        -- only create the table if it does not exist already
        IF cnt = 0 THEN
            dimension.create_sequence('mcubes_seq_pkg', 'mcubes_seq_seq');
            
            -- create the dimensions table
            EXECUTE IMMEDIATE
                'CREATE TABLE mcubes OF mcube_ty (' || chr(10) ||
                '    cname PRIMARY KEY,' || chr(10) ||
                '    id UNIQUE NOT NULL)';
        END IF;
    END;
    
    PROCEDURE create_mrel_table(mrel_#_ty        VARCHAR2,
                                mrel_table       VARCHAR2,
                                mcube_id VARCHAR2,
                                dim_ids  names_tty) IS
    
        mrel_table_declaration   CLOB;
        mrel_coordinate_names    CLOB;
        mrel_coordinate_not_null CLOB;
        
        i INTEGER;
    BEGIN
        -- loop through the names of the dimensions
        i := dim_ids.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                mrel_coordinate_names := mrel_coordinate_names || ', ';
            END IF;
            
            mrel_coordinate_names := mrel_coordinate_names ||
                'coordinate.' || dim_ids(i) || '_oname';       
            
            mrel_coordinate_not_null := mrel_coordinate_not_null ||
                '     CHECK(coordinate.' || dim_ids(i) || '_oname IS NOT NULL),' || chr(10);
            
            -- increment cursor variable
            i := dim_ids.NEXT(i);
        END LOOP;
        ----
        
        mrel_table_declaration :=
            'CREATE TABLE ' || mrel_table || ' OF ' || mrel_#_ty || chr(10) ||
            '    (PRIMARY KEY (' || mrel_coordinate_names || '),' || chr(10) ||
            '     id UNIQUE NOT NULL, ' || chr(10) ||
            '     coordinate NOT NULL, ' || chr(10) ||
                  mrel_coordinate_not_null ||
            '     mcube NOT NULL SCOPE IS mcubes)' || chr(10) ||
            '    NESTED TABLE ancestors STORE AS ' || mcube_id || '_ancestors' || chr(10) ||
            '    NESTED TABLE measure_tables STORE AS ' || mcube_id || '_measures' || chr(10) ||
            '    NESTED TABLE measure_metadata STORE AS ' || mcube_id || '_metadata';
        
        --dbms_output.put_line(mrel_table_declaration);
        EXECUTE IMMEDIATE mrel_table_declaration;
    END;
    /*** ***/
    
    
    /*** HEADERS ***/
    PROCEDURE create_coordinate_header(coordinate_#_ty VARCHAR2,
                                       coordinate_#_tty VARCHAR2,
                                       dimensions      names_tty,
                                       dim_ids names_tty,
                                       mobject_#_ty    names_tty) IS
        coordinate_header   CLOB;
        
        coordinate_mobjects CLOB;
        coordinate_onames   CLOB;
        
        coordinate_constructor  CLOB;
        coordinate_constructor1 CLOB;
        
        coordinate_constructor_alt  CLOB;
        coordinate_constructor_alt1 CLOB;
        
        coordinate_constructor2_alt  CLOB;
        coordinate_constructor2_alt1 CLOB;
        
        coordinate_equals CLOB;
        
        coordinate_tostring CLOB;
        
        coordinate_compare  CLOB;
        coordinate_is_sub   CLOB;
        
        coordinate_tty CLOB;
        
        i INTEGER;
    BEGIN        
        -- loop through the names of the dimensions
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                coordinate_constructor1 := coordinate_constructor1 || ', ';
                coordinate_constructor_alt1 := coordinate_constructor_alt1 || ', ';
                coordinate_constructor2_alt1 := coordinate_constructor2_alt1 || ', ';
            END IF;
            
            coordinate_mobjects := coordinate_mobjects ||
                '    ' || dim_ids(i) || '_obj REF ' || mobject_#_ty(i) || ',' || chr(10);
            
            coordinate_onames := coordinate_onames ||
                '    ' || dim_ids(i) || '_oname VARCHAR2(30),' || chr(10);
            
            coordinate_constructor1 := coordinate_constructor1 ||
                dim_ids(i) || '_obj REF ' || mobject_#_ty(i);
            
            coordinate_constructor_alt1 := coordinate_constructor_alt1 ||
                dim_ids(i) || '_obj REF ' || mobject_#_ty(i) || ', ' || 
                dim_ids(i) || '_oname VARCHAR2';
            
            coordinate_constructor2_alt1 := coordinate_constructor2_alt1 ||
                dim_ids(i) || '_oname VARCHAR2';
                
            -- increment cursor variable
            i := dimensions.NEXT(i);
        END LOOP;
        ----
        
        coordinate_constructor :=
            '    CONSTRUCTOR FUNCTION ' || coordinate_#_ty || '(' || coordinate_constructor1 || ') RETURN SELF AS RESULT,' || chr(10);
            
        coordinate_constructor_alt :=
            '    CONSTRUCTOR FUNCTION ' || coordinate_#_ty || '(' || coordinate_constructor_alt1 || ') RETURN SELF AS RESULT,' || chr(10);
        
        coordinate_constructor2_alt :=
            '    CONSTRUCTOR FUNCTION ' || coordinate_#_ty || '(' || coordinate_constructor2_alt1 || ') RETURN SELF AS RESULT,' || chr(10);
        
        coordinate_equals :=
            '    MEMBER FUNCTION equals(other ' || coordinate_#_ty || ') RETURN INTEGER,' || chr(10);
        
        coordinate_tostring :=
            '    MEMBER FUNCTION to_string RETURN VARCHAR2,' || chr(10);
        
        coordinate_compare :=
            '    ORDER MEMBER FUNCTION compare_to(other ' || coordinate_#_ty || ') RETURN INTEGER,' || chr(10);
        
        coordinate_is_sub :=
            '    MEMBER FUNCTION is_sub_coordinate_of(other ' || coordinate_#_ty || ') RETURN BOOLEAN' || chr(10);
        
        coordinate_header :=
            'CREATE OR REPLACE TYPE ' || coordinate_#_ty || ' AS OBJECT(' || chr(10) ||
                 coordinate_mobjects ||
                 coordinate_onames ||
                 coordinate_constructor ||
                 coordinate_constructor_alt ||
                 coordinate_constructor2_alt ||
                 coordinate_equals ||
                 coordinate_tostring ||
                 coordinate_compare ||
                 coordinate_is_sub ||
            ');';
        
        coordinate_tty :=
            'CREATE OR REPLACE TYPE ' || coordinate_#_tty || ' AS TABLE OF ' || coordinate_#_ty || ';';
        
        --dbms_output.put_line(coordinate_header);
        --dbms_output.put_line(coordinate_tty);
        EXECUTE IMMEDIATE coordinate_header;
        EXECUTE IMMEDIATE coordinate_tty;
    END;
    
    PROCEDURE create_conlevel_header(conlevel_#_ty   VARCHAR2,
                                     conlevel_#_tty  VARCHAR2,
                                     dimensions      names_tty,
                                     dim_ids names_tty) IS
        conlevel_header CLOB;
        conlevel_onames CLOB;
        
        conlevel_trty CLOB;
        
        i INTEGER;
    BEGIN
        -- loop through the names of the dimensions
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP            
            conlevel_onames := conlevel_onames ||
                '    ' || dim_ids(i) || '_level VARCHAR2(30),' || chr(10);
            
            -- increment cursor variable
            i := dimensions.NEXT(i);
        END LOOP;
        ----
        
        conlevel_header :=
            'CREATE OR REPLACE TYPE ' || conlevel_#_ty || ' AS OBJECT(' || chr(10) ||
                 conlevel_onames ||
            '    ORDER MEMBER FUNCTION compare_to(other ' || conlevel_#_ty || ') RETURN INTEGER,' || chr(10) ||
            '    MEMBER FUNCTION equals(other ' || conlevel_#_ty || ') RETURN INTEGER,' || chr(10) ||
            '    MEMBER FUNCTION to_string RETURN VARCHAR2,' || chr(10) ||
            '    MEMBER FUNCTION to_string2 RETURN VARCHAR2' || chr(10) ||
            ');';
        
        conlevel_trty :=
            'CREATE OR REPLACE TYPE ' || conlevel_#_tty || ' AS TABLE OF ' || conlevel_#_ty || ';';
            
        EXECUTE IMMEDIATE conlevel_header;
        EXECUTE IMMEDIATE conlevel_trty;
        --dbms_output.put_line(conlevel_header);
    END;
    
    PROCEDURE create_measure_header(measure_#_ty  VARCHAR2,
                                    measure_#_tty VARCHAR2,
                                    conlevel_#_ty VARCHAR2) IS
        
        measure_ty CLOB;
        measure_tty CLOB;
    BEGIN
        measure_ty :=
            'CREATE OR REPLACE TYPE ' || measure_#_ty || ' AS OBJECT(' || chr(10) ||
            '    measure_name VARCHAR2(30),' || chr(10) ||
            '    measure_level ' || conlevel_#_ty || ',' || chr(10) ||
            '    table_name VARCHAR2(30),' || chr(10) ||
            '    data_type VARCHAR2(30),' || chr(10) ||
            '    data_length NUMBER,' || chr(10) ||
            '    data_scale NUMBER' || chr(10) ||
            ');';
        
        measure_tty :=
            'CREATE OR REPLACE TYPE ' || measure_#_tty || ' AS TABLE OF ' ||
                 measure_#_ty || ';';
        
        EXECUTE IMMEDIATE measure_ty;
        EXECUTE IMMEDIATE measure_tty;
        --dbms_output.put_line(measure_ty);
        --dbms_output.put_line(measure_tty);
    END;
    
    PROCEDURE create_measure_collect_header(measure_#_collections VARCHAR2,
                                            measure_#_ty         VARCHAR2,
                                            measure_#_tty        VARCHAR2,
                                            conlevel_#_ty        VARCHAR2) IS
        measure_collection CLOB;
    BEGIN
        measure_collection :=
            'CREATE OR REPLACE PACKAGE ' || measure_#_collections || ' IS' || chr(10) ||
            '    FUNCTION get_measure_by_table(measure_name VARCHAR2, ' || chr(10) ||
            '                                  table_name   VARCHAR2,' || chr(10) ||
            '                                  conlevel     ' || conlevel_#_ty || ') RETURN ' || measure_#_ty || ';' || chr(10) ||
            'END;';
        
        EXECUTE IMMEDIATE measure_collection;
    END;
    
    PROCEDURE create_measure_table_header(measure_table_#_ty  VARCHAR2,
                                          measure_table_#_tty VARCHAR2,
                                          conlevel_#_ty       VARCHAR2) IS
        measure_table_ty CLOB;
        measure_table_tty CLOB;
    BEGIN
        measure_table_ty :=
            'CREATE OR REPLACE TYPE ' || measure_table_#_ty || ' AS OBJECT(' || chr(10) ||
            '    conlevel ' || conlevel_#_ty || ',' || chr(10) ||
            '    table_name VARCHAR2(30)' || chr(10) ||
            ');';
        
        measure_table_tty :=
            'CREATE OR REPLACE TYPE ' || measure_table_#_tty || ' AS TABLE OF ' ||
                 measure_table_#_ty || ';';
        
        EXECUTE IMMEDIATE measure_table_ty;
        EXECUTE IMMEDIATE measure_table_tty;
        --dbms_output.put_line(measure_table_ty);
        --dbms_output.put_line(measure_table_tty);
    END;
    
    PROCEDURE create_measure_meta_header(measure_meta_#_ty  VARCHAR2,
                                         measure_meta_#_tty VARCHAR2,
                                         conlevel_#_ty      VARCHAR2) IS
        measure_meta_ty CLOB;
        measure_meta_tty CLOB;
    BEGIN
        measure_meta_ty :=
            'CREATE OR REPLACE TYPE ' || measure_meta_#_ty || ' AS OBJECT(' || chr(10) ||
            '    measure_name VARCHAR2(30),' || chr(10) ||
            '    measure_level ' || conlevel_#_ty || ',' || chr(10) ||
            '    metalevel VARCHAR2(30),' || chr(10) ||
            '    default_value NUMBER,' || chr(10) ||
            '    measure_value ANYDATA' || chr(10) ||
            ');';
        
        measure_meta_tty :=
            'CREATE OR REPLACE TYPE ' || measure_meta_#_tty || ' AS TABLE OF ' ||
                 measure_meta_#_ty || ';';
        
        EXECUTE IMMEDIATE measure_meta_ty;
        EXECUTE IMMEDIATE measure_meta_tty;
        --dbms_output.put_line(measure_meta_ty);
        --dbms_output.put_line(measure_meta_tty);
    END;
    
    PROCEDURE create_conlvl_ancestor_header(conlvl_ancestor_#_ty  VARCHAR2,
                                            conlvl_ancestor_#_tty VARCHAR2,
                                            mrel_#_ty             VARCHAR2,
                                            conlevel_#_ty         VARCHAR2) IS
        mrel_forward CLOB;
        
        conlvl_ancestor_ty CLOB;
        conlvl_ancestor_tty CLOB;
    BEGIN
        mrel_forward := 'CREATE OR REPLACE TYPE ' || mrel_#_ty || ';';
        
        conlvl_ancestor_ty :=
            'CREATE OR REPLACE TYPE ' || conlvl_ancestor_#_ty || ' AS OBJECT(' || chr(10) ||
            '    conlevel ' || conlevel_#_ty || ',' || chr(10) ||
            '    ancestor REF ' || mrel_#_ty || chr(10) ||
            ');';
        
        conlvl_ancestor_tty :=
            'CREATE OR REPLACE TYPE ' || conlvl_ancestor_#_tty || ' AS TABLE OF ' ||
                 conlvl_ancestor_#_ty || ';';
        
        EXECUTE IMMEDIATE mrel_forward;
        EXECUTE IMMEDIATE conlvl_ancestor_ty;
        EXECUTE IMMEDIATE conlvl_ancestor_tty;        
        --dbms_output.put_line(conlvl_ancestor_ty);
        --dbms_output.put_line(conlvl_ancestor_tty);
    END;
    
    
    PROCEDURE create_mrel_header(mrel_#_ty             VARCHAR2,
                                 mrel_#_trty           VARCHAR2,
                                 dimensions            names_tty,
                                 dim_ids               names_tty,
                                 coordinate_#_ty       VARCHAR2,
                                 conlevel_#_ty         VARCHAR2,
                                 conlvl_ancestor_#_tty VARCHAR2,
                                 measure_#_ty          VARCHAR2,
                                 measure_#_tty         VARCHAR2,
                                 measure_table_#_tty   VARCHAR2,
                                 measure_meta_#_tty    VARCHAR2) IS
        mrel_header CLOB;
        mrel_levels CLOB;
        
        mrel_trty CLOB;
        
        i INTEGER;
    BEGIN
        -- loop through the names of the dimensions
        i := dim_ids.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                mrel_levels := mrel_levels || ', ';
            END IF;
            
            mrel_levels := mrel_levels ||
                dim_ids(i) || '_level VARCHAR2';       
            
            -- increment cursor variable
            i := dim_ids.NEXT(i);
        END LOOP;
        ----
        
        mrel_header :=
            'CREATE OR REPLACE TYPE ' || mrel_#_ty || ' UNDER mrel_ty(' || chr(10) ||
            '    coordinate ' || coordinate_#_ty || ',' || chr(10) ||
            '    ' || chr(10) ||
            '    -- for each connection-level of this m-relationship store the' || chr(10) ||
            '    -- ancestor m-relationship with the corresponding top connection-level.' || chr(10) ||
            '    ancestors ' || conlvl_ancestor_#_tty || ',' || chr(10) ||
            '    ' || chr(10) ||
            '    measure_tables ' || measure_table_#_tty || ',' || chr(10) ||
            '    measure_metadata ' || measure_meta_#_tty || ',' || chr(10) ||
            '    ' || chr(10) ||
            '    CONSTRUCTOR FUNCTION ' || mrel_#_ty || '(coordinate ' || coordinate_#_ty || ', id VARCHAR2)'|| chr(10) ||
            '        RETURN SELF AS RESULT,' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER FUNCTION does_specialize RETURN BOOLEAN,' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE delete_mrel,' || chr(10) ||
            '    ' || chr(10) ||
            '    MEMBER FUNCTION calculate_ancestors RETURN ' || conlvl_ancestor_#_tty || ',' || chr(10) ||
            '    ' || chr(10) ||
            '    MEMBER FUNCTION top_level RETURN ' || conlevel_#_ty || ',' || chr(10) ||
            '    ' || chr(10) ||
            '    MEMBER PROCEDURE add_measure(measure_name     VARCHAR2,' || chr(10) ||
            '                                 measure_level    ' || conlevel_#_ty || ',' || chr(10) ||
            '                                 measure_datatype VARCHAR2),' || chr(10) ||
            '    MEMBER PROCEDURE set_measure(measure_name  VARCHAR2,' || chr(10) ||
            '                                 metalevel     VARCHAR2,' || chr(10) ||
            '                                 default_value BOOLEAN,' || chr(10) ||
            '                                 measure_value ANYDATA),' || chr(10) ||
            '    MEMBER PROCEDURE set_measure(measure_name  VARCHAR2,' || chr(10) ||
            '                                 measure_value ANYDATA),' || chr(10) ||
            '    MEMBER PROCEDURE delete_measure(measure_name VARCHAR2),' || chr(10) ||
            '    ' || chr(10) ||
            '    MEMBER FUNCTION get_measure(measure_name VARCHAR2) RETURN ANYDATA,' || chr(10) ||
            '    MEMBER FUNCTION get_measure_unit(measure_name VARCHAR2) RETURN ANYDATA,' || chr(10) ||
            '    ' || chr(10) ||
            '    MEMBER FUNCTION has_measure(measure_name    IN  VARCHAR2,' || chr(10) ||
            '                                top_level_only  IN  BOOLEAN,' || chr(10) ||
            '                                introduced_only IN  BOOLEAN,' || chr(10) ||
            '                                description     OUT ' || measure_#_ty || ') RETURN BOOLEAN,' || chr(10) ||
            '    MEMBER FUNCTION has_measure(measure_name    VARCHAR2,' || chr(10) ||
            '                                top_level_only  INTEGER,' || chr(10) ||
            '                                introduced_only INTEGER) RETURN INTEGER,' || chr(10) ||
            '    ' || chr(10) ||
            '    MEMBER FUNCTION list_measures(top_level_only  BOOLEAN,' || chr(10) ||
            '                                  introduced_only BOOLEAN) RETURN ' || measure_#_tty || ',' || chr(10) ||
            '    MEMBER FUNCTION list_measures(top_level_only  INTEGER,' || chr(10) ||
            '                                  introduced_only INTEGER) RETURN ' || measure_#_tty || ',' || chr(10) ||
            '    ' || chr(10) ||
            '    MEMBER FUNCTION get_measure_table(conlevel ' || conlevel_#_ty || ') RETURN VARCHAR2,' || chr(10) ||
            '    ' || chr(10) ||
            '    MEMBER FUNCTION has_connection_level(conlevel ' || conlevel_#_ty || ') RETURN BOOLEAN,' || chr(10) ||
            '    MEMBER FUNCTION has_connection_level(' || mrel_levels || ') RETURN INTEGER,' || chr(10) ||
            '    MEMBER PROCEDURE persist,' || chr(10) ||
            '    ' || chr(10) ||
            '    MEMBER PROCEDURE init_measure_table(table_name VARCHAR2)' || chr(10) ||
            ');';
            
        mrel_trty :=
            'CREATE OR REPLACE TYPE ' || mrel_#_trty || ' AS TABLE OF REF ' || mrel_#_ty || ';';
        
        --dbms_output.put_line(mrel_header);
        EXECUTE IMMEDIATE mrel_header;
        EXECUTE IMMEDIATE mrel_trty;
    END;
    
    
    PROCEDURE create_mrel_value_header(mrel_#_value_ty   VARCHAR2,
                                       mrel_#_value_tty  VARCHAR2,
                                       dimensions        names_tty,
                                       dim_ids           names_tty) IS
        mrel_ty_header  CLOB;
        mrel_ty_header1 CLOB;
        mrel_tty_header CLOB;
        
        i INTEGER;
    BEGIN
        
        -- loop through the names of the dimensions
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            mrel_ty_header1 := mrel_ty_header1 ||
                '    ' || dim_ids(i) || '_oname VARCHAR2(30),' || chr(10);
            
            -- increment cursor variable
            i := dimensions.NEXT(i);
        END LOOP;
        
        mrel_ty_header :=
            'CREATE TYPE ' || mrel_#_value_ty || ' AS OBJECT (' || chr(10) ||
            mrel_ty_header1 ||
            '    measure_value ANYDATA' || chr(10) ||
            ');';
        
        mrel_tty_header :=
            'CREATE TYPE ' || mrel_#_value_tty || ' AS TABLE OF ' || mrel_#_value_ty || ';';
        
        EXECUTE IMMEDIATE mrel_ty_header;
        EXECUTE IMMEDIATE mrel_tty_header;
    END;
    
    PROCEDURE create_mcube_header(cname            VARCHAR2,
                                  mcube_#_ty       VARCHAR2,
                                  mrel_#_ty        VARCHAR2,
                                  mrel_#_trty      VARCHAR2,
                                  mrel_#_value_tty VARCHAR2,
                                  conlevel_#_ty    VARCHAR2,
                                  coordinate_#_ty  VARCHAR2,
                                  coordinate_#_tty VARCHAR2,
                                  measure_#_ty     VARCHAR2,
                                  measure_#_tty    VARCHAR2,
                                  queryview_#_ty   VARCHAR2,
                                  dimensions       names_tty,
                                  dim_ids          names_tty,
                                  dimension_#_ty   names_tty,
                                  mobject_#_ty     names_tty) IS
        
        mcube_header               CLOB;
        mcube_dimensions           CLOB; -- the variables that store the references to the dimensions
        mcube_constructor          CLOB;
        mcube_constructor_alt      CLOB;
        mcube_constructor_alt1     CLOB;
        mcube_create_mrel          CLOB;
        mcube_create_mrel1         CLOB;
        mcube_create2_mrel         CLOB;
        mcube_create2_mrel1        CLOB;
        mcube_create3_mrel         CLOB;
        mcube_bulk_create_mrel     CLOB;
        mcube_bulk_create_mrel_heu  CLOB;
        mcube_bulk_create_mrel_heu1 CLOB;
        mcube_get_mrel_ref         CLOB;
        mcube_get2_mrel_ref        CLOB;
        mcube_get2_mrel_ref1       CLOB;
        mcube_get3_mrel_ref        CLOB;
        mcube_get3_mrel_ref1       CLOB;
        mcube_get_measure_descr    CLOB;
        mcube_get_measure_descr1   CLOB;
        mcube_get_measure_descrs   CLOB;
        mcube_get_measure_unit     CLOB;
        mcube_get_measure_unit1    CLOB;
        mcube_get_measure_funct    CLOB;
        mcube_bulk_set_measure     CLOB;
        mcube_bulk_set_measure_heu CLOB;
        mcube_bulk_set_measure_heu1 CLOB;
        mcube_refresh_meas_unit_cache CLOB;
        mcube_new_queryview        CLOB;
        mcube_new_queryview2       CLOB;
        mcube_persist              CLOB;
        mcube_get_dimension_names  CLOB;
        mcube_get_dimension_ids    CLOB;
        mcube_export_star          CLOB;
        mcube_export_star1         CLOB;
        mcube_rollup               CLOB;
        mcube_rollup1              CLOB;
        mcube_rollup_unit_aware    CLOB;
        mcube_get_nearest_mrel     CLOB;
        mcube_get_nearest_mrel1    CLOB;
        
        queryview_forward VARCHAR2(100);
        
        -- cursor variables
        i INTEGER;
    BEGIN
        -- loop through the names of the dimensions
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                mcube_constructor_alt1 := mcube_constructor_alt1 || ', ';
                mcube_create_mrel1 := mcube_create_mrel1 || ', ';
                mcube_create2_mrel1 := mcube_create2_mrel1 || ', ';
                mcube_get2_mrel_ref1 := mcube_get2_mrel_ref1 || ', ';
                mcube_get3_mrel_ref1 := mcube_get3_mrel_ref1 || ', ';
                mcube_rollup1 := mcube_rollup1 || ', ';
                mcube_get_measure_descr1 := mcube_get_measure_descr1 || ', ';
                mcube_get_measure_unit1 := mcube_get_measure_unit1 || ', ';
                mcube_bulk_set_measure_heu1 := mcube_bulk_set_measure_heu1 || ', ';
                mcube_bulk_create_mrel_heu1 := mcube_bulk_create_mrel_heu1 || ', ';
                mcube_export_star1 := mcube_export_star1 || ', ';
                mcube_get_nearest_mrel1 := mcube_get_nearest_mrel1 || ', ';
            END IF;
            
            mcube_constructor_alt1 := mcube_constructor_alt1 ||
                dimensions(i) || ' REF ' || dimension_#_ty(i);
            
            mcube_dimensions := mcube_dimensions ||
                '    ' || dimensions(i) || ' REF ' || dimension_#_ty(i) || ',' || chr(10);
            
            mcube_create_mrel1 := mcube_create_mrel1 ||
                dim_ids(i) || '_obj REF ' || mobject_#_ty(i);
            
            mcube_create2_mrel1 := mcube_create2_mrel1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            mcube_get2_mrel_ref1 := mcube_get2_mrel_ref1 ||
                dim_ids(i) || '_obj REF ' || mobject_#_ty(i);
            
            mcube_get3_mrel_ref1 := mcube_get3_mrel_ref1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            mcube_rollup1 := mcube_rollup1 ||
                dim_ids(i) || '_level VARCHAR2';
                        
            mcube_get_measure_descr1 := mcube_get_measure_descr1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            mcube_get_measure_unit1 := mcube_get_measure_unit1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            mcube_bulk_set_measure_heu1 := mcube_bulk_set_measure_heu1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            mcube_bulk_create_mrel_heu1 := mcube_bulk_create_mrel_heu1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            mcube_export_star1 := mcube_export_star1 ||
                dim_ids(i) || '_star_table VARCHAR2';
            
            mcube_get_nearest_mrel1 := mcube_get_nearest_mrel1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            -- increment cursor variable
            i := dimensions.NEXT(i);
        END LOOP;
        ----
        
        mcube_constructor :=
            '    CONSTRUCTOR FUNCTION ' || mcube_#_ty || '(cname VARCHAR2, id VARCHAR2, root_coordinate ' || coordinate_#_ty || ')' || chr(10) ||
            '        RETURN SELF AS RESULT,' || chr(10);
        
        mcube_constructor_alt :=
            '    CONSTRUCTOR FUNCTION ' || mcube_#_ty || '(cname VARCHAR2, id VARCHAR2, ' || mcube_constructor_alt1 || ')' || chr(10) ||
            '        RETURN SELF AS RESULT,' || chr(10);
            
        mcube_create_mrel :=
            '    MEMBER FUNCTION create_mrel(' || mcube_create_mrel1 || ') RETURN REF ' 
            || mrel_#_ty || ',' || chr(10);
        
        mcube_create2_mrel :=
            '    MEMBER FUNCTION create_mrel(' || mcube_create2_mrel1 || ') RETURN REF ' 
            || mrel_#_ty || ',' || chr(10);
        
        
        mcube_create3_mrel :=
            '    MEMBER FUNCTION create_mrel(coordinate ' || coordinate_#_ty || ') RETURN REF '
            || mrel_#_ty || ',' || chr(10);
        
        mcube_bulk_create_mrel :=
            '    MEMBER PROCEDURE bulk_create_mrel(coordinates ' || coordinate_#_tty || '),' || chr(10);
        
        mcube_bulk_create_mrel_heu :=
            '    MEMBER PROCEDURE bulk_create_mrel(coordinates ' || coordinate_#_tty || ', parents ' || mrel_#_trty || '),' || chr(10);
        
        mcube_get_mrel_ref :=
            '    MEMBER FUNCTION get_mrel_ref(coordinate ' || coordinate_#_ty || ') RETURN REF ' || mrel_#_ty || ',' || chr(10);
        
        mcube_get2_mrel_ref :=
            '    MEMBER FUNCTION get_mrel_ref(' || mcube_get2_mrel_ref1 || ') RETURN REF ' || mrel_#_ty || ',' || chr(10);
        
        mcube_get3_mrel_ref :=
            '    MEMBER FUNCTION get_mrel_ref(' || mcube_get3_mrel_ref1 || ') RETURN REF ' || mrel_#_ty || ',' || chr(10);
        
        mcube_get_measure_descr :=
            '    MEMBER FUNCTION get_measure_description(measure_name VARCHAR2, ' || mcube_get_measure_descr1 || ') RETURN ' || measure_#_ty || ',' || chr(10);
        
        mcube_get_measure_descrs :=
            '    MEMBER FUNCTION get_measure_descriptions(measure_name VARCHAR2) RETURN ' || measure_#_tty || ',' || chr(10);
        
        mcube_get_measure_unit :=
            '    MEMBER FUNCTION get_measure_unit(measure_name VARCHAR2, ' || mcube_get_measure_unit1 ||  ') RETURN ANYDATA,' || chr(10);
        
        mcube_get_measure_funct :=
            '    MEMBER FUNCTION get_measure_function(measure_name VARCHAR2) RETURN ANYDATA,' || chr(10);
        
        mcube_bulk_set_measure :=
            '    MEMBER PROCEDURE bulk_set_measure(measure_name VARCHAR2, measure_values ' || mrel_#_value_tty || '),' || chr(10);
        
        mcube_bulk_set_measure_heu :=
            '    MEMBER PROCEDURE bulk_set_measure(measure_name VARCHAR2, measure_values ' || mrel_#_value_tty || ', ' || mcube_bulk_set_measure_heu1 || '),' || chr(10);
        
        mcube_refresh_meas_unit_cache :=
            '    MEMBER PROCEDURE refresh_measure_unit_cache(measure_name VARCHAR2, mrel_refs ' || mrel_#_trty || '),' || chr(10);
        
        mcube_new_queryview :=
            '    MEMBER FUNCTION new_queryview RETURN ' || queryview_#_ty || ',' || chr(10);
        
        mcube_new_queryview2 :=
            '    --MEMBER FUNCTION new_queryview(qname VARCHAR2) RETURN REF ' || queryview_#_ty || ',' || chr(10);
            
        mcube_persist :=
            '    OVERRIDING MEMBER PROCEDURE persist,' || chr(10);
        
        mcube_get_dimension_names :=
            '    OVERRIDING MEMBER FUNCTION get_dimension_names RETURN names_tty,' || chr(10);
        
        mcube_get_dimension_ids :=
            '    OVERRIDING MEMBER FUNCTION get_dimension_ids RETURN names_tty,' || chr(10);
        
        mcube_export_star :=
            '    MEMBER PROCEDURE export_star(table_name VARCHAR2, ' || mcube_export_star1 || '),' || chr(10);
        
        mcube_rollup :=
            '    MEMBER PROCEDURE rollup(table_name VARCHAR2, include_non_dimension_attr BOOLEAN, ' || mcube_rollup1 || '),' || chr(10);
        
        mcube_rollup_unit_aware :=
            '    MEMBER PROCEDURE rollup(table_name VARCHAR2, include_non_dimension_attr BOOLEAN, measure_units measure_unit_tty, ' || mcube_rollup1 || '),' || chr(10);
        
        mcube_get_nearest_mrel :=
            '    MEMBER FUNCTION get_nearest_mrel(' || mcube_get_nearest_mrel1 || ') RETURN ' || mrel_#_trty || chr(10);
        
        -- assemble the parts of the declaration
        mcube_header :=
            'CREATE OR REPLACE TYPE ' || mcube_#_ty || ' UNDER mcube_ty(' || chr(10) ||
            '    root_coordinate ' || coordinate_#_ty || ',' || chr(10) ||
            '    ' || chr(10) ||
                 mcube_dimensions ||
            '    ' || chr(10) ||
                 mcube_constructor ||
                 mcube_constructor_alt ||
                 mcube_create_mrel ||
                 mcube_create2_mrel ||
                 mcube_create3_mrel ||
                 mcube_get_measure_descr ||
                 mcube_get_measure_descrs ||
                 mcube_get_measure_unit ||
                 mcube_get_measure_funct ||
                 mcube_bulk_create_mrel ||
                 mcube_bulk_create_mrel_heu ||
                 mcube_get_mrel_ref ||
                 mcube_get2_mrel_ref ||
                 mcube_get3_mrel_ref ||
                 mcube_bulk_set_measure ||
                 mcube_bulk_set_measure_heu ||
                 mcube_refresh_meas_unit_cache ||
                 mcube_new_queryview ||
                 mcube_new_queryview2 ||
                 mcube_persist ||
                 mcube_get_dimension_names ||
                 mcube_get_dimension_ids ||
                 mcube_export_star ||
                 mcube_rollup ||
                 mcube_rollup_unit_aware ||
                 mcube_get_nearest_mrel ||
            ');';
        
        --dbms_output.put_line(mcube_header);
        EXECUTE IMMEDIATE mcube_header;
    END;
    
    PROCEDURE create_expressions_header(expr_#_ty         VARCHAR2,
                                        expr_#_tty        VARCHAR2,
                                        dice_expr_#_ty    VARCHAR2,
                                        slice_expr_#_ty   VARCHAR2,
                                        project_expr_#_ty VARCHAR2,
                                        queryview_#_ty    VARCHAR2,
                                        coordinate_#_ty   VARCHAR2,
                                        dimensions        names_tty,
                                        dim_ids   names_tty) IS
        expr_ty  CLOB;
        expr_tty CLOB;
        dice_expr_ty CLOB;
        slice_expr_ty CLOB;
        slice_expr_ty_predicates CLOB;
        slice_expr_ty_constructor CLOB;
        project_expr_ty CLOB;
        
        queryview_forward CLOB;
        
        i INTEGER;
    BEGIN
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                slice_expr_ty_constructor := slice_expr_ty_constructor || ', ';
            END IF;
            
            slice_expr_ty_predicates := slice_expr_ty_predicates ||
                '    ' || dim_ids(i) || '_predicate slice_predicate_ty,' || chr(10);
            
            slice_expr_ty_constructor := slice_expr_ty_constructor ||
                dim_ids(i) || '_predicate slice_predicate_ty';
            
            i := dimensions.NEXT(i);
        END LOOP;
        
        expr_ty := 
            'CREATE OR REPLACE TYPE ' || expr_#_ty || ' AS OBJECT (' || chr(10) ||
            '    id VARCHAR2(30)' || chr(10) ||
            ') NOT FINAL NOT INSTANTIABLE;';
        
        expr_tty :=
            'CREATE OR REPLACE TYPE ' || expr_#_tty || ' AS TABLE OF ' || expr_#_ty || ';';
        
        dice_expr_ty := 
            'CREATE OR REPLACE TYPE ' || dice_expr_#_ty || ' UNDER ' || expr_#_ty || '(' || chr(10) ||
            '    dice_coordinate ' || coordinate_#_ty || ',' || chr(10) ||
            '    CONSTRUCTOR FUNCTION ' || dice_expr_#_ty || '(dice_coordinate ' || coordinate_#_ty || ') RETURN SELF AS RESULT' || chr(10) ||
            ');';
        
        slice_expr_ty := 
            '/**' || chr(10) ||
            ' * For each dimension of the m-cube, the slice expression holds a' || chr(10) ||
            ' * slice predicate.' || chr(10) ||
            ' */' || chr(10) ||
            'CREATE OR REPLACE TYPE ' || slice_expr_#_ty || ' UNDER ' || expr_#_ty || '(' || chr(10) ||
                 slice_expr_ty_predicates ||
            '    CONSTRUCTOR FUNCTION ' || slice_expr_#_ty || '(' || slice_expr_ty_constructor || ') RETURN SELF AS RESULT' || chr(10) ||
            ');';
        
        project_expr_ty :=
            'CREATE OR REPLACE TYPE ' || project_expr_#_ty || ' UNDER ' || expr_#_ty || '(' || chr(10) ||
            '    measure_set names_tty,' || chr(10) ||
            '    CONSTRUCTOR FUNCtION ' || project_expr_#_ty || ' (measure_set names_tty) RETURN SELF AS RESULT' || chr(10) ||
            ');';
        
        EXECUTE IMMEDIATE expr_ty;
        EXECUTE IMMEDIATE expr_tty;
        EXECUTE IMMEDIATE dice_expr_ty;
        EXECUTE IMMEDIATE slice_expr_ty;
        EXECUTE IMMEDIATE project_expr_ty;
    END;
    
    PROCEDURE create_queryview_header(mcube_#_ty        VARCHAR2,
                                      mrel_#_trty       VARCHAR2,
                                      conlevel_#_ty     VARCHAR2,
                                      coordinate_#_ty   VARCHAR2,
                                      queryview_#_ty    VARCHAR2,
                                      expr_#_tty        VARCHAR2,
                                      dice_expr_#_ty    VARCHAR2,
                                      slice_expr_#_ty   VARCHAR2,
                                      project_expr_#_ty VARCHAR2,
                                      dimensions      names_tty,
                                      dim_ids names_tty) IS
        queryview_header  CLOB;
        queryview_header1 CLOB;
        queryview_header2 CLOB;
        queryview_rollup1 CLOB;
        
        mcube_forward CLOB;
        
        i INTEGER;
    BEGIN
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                queryview_header1 := queryview_header1 || ', ';
                queryview_header2 := queryview_header2 || ', ';
                queryview_rollup1 := queryview_rollup1 || ', ';
            END IF;
            
            queryview_header1 := queryview_header1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            queryview_header2 := queryview_header2 ||
                dim_ids(i) || '_predicate slice_predicate_ty';
            
            queryview_rollup1 := queryview_rollup1 ||
                dim_ids(i) || '_level VARCHAR2';
                
            i := dimensions.NEXT(i);
        END LOOP;
        
        queryview_header :=
            'CREATE OR REPLACE TYPE ' || queryview_#_ty || ' AS OBJECT (' || chr(10) ||
            '    mcube REF ' || mcube_#_ty || ',' || chr(10) ||
            '    root_coordinate ' || coordinate_#_ty || ',' || chr(10) ||
            '    mrelationship_set ' || mrel_#_trty || ',' || chr(10) ||
            '    measure_set names_tty,' || chr(10) ||
            '    expressions ' || expr_#_tty || ',' || chr(10) ||
            '    ' || chr(10) ||
            '    CONSTRUCTOR FUNCTION ' || queryview_#_ty || '(mcube REF ' || mcube_#_ty || ') RETURN SELF AS RESULT,' || chr(10) ||
            '    ' || chr(10) ||
            '    -- the following functions append expressions to the expression list.' || chr(10) ||
            '    -- the list of measures and the list of m-relationships are not changed.' || chr(10) ||
            '    MEMBER FUNCTION dice(SELF IN OUT ' || queryview_#_ty || ', ' || queryview_header1 || ') RETURN ' || queryview_#_ty || ',' || chr(10) ||
            '    --MEMBER FUNCTION dice(SELF IN OUT ' || queryview_#_ty || ', coordinate ' || coordinate_#_ty || ') RETURN ' || queryview_#_ty || ',' || chr(10) ||
            '    --MEMBER FUNCTION dice(SELF IN OUT ' || queryview_#_ty || ', expr ' || dice_expr_#_ty || ') RETURN ' || queryview_#_ty || ',' || chr(10) ||
            '    ' || chr(10) ||
            '    MEMBER FUNCTION slice(SELF IN OUT ' || queryview_#_ty || ', ' || queryview_header2 || ') RETURN ' || queryview_#_ty || ',' || chr(10) ||
            '    --MEMBER FUNCTION slice(SELF IN OUT ' || queryview_#_ty || ', expr ' || slice_expr_#_ty || ') RETURN ' || queryview_#_ty || ',' || chr(10) ||
            '    ' || CHR(10) ||
            '    MEMBER FUNCTION project(SELF IN OUT ' || queryview_#_ty || ', measure_set names_tty) RETURN ' || queryview_#_ty || ',' || chr(10) ||
            '    --MEMBER FUNCTION project(SELF IN OUT ' || queryview_#_ty || ', expr ' || project_expr_#_ty || ') RETURN ' || queryview_#_ty || ',' || chr(10) ||
            '    ' || chr(10) ||
            '    -- the evaluate function actually changes the sets of measures and m-relationships' || chr(10) ||
            '    MEMBER FUNCTION evaluate(SELF IN OUT ' || queryview_#_ty || ') RETURN ' || queryview_#_ty  || ',' || chr(10) ||
            '    MEMBER FUNCTION evaluate(SELF IN OUT ' || queryview_#_ty || ', dice_expression ' || dice_expr_#_ty || ') RETURN ' || queryview_#_ty  || ',' || chr(10) ||
            '    MEMBER FUNCTION evaluate(SELF IN OUT ' || queryview_#_ty || ', slice_expression ' || slice_expr_#_ty || ') RETURN ' || queryview_#_ty  || ',' || chr(10) ||
            '    MEMBER FUNCTION evaluate(SELF IN OUT ' || queryview_#_ty || ', project_expression ' || project_expr_#_ty || ') RETURN ' || queryview_#_ty  || ',' || chr(10) ||
            '    ' || chr(10) ||
            '    -- export functions, fact and cube extraction' || chr(10) ||
            '    MEMBER PROCEDURE rollup(table_name VARCHAR2, include_non_dimension_attr BOOLEAN, ' || queryview_rollup1 || '),' || chr(10) ||
            '    MEMBER PROCEDURE rollup(table_name VARCHAR2, include_non_dimension_attr BOOLEAN, measure_units measure_unit_tty, ' || queryview_rollup1 || ')' || chr(10) ||
            '    ' || chr(10) ||
            ');';
            
        mcube_forward :=
            'CREATE OR REPLACE TYPE ' || mcube_#_ty || ';';
        
        EXECUTE IMMEDIATE mcube_forward;
        EXECUTE IMMEDIATE queryview_header;
    END;    
    /*** ***/
    
    
    /*** BODIES ***/
    PROCEDURE create_coordinate_body(coordinate_#_ty VARCHAR2,
                                     dimensions      names_tty,
                                     dim_ids names_tty,
                                     mobject_#_ty    names_tty,
                                     mobject_tables  names_tty) IS
    
        coordinate_body CLOB;
        
        coordinate_constructor  CLOB;
        coordinate_constructor1 CLOB;
        coordinate_constructor2 CLOB;
        
        coordinate_constructor_alt  CLOB;
        coordinate_constructor_alt1 CLOB;
        coordinate_constructor_alt2 CLOB;
        
        coordinate_constructor2_alt  CLOB;
        coordinate_constructor2_alt1 CLOB;
        coordinate_constructor2_alt2 CLOB;
        
        coordinate_equals  CLOB;
        coordinate_equals1 CLOB;
        
        coordinate_tostring  CLOB;
        coordinate_tostring1 CLOB;
        
        coordinate_compare CLOB;
        
        coordinate_is_sub  CLOB;
        coordinate_is_sub1 CLOB;
        coordinate_is_sub_equ CLOB;
        
        i INTEGER;
    BEGIN        
        -- loop through the names of the dimensions
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                coordinate_constructor1 := coordinate_constructor1 || ', ';
                coordinate_constructor_alt1 := coordinate_constructor_alt1 || ', ';
                coordinate_constructor2_alt1 := coordinate_constructor2_alt1 || ', ';
                coordinate_is_sub_equ := coordinate_is_sub_equ || ' AND ';
                coordinate_equals1 := coordinate_equals1 || ' AND ';
            END IF;
            
            coordinate_constructor1 := coordinate_constructor1 ||
                dim_ids(i) || '_obj REF ' || mobject_#_ty(i);
            
            coordinate_constructor2 := coordinate_constructor2 ||
                '        -- store the reference to the m-object' || chr(10) ||
                '        SELF.' || dim_ids(i) || '_obj := ' || dim_ids(i) || '_obj;' || chr(10) ||
                '        utl_ref.select_object(TREAT (' || dim_ids(i) || '_obj AS REF mobject_ty), obj);' || chr(10) ||
                '        -- get the name of the m-object' || chr(10) ||
                '        SELF.' || dim_ids(i) || '_oname := obj.oname;' || chr(10) ||
                '        ' || chr(10);
            
            coordinate_constructor_alt1 := coordinate_constructor_alt1 ||
                dim_ids(i) || '_obj REF ' || mobject_#_ty(i) || ', ' || 
                dim_ids(i) || '_oname VARCHAR2';
            
            coordinate_constructor_alt2 := coordinate_constructor_alt2 ||
                '        -- store the reference to the m-object' || chr(10) ||
                '        SELF.' || dim_ids(i) || '_obj := ' || dim_ids(i) || '_obj;' || chr(10) ||
                '        -- store the name of the m-object' || chr(10) ||
                '        SELF.' || dim_ids(i) || '_oname := ' || dim_ids(i) || '_oname;' || chr(10) ||
                '        ' || chr(10);
            
            coordinate_constructor2_alt1 := coordinate_constructor2_alt1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            coordinate_constructor2_alt2 := coordinate_constructor2_alt2 ||
                '        SELECT REF(o), o.oname INTO SELF.' || dim_ids(i) || '_obj, SELF.' || dim_ids(i) || '_oname' || chr(10) || 
                '        FROM   ' || mobject_tables(i) || ' o' || chr(10) ||
                '        WHERE  o.oname = ' || dim_ids(i) || '_oname;' || chr(10) ||
                '        ' || chr(10);
            
            coordinate_equals1 := coordinate_equals1 ||
                'SELF.' || dim_ids(i) || '_oname = other.' || dim_ids(i) || '_oname';
            
            coordinate_tostring1 := coordinate_tostring1 ||
                '        string_representation := string_representation ||' || chr(10) ||
                '            SUBSTR(' || dim_ids(i) || '_oname, 1, 2);' || chr(10) ||
                '        ' || chr(10);
            
            coordinate_is_sub1 := coordinate_is_sub1 ||
                '        -- only continue if the last query had a match' || chr(10) ||
                '        IF cnt > 0 THEN' || chr(10) ||
                '            SELECT COUNT(*) INTO cnt' || chr(10) ||
                '            FROM   ' || mobject_tables(i) || ' o' || chr(10) ||
                '            WHERE  o.oname = SELF.' || dim_ids(i) || '_oname AND' || chr(10) ||
                '                   other.' || dim_ids(i) || '_obj IN (SELECT a.ancestor FROM TABLE(o.ancestors) a);' || chr(10) ||
                '            ' || chr(10) ||
                '            IF cnt <= 0 AND SELF.' || dim_ids(i) || '_oname = other.' || dim_ids(i) || '_oname THEN' || chr(10) ||
                '                cnt := 1;' || chr(10) ||
                '            END IF;' || chr(10) ||
                '        END IF;' || chr(10) ||
                '        ' || chr(10);
            
            coordinate_is_sub_equ := coordinate_is_sub_equ ||
                'SELF.' || dim_ids(i) || '_oname = other.'  || dim_ids(i) || '_oname';
            
            -- increment cursor variable
            i := dimensions.NEXT(i);
        END LOOP;
        ----
        
        coordinate_constructor :=
            '    CONSTRUCTOR FUNCTION ' || coordinate_#_ty || '(' || coordinate_constructor1 || ') RETURN SELF AS RESULT IS' || chr(10) ||
            '        obj mobject_ty;' || chr(10) ||
            '    BEGIN' || chr(10) ||
                     coordinate_constructor2 ||
            '        RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
            
        coordinate_constructor_alt :=
            '    CONSTRUCTOR FUNCTION ' || coordinate_#_ty || '(' || coordinate_constructor_alt1 || ') RETURN SELF AS RESULT IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
                     coordinate_constructor_alt2 ||
            '        RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
            
        coordinate_constructor2_alt :=
            '    CONSTRUCTOR FUNCTION ' || coordinate_#_ty || '(' || coordinate_constructor2_alt1 || ') RETURN SELF AS RESULT IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
                     coordinate_constructor2_alt2 ||
            '        RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        coordinate_tostring :=
            '    MEMBER FUNCTION to_string RETURN VARCHAR2 IS' || chr(10) ||
            '        string_representation VARCHAR2(100);' || chr(10) ||
            '    BEGIN' || chr(10) ||
                     coordinate_tostring1 ||
            '        RETURN string_representation;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        coordinate_equals :=
            '    MEMBER FUNCTION equals(other ' || coordinate_#_ty || ') RETURN INTEGER IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        IF ' || coordinate_equals1 || ' THEN' || chr(10) ||
            '            RETURN 1;' || chr(10) ||
            '        ELSE' || chr(10) ||
            '            RETURN 0;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN 0;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        coordinate_compare :=
            '    ORDER MEMBER FUNCTION compare_to(other ' || coordinate_#_ty || ') RETURN INTEGER IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        IF SELF.is_sub_coordinate_of(other) THEN' || chr(10) ||
            '            RETURN -1;' || chr(10) ||
            '        ELSIF other.is_sub_coordinate_of(SELF) THEN' || chr(10) ||
            '            RETURN 1;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN 0;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        coordinate_is_sub :=
            '    MEMBER FUNCTION is_sub_coordinate_of(other ' || coordinate_#_ty || ') RETURN BOOLEAN IS' || chr(10) ||
            '        cnt INTEGER := 1;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        IF ' || coordinate_is_sub_equ || ' THEN' || chr(10) ||
            '            RETURN FALSE;'  || chr(10) ||
            '        END IF;' || chr(10) ||
            '        '  || chr(10) ||
                     coordinate_is_sub1 ||
            '        RETURN cnt > 0;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        coordinate_body :=
            'CREATE OR REPLACE TYPE BODY ' || coordinate_#_ty || ' IS ' || chr(10) ||
                 coordinate_constructor ||
                 coordinate_constructor_alt ||
                 coordinate_constructor2_alt ||
                 coordinate_equals ||
                 coordinate_tostring ||
                 coordinate_compare ||
                 coordinate_is_sub ||
            'END;';
        
        --dbms_output.put_line(coordinate_body);
        EXECUTE IMMEDIATE coordinate_body;
    END;
    
    PROCEDURE create_conlevel_body(conlevel_#_ty   VARCHAR2,
                                   dimensions      names_tty,
                                   dim_ids names_tty) IS
        conlevel_body      CLOB;
        conlevel_compare   CLOB;
        conlevel_order_self1 CLOB;
        conlevel_order_self2 CLOB;
        conlevel_order_self3 CLOB;
        conlevel_order_other1 CLOB;
        conlevel_order_other2 CLOB;
        conlevel_order_other3 CLOB;
        conlevel_equals    CLOB;
        conlevel_equals1   CLOB;
        conlevel_tostring  CLOB;
        conlevel_tostring1 CLOB;
        conlevel_2tostring  CLOB;
        conlevel_2tostring1 CLOB;
        
        i INTEGER;
    BEGIN
        -- loop through the names of the dimensions
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                conlevel_equals1 := conlevel_equals1 || ' AND ';
                conlevel_order_self1 := conlevel_order_self1 || ' + ';
                conlevel_order_self2 := conlevel_order_self2 || ', ' || chr(10);
                conlevel_order_self3 := conlevel_order_self3 || ' AND ' || chr(10);
                conlevel_order_other1 := conlevel_order_other1 || ' + ';
                conlevel_order_other2 := conlevel_order_other2 || ', ' || chr(10);
                conlevel_order_other3 := conlevel_order_other3 || ' AND ' || chr(10);
                conlevel_2tostring1 := conlevel_2tostring1 || ' ';
            END IF;
            
            conlevel_order_self1 := conlevel_order_self1 ||
                dim_ids(i) || '_level.position';
            
            conlevel_order_self2 := conlevel_order_self2 || chr(10) ||
                '                   TABLE(SELECT d.level_positions' || chr(10) ||
                '                         FROM   dimensions d' || chr(10) ||
                '                         WHERE  d.dname = ''' || dimensions(i) || ''') ' || dim_ids(i) || '_level';
            
            conlevel_order_self3 := conlevel_order_self3 || chr(10) ||
                '                  SELF.' || dim_ids(i) || '_level = ' || dim_ids(i) || '_level.lvl';
                
            conlevel_order_other1 := conlevel_order_other1 ||
                dim_ids(i) || '_level.position';
            
            conlevel_order_other2 := conlevel_order_other2 || chr(10) ||
                '                   TABLE(SELECT d.level_positions' || chr(10) ||
                '                         FROM   dimensions d' || chr(10) ||
                '                         WHERE  d.dname = ''' || dimensions(i) || ''') ' || dim_ids(i) || '_level';
            
            conlevel_order_other3 := conlevel_order_other3 || chr(10) ||
                '                  other.' || dim_ids(i) || '_level = ' || dim_ids(i) || '_level.lvl';
                
            conlevel_equals1 := conlevel_equals1 ||
                'SELF.' || dim_ids(i) || '_level = other.'
                        || dim_ids(i) || '_level';
            
            conlevel_tostring1 := conlevel_tostring1 ||
                '        string_representation := string_representation ||' || chr(10) ||
                '            SUBSTR(' || dim_ids(i) || '_level, 1, 2);' || chr(10) ||
                '        ' || chr(10);
            
            conlevel_2tostring1 := conlevel_2tostring1 ||
                '        string_representation := string_representation ||' || chr(10) ||
                '            ' || dim_ids(i) || '_level;' || chr(10) ||
                '        ' || chr(10);
            
            -- increment cursor variable
            i := dimensions.NEXT(i);
        END LOOP;
        ----
        
        conlevel_compare :=
            '    /***' || chr(10) ||
            '     * This function returns an integer less than 0 if SELF is less' || chr(10) ||
            '     * finely grained than the other connection level.' || chr(10) ||
            '     * It returns an integer greater than 0 if SELF is more finely' || chr(10) ||
            '     * grained than the other connection level.' || chr(10) ||
            '     * If they are in no direct order, the function returns 0.' || chr(10) ||
            '     ***/' || chr(10) ||
            '    ORDER MEMBER FUNCTION compare_to(other ' || conlevel_#_ty || ') RETURN INTEGER IS' || chr(10) ||
            '        self_position INTEGER;' || chr(10) ||
            '        other_position INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        SELECT ' || conlevel_order_self1 || ' INTO self_position' || chr(10) ||
            '        FROM   ' || conlevel_order_self2 || chr(10) ||
            '        WHERE  ' || conlevel_order_self3 || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        SELECT ' || conlevel_order_other1 || ' INTO other_position' || chr(10) ||
            '        FROM   ' || conlevel_order_other2 || chr(10) ||
            '        WHERE  ' || conlevel_order_other3 || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN self_position - other_position;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        conlevel_equals :=
            '    MEMBER FUNCTION equals(other ' || conlevel_#_ty || ') RETURN INTEGER IS' || chr(10) ||
            '    ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        IF ' || conlevel_equals1 || ' THEN' || chr(10) ||
            '            RETURN 1;' || chr(10) ||
            '        ELSE' || chr(10) ||
            '            RETURN 0;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        conlevel_tostring :=
            '    MEMBER FUNCTION to_string RETURN VARCHAR2 IS' || chr(10) ||
            '        string_representation VARCHAR2(' || dimensions.COUNT * 2 || ');' || chr(10) ||
            '    BEGIN' || chr(10) ||
            conlevel_tostring1 ||
            '        RETURN string_representation;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        conlevel_2tostring :=
            '    MEMBER FUNCTION to_string2 RETURN VARCHAR2 IS' || chr(10) ||
            '        string_representation VARCHAR2(' || dimensions.COUNT * 30 || ');' || chr(10) ||
            '    BEGIN' || chr(10) ||
            conlevel_2tostring1 ||
            '        RETURN string_representation;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
            
        conlevel_body :=
            'CREATE OR REPLACE TYPE BODY ' || conlevel_#_ty || ' IS' || chr(10) ||
                 conlevel_compare ||
                 conlevel_equals ||
                 conlevel_tostring ||
                 conlevel_2tostring ||
            'END;';
            
        EXECUTE IMMEDIATE conlevel_body;
        --dbms_output.put_line(conlevel_header);
    END;
    
    PROCEDURE create_measure_collect_body(measure_#_collections VARCHAR2,
                                          measure_#_ty         VARCHAR2,
                                          measure_#_tty        VARCHAR2,
                                          conlevel_#_ty        VARCHAR2) IS
        measure_collection CLOB;
        measure_collect_get_measure  CLOB;
        measure_collect_get_measures CLOB;
    BEGIN
        measure_collect_get_measure :=
            '    FUNCTION get_measure_by_table(measure_name VARCHAR2, ' || chr(10) ||
            '                                  table_name   VARCHAR2,' || chr(10) ||
            '                                  conlevel     ' || conlevel_#_ty || ') RETURN ' || measure_#_ty || ' IS' || chr(10) ||
            '        ' || chr(10) ||
            '        table_name1 VARCHAR2(30) := table_name;' || chr(10) ||
            '        ' || chr(10) ||
            '        CURSOR utc_cursor IS ' || chr(10) ||
            '            SELECT utc.data_type, utc.data_length, utc.data_scale' || chr(10) || 
            '            FROM   user_tab_columns utc ' || chr(10) ||
            '            WHERE  utc.table_name = UPPER(table_name1) AND' || chr(10) ||
            '                   utc.column_name = UPPER(measure_name);' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_descr ' || measure_#_ty || ';' || chr(10) ||
            '         ' || chr(10) ||
            '        data_type VARCHAR2(30); ' || chr(10) ||
            '        data_length INTEGER; ' || chr(10) ||
            '        data_scale INTEGER;' || chr(10) ||
            '    BEGIN                  ' || chr(10) ||
            '        OPEN utc_cursor; ' || chr(10) ||
            '            ' || chr(10) ||
            '        FETCH utc_cursor INTO data_type, ' || chr(10) ||
            '                              data_length,' || chr(10) ||
            '                              data_scale; ' || chr(10) ||
            '        ' || chr(10) ||
            '        IF utc_cursor%FOUND THEN' || chr(10) ||
            '            measure_descr := ' || measure_#_ty || '(UPPER(measure_name),' || chr(10) || 
            '                                            conlevel, ' || chr(10) ||
            '                                            UPPER(table_name), ' || chr(10) ||
            '                                            data_type, ' || chr(10) ||
            '                                            data_length, ' || chr(10) ||
            '                                            data_scale);' || chr(10) ||
            '        END IF; ' || chr(10) ||
            '        ' || chr(10) ||
            '        CLOSE utc_cursor;' || chr(10) ||
            '                 ' || chr(10) ||
            '        RETURN measure_descr;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10);
        
        measure_collection :=
            'CREATE OR REPLACE PACKAGE BODY ' || measure_#_collections || ' IS' || chr(10) ||
                 measure_collect_get_measure ||
            'END;';
        
        EXECUTE IMMEDIATE measure_collection;
    END;
    
    
    
    PROCEDURE create_expressions_body(expr_#_ty         VARCHAR2,
                                      expr_#_tty        VARCHAR2,
                                      dice_expr_#_ty    VARCHAR2,
                                      slice_expr_#_ty   VARCHAR2,
                                      project_expr_#_ty VARCHAR2,
                                      queryview_#_ty    VARCHAR2,
                                      coordinate_#_ty   VARCHAR2,
                                      dimensions        names_tty,
                                      dim_ids   names_tty) IS
        dice_expr_body CLOB;
        
        slice_expr_body CLOB;
        slice_expr_constructor1 CLOB;
        slice_expr_constructor2 CLOB;
        
        project_expr_body CLOB;
         
        i INTEGER;
    BEGIN
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                slice_expr_constructor1 := slice_expr_constructor1 || ', ';
            END IF;
            
            slice_expr_constructor1 := slice_expr_constructor1 ||
                dim_ids(i) || '_predicate slice_predicate_ty';
            
            slice_expr_constructor2 := slice_expr_constructor2 ||
                '        SELF.' || dim_ids(i) || '_predicate := ' ||
                         dim_ids(i) || '_predicate;' || chr(10);
            
            i := dimensions.NEXT(i);
        END LOOP;
        
        dice_expr_body := 
            'CREATE OR REPLACE TYPE BODY ' || dice_expr_#_ty || ' IS' || chr(10) ||
            '    CONSTRUCTOR FUNCTION ' || dice_expr_#_ty ||'(dice_coordinate ' || coordinate_#_ty || ') RETURN SELF AS RESULT IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        SELF.dice_coordinate := dice_coordinate;' || chr(10) ||
            '        SELF.id := ''DICE'';' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            'END;';
        
        slice_expr_body := 
            'CREATE OR REPLACE TYPE BODY ' || slice_expr_#_ty || ' IS' || chr(10) ||
            '    CONSTRUCTOR FUNCTION ' || slice_expr_#_ty || '(' || slice_expr_constructor1 || ') RETURN SELF AS RESULT IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
                     slice_expr_constructor2 ||
            '        ' || chr(10) ||
            '        SELF.id := ''SLICE'';' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            'END;';
        
        project_expr_body :=
            'CREATE OR REPLACE TYPE BODY ' || project_expr_#_ty || ' IS' || chr(10) ||
            '    CONSTRUCTOR FUNCTION ' || project_expr_#_ty || ' (measure_set names_tty) RETURN SELF AS RESULT IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        SELF.measure_set := measure_set;' || chr(10) ||
            '        ' || chr(10) ||
            '        SELF.id := ''PROJECT'';' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            'END;';
        
        EXECUTE IMMEDIATE dice_expr_body;
        EXECUTE IMMEDIATE slice_expr_body;
        EXECUTE IMMEDIATE project_expr_body;
    END;
    
    PROCEDURE create_queryview_body(mcube_#_ty        VARCHAR2,
                                    mrel_#_trty       VARCHAR2,
                                    mrel_table        VARCHAR2,
                                    conlevel_#_ty     VARCHAR2,
                                    coordinate_#_ty   VARCHAR2,
                                    measure_#_ty      VARCHAR2,
                                    measure_#_tty     VARCHAR2,
                                    queryview_#_ty    VARCHAR2,
                                    expr_#_tty        VARCHAR2,
                                    dice_expr_#_ty    VARCHAR2,
                                    slice_expr_#_ty   VARCHAR2,
                                    project_expr_#_ty VARCHAR2,
                                    dimensions        names_tty,
                                    dim_ids           names_tty,
                                    mobject_tables    names_tty) IS
        
        queryview_body CLOB;
        queryview_constructor CLOB;
        
        queryview_dice  CLOB;
        queryview_dice1 CLOB;
        queryview_dice2 CLOB;
        
        queryview_slice  CLOB;
        queryview_slice1 CLOB;
        queryview_slice2 CLOB;
        
        queryview_project CLOB;
        
        queryview_eval CLOB;
        
        queryview_eval_dice          CLOB;
        queryview_eval_dice_var_decl CLOB;
        queryview_eval_dice_get_desc CLOB;
        queryview_eval_dice_tables   CLOB;
        queryview_eval_dice_cond     CLOB;
        
        queryview_eval_slice          CLOB;
        queryview_eval_slice_var_decl CLOB;
        queryview_eval_slice_get_obj  CLOB;
        queryview_eval_slice_cond     CLOB;
        
        queryview_eval_project CLOB;
        
        queryview_rollup              CLOB;
        queryview_rollup1             CLOB;
        queryview_rollup_var_decl     CLOB;
        queryview_rollup_select       CLOB;
        queryview_rollup_select_union CLOB;
        queryview_rollup_from         CLOB;
        queryview_rollup_from1        CLOB;
        queryview_rollup_where        CLOB;
        queryview_rollup_join_attr    CLOB;
        queryview_rollup_join_sel_var CLOB;
        queryview_rollup_join_select  CLOB;
        queryview_rollup_join_select1 CLOB;
        queryview_rollup_join_select2 CLOB;
        queryview_rollup_group_by     CLOB;
        queryview_rollup_grp_by_union CLOB;
        queryview_rollup_create_tab   CLOB;
        queryview_rollup_bind_var     CLOB;
        
        queryview_rollup_unit_aware    CLOB;
        queryview_rollup2_var_decl     CLOB;
        queryview_rollup2_select       CLOB;
        queryview_rollup2_select_union CLOB;
        queryview_rollup2_from         CLOB;
        queryview_rollup2_from1        CLOB;
        queryview_rollup2_from2        CLOB;
        queryview_rollup2_where        CLOB;
        queryview_rollup2_join_attr    CLOB;
        queryview_rollup2_join_sel_var CLOB;
        queryview_rollup2_join_select  CLOB;
        queryview_rollup2_join_select1 CLOB;
        queryview_rollup2_join_select2 CLOB;
        queryview_rollup2_group_by     CLOB;
        queryview_rollup2_grp_by_union CLOB;
        queryview_rollup2_create_tab   CLOB;
        queryview_rollup2_bind_var     CLOB;
        queryview_2rollup_elsifs       CLOB;
        
        i INTEGER;
    BEGIN
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                queryview_dice1 := queryview_dice1 || ', ';
                queryview_dice2 := queryview_dice2 || ', ';
                queryview_slice1 := queryview_slice1 || ', ';
                queryview_slice2 := queryview_slice2 || ', ';
                queryview_eval_dice_tables := queryview_eval_dice_tables || ', ';
                queryview_eval_dice_cond := queryview_eval_dice_cond || ' AND ' || chr(10);
                queryview_eval_slice_cond := queryview_eval_slice_cond || ' AND ' || chr(10);
                
                queryview_rollup1 := queryview_rollup1 || ', ';
                
                queryview_rollup_where := queryview_rollup_where || ' AND ';
                queryview_rollup_group_by := queryview_rollup_group_by || ', ';
                queryview_rollup_grp_by_union := queryview_rollup_grp_by_union || ', ';
                queryview_rollup_join_attr := queryview_rollup_join_attr || ' || '' AND ''';
                
                queryview_rollup2_where := queryview_rollup2_where || ' AND ';
                queryview_rollup2_group_by := queryview_rollup2_group_by || ', ';
                queryview_rollup2_grp_by_union := queryview_rollup2_grp_by_union || ', ';
                queryview_rollup2_join_attr := queryview_rollup2_join_attr || ' || '' AND ''';
            END IF;
            
            queryview_dice1 := queryview_dice1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            queryview_dice2 := queryview_dice2 ||
                dim_ids(i) || '_oname';
            
            queryview_slice1 := queryview_slice1 ||
                dim_ids(i) || '_predicate slice_predicate_ty';
            
            queryview_slice2 := queryview_slice2 ||
                dim_ids(i) || '_predicate';
            
            queryview_eval_dice_var_decl := queryview_eval_dice_var_decl ||
                '        ' || dim_ids(i) || '_obj         mobject_ty;' || chr(10) ||
                '        ' || dim_ids(i) || '_descendants names_tty;' || chr(10) ||
                '        ' || chr(10);
            
            queryview_eval_dice_get_desc := queryview_eval_dice_get_desc ||
                '        utl_ref.select_object(TREAT(dice_expression.dice_coordinate.' || dim_ids(i) || '_obj AS REF mobject_ty), ' || dim_ids(i) || '_obj);' || chr(10) ||
                '        ' || dim_ids(i) || '_descendants := ' || dim_ids(i) || '_obj.get_descendants_onames();' || chr(10) ||
                '        ' || dim_ids(i) || '_descendants.EXTEND;' || chr(10) ||
                '        ' || dim_ids(i) || '_descendants(' || dim_ids(i) || '_descendants.LAST) := dice_expression.dice_coordinate.' || dim_ids(i) || '_oname;' || chr(10) ||
                '        ' || chr(10);
            
            queryview_eval_dice_tables := queryview_eval_dice_tables ||
                'TABLE(' || dim_ids(i) || '_descendants) ' || dim_ids(i);
            
            /*
            queryview_eval_dice_cond := queryview_eval_dice_cond ||
                '               m.coordinate.' || dim_ids(i) || '_oname IN ' || dim_ids(i) || '.column_value';
            */
            
            queryview_eval_dice_cond := queryview_eval_dice_cond ||
                '               m.coordinate.' || dim_ids(i) || '_oname IN (SELECT x.column_value FROM TABLE(' || dim_ids(i) || '_descendants) x)';
            
            queryview_eval_slice_var_decl := queryview_eval_slice_var_decl ||
                '        ' || dim_ids(i) || '_obj     mobject_ty;' || chr(10) ||
                '        ' || dim_ids(i) || '_names   names_tty;' || chr(10) ||
                '        ' || dim_ids(i) || '_sat     names_tty;' || chr(10) ||
                '        ' || dim_ids(i) || '_desc    names_tty;' || chr(10) ||
                '        ' || dim_ids(i) || '_anc     names_tty;' || chr(10) ||
                '        ' || dim_ids(i) || '_objects mobject_trty;' || chr(10) ||
                '        ' || chr(10);
            
            queryview_eval_slice_get_obj := queryview_eval_slice_get_obj ||
                '        IF slice_expression.' || dim_ids(i) || '_predicate IS NOT NULL THEN' || chr(10) ||
                '            ' || dim_ids(i) || '_objects :=' || chr(10) ||
                '                slice_expression.' || dim_ids(i) || '_predicate.get_satisfying_objects;' || chr(10) ||
                '            ' || chr(10) ||
                '            -- retrieve the names of the satisfying m-objects' || chr(10) ||
                '            SELECT o.oname BULK COLLECT INTO ' || dim_ids(i) || '_sat' || chr(10) ||
                '            FROM   ' || mobject_tables(i) || ' o, TABLE(' || dim_ids(i) || '_objects) sat' || chr(10) ||
                '            WHERE  REF(o) = sat.column_value;' || chr(10) ||
                '            ' || chr(10) ||
                '            -- retrieve the descendants of the satisfying m-objects' || chr(10) ||
                '            SELECT des.oname BULK COLLECT INTO ' || dim_ids(i) || '_desc' || chr(10) ||
                '            FROM   (SELECT o.oname AS oname, a.ancestor.oname AS ancestor_name' || chr(10) ||
                '                    FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) a' || chr(10) ||
                '                    WHERE  o.ancestors IS NOT NULL) des -- TODO: Perhaps replace this subquery with a view/table to cache the values and speed up the query.' || chr(10) ||
                '            WHERE  des.ancestor_name IN  ' || chr(10) ||
                '                       (SELECT a.column_value FROM TABLE(' || dim_ids(i) || '_sat) a);' || chr(10) ||
                '            ' || chr(10) ||
                '            -- retrieve the ancestors of the satisfying m-objects' || chr(10) ||
                '            SELECT DISTINCT anc.ancestor_name BULK COLLECT INTO ' || dim_ids(i) || '_anc' || chr(10) ||
                '            FROM   (SELECT o.oname AS oname, a.ancestor.oname AS ancestor_name' || chr(10) ||
                '                    FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) a' || chr(10) ||
                '                    WHERE  o.oname IN (SELECT a.column_value FROM TABLE(' || dim_ids(i) || '_sat) a)) anc;' || chr(10) ||
                '            ' || chr(10) ||
                '            -- union the collection' || chr(10) ||
                '            SELECT u.oname BULK COLLECT INTO ' || dim_ids(i) || '_names' || chr(10) ||
                '            FROM   (SELECT s.column_value AS oname' || chr(10) ||
                '                    FROM   TABLE(' || dim_ids(i) || '_sat) s' || chr(10) ||
                '                    UNION' || chr(10) ||
                '                    SELECT d.column_value AS oname' || chr(10) ||
                '                    FROM   TABLE(' || dim_ids(i) || '_desc) d' || chr(10) ||
                '                    UNION' || chr(10) ||
                '                    SELECT a.column_value AS oname' || chr(10) ||
                '                    FROM   TABLE(' || dim_ids(i) || '_anc) a) u;' || chr(10) ||
                '            ' || chr(10) ||
                '            -- free memory' || chr(10) ||
                '            ' || dim_ids(i) || '_sat  := NULL;' || chr(10) ||
                '            ' || dim_ids(i) || '_desc := NULL;' || chr(10) ||
                '            ' || dim_ids(i) || '_anc  := NULL;' || chr(10) ||
                '            ' || chr(10) ||
                '        END IF;' || chr(10) ||
                '        ' || chr(10);
            
            queryview_eval_slice_cond := queryview_eval_slice_cond ||
                '               (slice_expression.' || dim_ids(i) || '_predicate IS NULL OR m.coordinate.' || dim_ids(i) || '_oname IN (SELECT y.column_value FROM TABLE(' || dim_ids(i) || '_names) y))';
            
            queryview_rollup1 := queryview_rollup1 ||
                dim_ids(i) || '_level VARCHAR2';
            
            queryview_rollup_select := queryview_rollup_select ||
                dim_ids(i) || '_anc.ancestor AS ' || dim_ids(i) || ', ';
            
            queryview_rollup_select_union := queryview_rollup_select_union ||
                dim_ids(i) || ', ';
            
            queryview_rollup_group_by := queryview_rollup_group_by ||
                dim_ids(i) || '_anc.ancestor';
            
            queryview_rollup_grp_by_union := queryview_rollup_grp_by_union ||
                dim_ids(i);
            
            queryview_rollup_var_decl := queryview_rollup_var_decl ||
                '        select_subquery1_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        select_subquery2_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        select_subquery3_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        ' || chr(10) ||
                '        ' || dim_ids(i) || '_dname VARCHAR2(30) := ''' || dimensions(i) || ''';' || chr(10) ||
                '        ' || chr(10);
            
            queryview_rollup_from := queryview_rollup_from || 
                '        select_subquery1_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, anc.ancestor.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) anc'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  anc.lvl = :' || dim_ids(i) || '_level'' || chr(10) ||' || chr(10) ||
                '            ''                 UNION'' || chr(10) ||' || chr(10) ||
                '            ''             SELECT o.oname AS oname, ''''Other: '''' || anc.ancestor.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) anc'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  NOT EXISTS(SELECT * FROM TABLE(o.ancestors) WHERE lvl = :' || dim_ids(i) || '_level) AND'' || chr(10) ||' || chr(10) ||
                '            ''                    ('' || chr(10) ||' || chr(10) ||
                '            ''                     ('' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.parents)) = 1 AND'' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT p.COLUMN_VALUE.oname FROM TABLE(o.parents) p) = anc.ancestor.oname'' || chr(10) ||' || chr(10) ||
                '            ''                     ) OR'' || chr(10) ||' || chr(10) ||
                '            ''                     ('' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.parents)) > 1 AND'' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.dim.level_positions) p WHERE p.position = (SELECT p.position FROM TABLE(o.dim.level_positions) p WHERE p.lvl = anc.ancestor.top_level)) = 1'' || chr(10) ||' || chr(10) ||
                '            ''                     )'' || chr(10) ||' || chr(10) ||
                '            ''                    )'' || chr(10) ||' || chr(10) ||
                '            ''            ) ' || dim_ids(i) || '_anc, '' || chr(10);' || chr(10) ||
                '        ' || chr(10) ||
                '        select_subquery2_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, o.oname || '''' ('''' || o.top_level || '''')'''' AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  NOT EXISTS(SELECT * FROM TABLE(o.ancestors) WHERE lvl = :' || dim_ids(i) || '_level) AND'' || chr(10) ||' || chr(10) ||
                '            ''                    NOT EXISTS(SELECT * FROM TABLE(o.level_hierarchy) WHERE lvl = :' || dim_ids(i) || '_level)) ' || dim_ids(i) || '_anc, '' || chr(10); ' || chr(10) ||
                '        ' || chr(10) ||
                '        select_subquery3_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, o.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  o.top_level = :' || dim_ids(i) || '_level) ' || dim_ids(i) || '_anc, '' || chr(10); ' || chr(10) ||
                '        ' || chr(10);
            
            queryview_rollup_from1 := queryview_rollup_from1 ||
                '                ' || chr(10) ||
                '                SELECT p.position INTO measure_level_pos' || chr(10) ||
                '                FROM   dimensions d, TABLE(d.level_positions) p' || chr(10) ||
                '                WHERE  d.dname = ' || dim_ids(i) || '_dname AND' || chr(10) ||
                '                       p.lvl = measure_descriptions(j).measure_level.' || dim_ids(i) || '_level;' || chr(10) ||
                '                ' || chr(10) ||
                '                SELECT p.position INTO rollup_level_pos' || chr(10) ||
                '                FROM   dimensions d, TABLE(d.level_positions) p' || chr(10) ||
                '                WHERE  d.dname = ' || dim_ids(i) || '_dname AND' || chr(10) ||
                '                       p.lvl = ' || dim_ids(i) || '_level;' || chr(10) ||
                '                ' || chr(10) ||
                '                IF measure_descriptions(j).measure_level.' || dim_ids(i) || '_level <> ' || dim_ids(i) || '_level AND ' || chr(10) ||
                '                   measure_level_pos >= rollup_level_pos THEN' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery1_' || dim_ids(i) || ';' || chr(10) ||
                '                ELSIF measure_descriptions(j).measure_level.' || dim_ids(i) || '_level <> ' || dim_ids(i) || '_level AND ' || chr(10) ||
                '                   measure_level_pos < rollup_level_pos THEN' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery2_' || dim_ids(i) || ';' || chr(10) ||
                '                ELSE' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery3_' || dim_ids(i) || ';' || chr(10) ||
                '                END IF;' || chr(10) ||
                '                ' || chr(10);
            
            queryview_rollup2_from2 := queryview_rollup2_from2 ||
                '                        ELSIF units_cube_dimension_names(k) = ''' || dimensions(i) || ''' THEN' || chr(10) ||
                '                            select_rollup_where := select_rollup_where ||' || chr(10) ||
                '                                '' AND m.coordinate.' || dim_ids(i) || '_oname = r.' || dimensions(i) || ''';' || chr(10);
            
            queryview_rollup_where := queryview_rollup_where ||
                'm.coordinate.' || dim_ids(i) || '_oname = ' || dim_ids(i) || '_anc.oname';
            
            queryview_rollup_join_attr := queryview_rollup_join_attr ||
                '|| measure_name || ''.' || dim_ids(i) || ' = '' || measure_list(measure_list.PRIOR(i)) || ''.' || dim_ids(i) || '''';
            
            queryview_rollup_create_tab := queryview_rollup_create_tab ||
                '            ''    ' || dimensions(i) || ' VARCHAR2(63),'' || chr(10) ||' || chr(10);
            
            queryview_rollup_bind_var := queryview_rollup_bind_var ||
                '        dbms_sql.bind_variable(sql_cursor, ''' || dim_ids(i) || '_level'', ' || dim_ids(i) || '_level);' || chr(10);
            
            queryview_rollup_join_sel_var := queryview_rollup_join_sel_var ||
                '        select_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10);
            
            queryview_rollup_join_select1 := queryview_rollup_join_select1 ||
                '                select_' || dim_ids(i) || ' := measure_list(i) || ''.' || dim_ids(i) || ''' ;' || chr(10);
            
            queryview_rollup_join_select2 := queryview_rollup_join_select2 ||
                '                select_' || dim_ids(i) || ' := ' || chr(10) ||
                '                    ''NVL('' || measure_list(i) || ''.' || dim_ids(i) || ', '' || select_' || dim_ids(i) || ' || '')'';' || chr(10);
            
            queryview_rollup_join_select := queryview_rollup_join_select ||
                'select_' || dim_ids(i) || ' || '', '' ||';
            
            /*****/
            
            queryview_rollup2_select := queryview_rollup2_select ||
                dim_ids(i) || '_anc.ancestor AS ' || dim_ids(i) || ', ';
            
            queryview_rollup2_select_union := queryview_rollup2_select_union ||
                dim_ids(i) || ', ';
            
            queryview_rollup2_group_by := queryview_rollup2_group_by ||
                dim_ids(i) || '_anc.ancestor';
            
            queryview_rollup2_grp_by_union := queryview_rollup2_grp_by_union ||
                dim_ids(i);
            
            queryview_rollup2_var_decl := queryview_rollup2_var_decl ||
                '        select_subquery1_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        select_subquery2_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        select_subquery3_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        ' || chr(10) ||
                '        ' || dim_ids(i) || '_dname VARCHAR2(30) := ''' || dimensions(i) || ''';' || chr(10) ||
                '        ' || chr(10);
            
            queryview_rollup2_from := queryview_rollup2_from || 
                '        select_subquery1_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, anc.ancestor.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) anc'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  anc.lvl = :' || dim_ids(i) || '_level'' || chr(10) ||' || chr(10) ||
                '            ''                 UNION'' || chr(10) ||' || chr(10) ||
                '            ''             SELECT o.oname AS oname, ''''Other: '''' || anc.ancestor.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) anc'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  NOT EXISTS(SELECT * FROM TABLE(o.ancestors) WHERE lvl = :' || dim_ids(i) || '_level) AND'' || chr(10) ||' || chr(10) ||
                '            ''                    ('' || chr(10) ||' || chr(10) ||
                '            ''                     ('' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.parents)) = 1 AND'' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT p.COLUMN_VALUE.oname FROM TABLE(o.parents) p) = anc.ancestor.oname'' || chr(10) ||' || chr(10) ||
                '            ''                     ) OR'' || chr(10) ||' || chr(10) ||
                '            ''                     ('' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.parents)) > 1 AND'' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.dim.level_positions) p WHERE p.position = (SELECT p.position FROM TABLE(o.dim.level_positions) p WHERE p.lvl = anc.ancestor.top_level)) = 1'' || chr(10) ||' || chr(10) ||
                '            ''                     )'' || chr(10) ||' || chr(10) ||
                '            ''                    )'' || chr(10) ||' || chr(10) ||
                '            ''            ) ' || dim_ids(i) || '_anc, '' || chr(10);' || chr(10) ||
                '        ' || chr(10) ||
                '        select_subquery2_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, o.oname || '''' ('''' || o.top_level || '''')'''' AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  NOT EXISTS(SELECT * FROM TABLE(o.ancestors) WHERE lvl = :' || dim_ids(i) || '_level) AND'' || chr(10) ||' || chr(10) ||
                '            ''                    NOT EXISTS(SELECT * FROM TABLE(o.level_hierarchy) WHERE lvl = :' || dim_ids(i) || '_level)) ' || dim_ids(i) || '_anc, '' || chr(10); ' || chr(10) ||
                '        ' || chr(10) ||
                '        select_subquery3_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, o.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  o.top_level = :' || dim_ids(i) || '_level) ' || dim_ids(i) || '_anc, '' || chr(10); ' || chr(10) ||
                '        ' || chr(10);
            
            queryview_rollup2_from1 := queryview_rollup2_from1 ||
                '                ' || chr(10) ||
                '                SELECT p.position INTO measure_level_pos' || chr(10) ||
                '                FROM   dimensions d, TABLE(d.level_positions) p' || chr(10) ||
                '                WHERE  d.dname = ' || dim_ids(i) || '_dname AND' || chr(10) ||
                '                       p.lvl = measure_descriptions(j).measure_level.' || dim_ids(i) || '_level;' || chr(10) ||
                '                ' || chr(10) ||
                '                SELECT p.position INTO rollup_level_pos' || chr(10) ||
                '                FROM   dimensions d, TABLE(d.level_positions) p' || chr(10) ||
                '                WHERE  d.dname = ' || dim_ids(i) || '_dname AND' || chr(10) ||
                '                       p.lvl = ' || dim_ids(i) || '_level;' || chr(10) ||
                '                ' || chr(10) ||
                '                IF measure_descriptions(j).measure_level.' || dim_ids(i) || '_level <> ' || dim_ids(i) || '_level AND ' || chr(10) ||
                '                   measure_level_pos >= rollup_level_pos THEN' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery1_' || dim_ids(i) || ';' || chr(10) ||
                '                ELSIF measure_descriptions(j).measure_level.' || dim_ids(i) || '_level <> ' || dim_ids(i) || '_level AND ' || chr(10) ||
                '                   measure_level_pos < rollup_level_pos THEN' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery2_' || dim_ids(i) || ';' || chr(10) ||
                '                ELSE' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery3_' || dim_ids(i) || ';' || chr(10) ||
                '                END IF;' || chr(10) ||
                '                ' || chr(10);
            
            queryview_rollup2_where := queryview_rollup2_where ||
                'm.coordinate.' || dim_ids(i) || '_oname = ' || dim_ids(i) || '_anc.oname';
            
            queryview_rollup2_join_attr := queryview_rollup2_join_attr ||
                '|| measure_name || ''.' || dim_ids(i) || ' = '' || measure_list(measure_list.PRIOR(i)) || ''.' || dim_ids(i) || '''';
            
            queryview_rollup2_create_tab := queryview_rollup2_create_tab ||
                '            ''    ' || dimensions(i) || ' VARCHAR2(63),'' || chr(10) ||' || chr(10);
            
            queryview_rollup2_bind_var := queryview_rollup2_bind_var ||
                '        dbms_sql.bind_variable(sql_cursor, ''' || dim_ids(i) || '_level'', ' || dim_ids(i) || '_level);' || chr(10);
            
            queryview_rollup2_join_sel_var := queryview_rollup2_join_sel_var ||
                '        select_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10);
            
            queryview_rollup2_join_select1 := queryview_rollup2_join_select1 ||
                '                select_' || dim_ids(i) || ' := measure_list(i) || ''.' || dim_ids(i) || ''' ;' || chr(10);
            
            queryview_rollup2_join_select2 := queryview_rollup2_join_select2 ||
                '                select_' || dim_ids(i) || ' := ' || chr(10) ||
                '                    ''NVL('' || measure_list(i) || ''.' || dim_ids(i) || ', '' || select_' || dim_ids(i) || ' || '')'';' || chr(10);
            
            queryview_rollup2_join_select := queryview_rollup2_join_select ||
                'select_' || dim_ids(i) || ' || '', '' ||';
            
            queryview_2rollup_elsifs := queryview_2rollup_elsifs ||
                '                            ELSIF units_cube_dimension_names(k) = ''' || dimensions(i) || ''' THEN' || chr(10) ||
                '                                rollup_table_args := rollup_table_args || ' || chr(10) ||
                '                                    '', '' ||  '''''''' || measure_descriptions(j).measure_level. ' || dim_ids(i) ||  '_level || '''''''';' || chr(10);
            
            i := dimensions.NEXT(i);
        END LOOP;
        
        queryview_constructor := 
            '    CONSTRUCTOR FUNCTION ' || queryview_#_ty || '(mcube REF ' || mcube_#_ty || ') RETURN SELF AS RESULT IS' || chr(10) ||
            '        mc ' || mcube_#_ty || ';' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- store the base m-cube''s reference' || chr(10) ||
            '        SELF.mcube := mcube;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- adopt the base m-cube''s root coordinate' || chr(10) ||
            '        utl_ref.select_object(mcube, mc);' || chr(10) ||
            '        SELF.root_coordinate := mc.root_coordinate;' || chr(10) ||
            '        ' || chr(10) ||
            '        /*** initialize the collections ***/' || chr(10) ||
            '        -- get all m-relationships' || chr(10) ||
            '        SELECT REF(m) BULK COLLECT INTO SELF.mrelationship_set' || chr(10) ||
            '        FROM   ' || mrel_table || ' m;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- get all measures' || chr(10) ||
            '        SELECT DISTINCT utc.column_name BULK COLLECT INTO SELF.measure_set' || chr(10) ||
            '        FROM   user_tab_columns utc, ' || chr(10) ||
            '               (SELECT UPPER(t.table_name) AS table_name' || chr(10) ||
            '                FROM   ' || mrel_table || ' o, TABLE(o.measure_tables) t) x' || chr(10) ||
            '        WHERE  utc.table_name IN x.table_name AND ' || chr(10) ||
            '               utc.column_name <> ''COORDINATE'' AND ' || chr(10) ||
            '               utc.column_name <> ''MREL'' AND utc.column_name NOT LIKE ''%_UNIT'';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- initialize the expressions collection (empty)' || chr(10) ||
            '        SELF.expressions := ' || expr_#_tty || '();' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10);
        
        queryview_dice :=
            '    MEMBER FUNCTION dice(SELF IN OUT ' || queryview_#_ty || ', ' || queryview_dice1 || ') RETURN ' || queryview_#_ty || ' IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        SELF.expressions.EXTEND;' || chr(10) ||
            '        SELF.expressions(SELF.expressions.LAST) := ' || dice_expr_#_ty || '(' || coordinate_#_ty || '(' || queryview_dice2 || '));' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN SELF;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10);
        
        queryview_slice :=
            '    MEMBER FUNCTION slice(SELF IN OUT ' || queryview_#_ty || ', ' || queryview_slice1 || ') RETURN ' || queryview_#_ty || ' IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        SELF.expressions.EXTEND;' || chr(10) ||
            '        SELF.expressions(SELF.expressions.LAST) := ' || slice_expr_#_ty || '(' || queryview_slice2 || ');' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN SELF;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10);
        
        queryview_project :=
            '    MEMBER FUNCTION project(SELF IN OUT ' || queryview_#_ty || ', measure_set names_tty) RETURN ' || queryview_#_ty || ' IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        SELF.expressions.EXTEND;' || chr(10) ||
            '        SELF.expressions(SELF.expressions.LAST) := ' || project_expr_#_ty || '(measure_set);' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN SELF;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || CHR(10);
        
        queryview_eval :=
            '    MEMBER FUNCTION evaluate(SELF IN OUT ' || queryview_#_ty || ') RETURN ' || queryview_#_ty  || ' IS' || chr(10) ||
            '        queryview ' ||queryview_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- loop through the list of expressions and invoke for each' || chr(10) ||
            '        -- expression type the corresponding function.' || chr(10) ||
            '        -- NOTE: Tried to use polymorphism but problem with mutual dependencies!' || chr(10) ||
            '        i := SELF.expressions.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            CASE UPPER(SELF.expressions(i).id)' || chr(10) ||
            '                WHEN ''DICE'' THEN' || chr(10) ||
            '                    queryview := SELF.evaluate(TREAT(SELF.expressions(i) AS ' || dice_expr_#_ty || '));' || chr(10) ||
            '                WHEN ''SLICE'' THEN' || chr(10) ||
            '                    queryview := SELF.evaluate(TREAT(SELF.expressions(i) AS ' || slice_expr_#_ty || '));' || chr(10) ||
            '                WHEN ''PROJECT'' THEN' || chr(10) ||
            '                    queryview := SELF.evaluate(TREAT(SELF.expressions(i) AS ' || project_expr_#_ty || '));' || chr(10) ||
            '            END CASE;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := SELF.expressions.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- reset the expression list' || chr(10) ||
            '        SELF.expressions := ' || expr_#_tty || '();' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN SELF;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10);
        
        queryview_eval_dice :=
            '    MEMBER FUNCTION evaluate(SELF IN OUT ' || queryview_#_ty || ', dice_expression ' || dice_expr_#_ty || ') RETURN ' || queryview_#_ty  || ' IS' || chr(10) ||
                     queryview_eval_dice_var_decl ||
            '        ' || chr(10) ||
            '        new_mrelationship_set  ' || mrel_#_trty || ';' || chr(10) ||
            '    BEGIN' || chr(10) ||
                     queryview_eval_dice_get_desc ||
            '        SELECT REF(m) BULK COLLECT INTO new_mrelationship_set' || chr(10) ||
            '        FROM   ' || mrel_table || ' m --, TABLE(SELF.mrelationship_set) x, ' || queryview_eval_dice_tables  || chr(10) ||
            '        WHERE  --REF(m) = x.column_value AND' || chr(10) ||
            '               REF(m) IN (SELECT x.column_value FROM TABLE(SELF.mrelationship_set) x) AND' || chr(10) ||
                            queryview_eval_dice_cond || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- update the root coordinate which is now the dice_coordinate' || chr(10) ||
            '        SELF.root_coordinate := dice_expression.dice_coordinate;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- update the m-relationship set' || chr(10) ||
            '        SELF.mrelationship_set := new_mrelationship_set;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN SELF;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10);
        
        queryview_eval_slice :=
            '    MEMBER FUNCTION evaluate(SELF IN OUT ' || queryview_#_ty || ', slice_expression ' || slice_expr_#_ty || ') RETURN ' || queryview_#_ty  || ' IS' || chr(10) ||
                     queryview_eval_slice_var_decl ||
            '        ' || chr(10) ||
            '        new_mrelationship_set  ' || mrel_#_trty || ';' || chr(10) ||
            '    BEGIN' || chr(10) ||
                     queryview_eval_slice_get_obj ||
            '        SELECT REF(m) BULK COLLECT INTO new_mrelationship_set' || chr(10) ||
            '        FROM   ' || mrel_table || ' m, TABLE(SELF.mrelationship_set) x' || chr(10) ||
            '        WHERE  REF(m) = x.column_value AND' || chr(10) ||
                            queryview_eval_slice_cond || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- update the m-relationship set' || chr(10) ||
            '        SELF.mrelationship_set := new_mrelationship_set;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN SELF;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10);
        
        queryview_eval_project :=
            '    MEMBER FUNCTION evaluate(SELF IN OUT ' || queryview_#_ty || ', project_expression ' || project_expr_#_ty || ') RETURN ' || queryview_#_ty  || ' IS' || chr(10) ||
            '        row_count INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- verify that all measures contained in the new measure' || chr(10) ||
            '        -- set are contained in the current measure set.' || chr(10) ||
            '        SELECT COUNT(*) INTO row_count' || chr(10) ||
            '        FROM   TABLE(project_expression.measure_set) x' || chr(10) ||
            '        WHERE  NOT EXISTS(SELECT y.column_value' || chr(10) ||
            '                          FROM   TABLE(SELF.measure_set) y' || chr(10) ||
            '                          WHERE  UPPER(x.column_value) = UPPER(y.column_value));' || chr(10) ||
            '        ' || chr(10) ||
            '        IF row_count = 0 THEN' || chr(10) ||
            '            SELF.measure_set := project_expression.measure_set;' || chr(10) ||
            '        ELSE' || chr(10) ||
            '            -- TODO: error handling!' || chr(10) ||
            '            NULL;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN SELF;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10);
        
        queryview_rollup_unit_aware :=
            '    MEMBER PROCEDURE rollup(table_name VARCHAR2, include_non_dimension_attr BOOLEAN, measure_units measure_unit_tty, ' || queryview_rollup1 || ') IS' || chr(10) ||
            '        ' || chr(10) ||
            '        mc ' || mcube_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        TYPE string_tty IS TABLE OF VARCHAR2(10000) INDEX BY VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- this type is used to store table names of the rollup tables,' || chr(10) ||
            '        -- indexed by their aggregation level.' || chr(10) ||
            '        TYPE table_names_tty IS TABLE OF VARCHAR2(30) INDEX BY VARCHAR2(' || dimensions.COUNT * 30 || ');' || chr(10) ||
            '        ' || chr(10) ||
            '        -- this type stores the rollup table names by rollup cube' || chr(10) ||
            '        TYPE table_names_ttty IS TABLE OF table_names_tty INDEX BY VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_measures string_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        rollup_tables_by_cube table_names_ttty;' || chr(10) ||
            '        rollup_table  VARCHAR2(30);' || chr(10) ||
            '        rollup_level  VARCHAR2(30);' || chr(10) ||
            '        rollup_table_args VARCHAR2(5000);' || chr(10) ||
            '        ' || chr(10) ||
            '        conlevel_string VARCHAR2(' || dimensions.COUNT * 30 || ');' || chr(10) ||
            '        ' || chr(10) ||
            '        create_table  VARCHAR2(2000);' || chr(10) ||
            '        create_table1 VARCHAR2(1000);' || chr(10) ||
            '        create_table2 VARCHAR2(2000);' || chr(10) ||
            '        insert_data   VARCHAR2(30000);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_join                 CLOB;' || chr(10) ||
            '        select_join_select_dim      VARCHAR2(5000);' || chr(10) ||
            '        select_join_select_measures VARCHAR2(5000);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_rollup        VARCHAR2(10000);' || chr(10) ||
            '        select_rollup_from   VARCHAR2(5000);' || chr(10) ||
            '        select_rollup_from1  VARCHAR2(5000);' || chr(10) ||
            '        select_rollup_select VARCHAR2(4000);' || chr(10) ||
            '        select_rollup_where  VARCHAR2(4000);' || chr(10) ||
            '        ' || chr(10) ||
                     queryview_rollup2_var_decl ||
            '        ' || chr(10) ||
                     queryview_rollup2_join_sel_var ||
            '        ' || chr(10) ||
            '        measure_list names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_name         VARCHAR2(30);' || chr(10) ||
            '        measure_descriptions ' || measure_#_tty || ';' || chr(10) ||
            '        measure_table        VARCHAR2(30);' || chr(10) ||
            '        measure_datatype     VARCHAR2(30);' || chr(10) ||
            '        typecode             VARCHAR2(100);' || chr(10) ||
            '        ' || chr(10) ||
            '        nested_table_name  VARCHAR2(30);' || chr(10) ||
            '        nested_table_name1 VARCHAR2(60);' || chr(10) ||
            '        ' || chr(10) ||
            '        aggregation_function VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_level_pos INTEGER;' || chr(10) ||
            '        rollup_level_pos INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        convert_measure INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        unit_any ANYDATA;' || chr(10) ||
            '        unit_ref REF mobject_ty;' || chr(10) ||
            '        unit_obj mobject_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        dim_ref  REF dimension_ty;' || chr(10) ||
            '        dim_obj  dimension_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        unit_dname VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        units_cube_any ANYDATA;' || chr(10) ||
            '        units_cube_ref REF mcube_ty;' || chr(10) ||
            '        units_cube_obj mcube_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        units_cname   VARCHAR2(30);' || chr(10) ||
            '        units_cube_dimension_names names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '        j INTEGER;' || chr(10) ||
            '        k INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        sql_cursor INTEGER;' || chr(10) ||
            '        rows_processed INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        status PLS_INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        utl_ref.select_object(SELF.mcube, mc);' || chr(10) ||
            '        ' || chr(10) ||
                     queryview_rollup2_from || chr(10) ||
            '        ' || chr(10) ||
            '        -- get all measures from the query view' || chr(10) ||
            '        measure_list := SELF.measure_set;' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.LAST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            IF i = measure_list.LAST THEN' || chr(10) ||
                             queryview_rollup2_join_select1 ||
            '            ELSE' || chr(10) ||
                             queryview_rollup2_join_select2 ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.PRIOR(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- rollup the unit cube of every measure that should be converted' || chr(10) ||
            '        i := measure_list.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            -- check if the current measure is to be converted' || chr(10) ||
            '            SELECT COUNT(*) INTO convert_measure' || chr(10) ||
            '            FROM   TABLE(measure_units)' || chr(10) ||
            '            WHERE  UPPER(measure_name) = UPPER(measure_list(i));' || chr(10) ||
            '            ' || chr(10) ||
            '            -- only convert if the count is (exactly) one,' || chr(10) ||
            '            -- otherwise conversion doesn''t make sense' || chr(10) ||
            '            IF convert_measure = 1 THEN' || chr(10) ||
            '                -- get the target unit and the units cube used for conversion' || chr(10) ||
            '                -- the following raised a generic error!' || chr(10) ||
            '                -- SELECT measure_unit, conversion_rule INTO unit_any, units_cube_any' || chr(10) ||
            '                -- FROM   TABLE(measure_units)' || chr(10) ||
            '                -- WHERE  UPPER(measure_name) = UPPER(measure_list(i));' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the target unit and the units cube used for conversion' || chr(10) ||
            '                j := measure_units.FIRST;' || chr(10) ||
            '                WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                    IF UPPER(measure_units(j).measure_name) = UPPER(measure_list(i)) THEN' || chr(10) ||
            '                        unit_any := measure_units(j).measure_unit;' || chr(10) ||
            '                        units_cube_any := measure_units(j).conversion_rule;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        j := NULL;' || chr(10) ||
            '                    ELSE' || chr(10) ||
            '                        j := measure_units.NEXT(j);' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the references' || chr(10) ||
            '                status := ANYDATA.getRef(unit_any, unit_ref);' || chr(10) ||
            '                status := ANYDATA.getRef(units_cube_any, units_cube_ref);' || chr(10) ||
            '                ' || chr(10) ||
            '                -- select the objects' || chr(10) ||
            '                utl_ref.select_object(unit_ref, unit_obj);' || chr(10) ||
            '                utl_ref.select_object(units_cube_ref, units_cube_obj);' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the name of the unit''s dimension' || chr(10) ||
            '                SELECT d.dname INTO unit_dname' || chr(10) ||
            '                FROM   dimensions d' || chr(10) ||
            '                WHERE  REF(d) = unit_obj.dim;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the name of the current measure' || chr(10) ||
            '                measure_name := measure_list(i);' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the name of the units cube' || chr(10) ||
            '                units_cname := units_cube_obj.cname;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the dimension names of the units cube' || chr(10) ||
            '                units_cube_dimension_names :=' || chr(10) ||
            '                    units_cube_obj.get_dimension_names();' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the different connection levels of this measure' || chr(10) ||
            '                measure_descriptions :=' || chr(10) ||
            '                    mc.get_measure_descriptions(measure_name);' || chr(10) ||
            '                ' || chr(10) ||
            '                -- a rollup of the units cube has to be done for' || chr(10) ||
            '                -- every connection level.' || chr(10) ||
            '                j := measure_descriptions.FIRST;' || chr(10) ||
            '                WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                    IF rollup_tables_by_cube.EXISTS(units_cname) THEN' || chr(10) ||
            '                        IF rollup_tables_by_cube(units_cname).EXISTS(measure_descriptions(j).measure_level.to_string2()) THEN' || chr(10) ||
            '                            rollup_table := ' || chr(10) ||
            '                                 rollup_tables_by_cube(units_cname)(measure_descriptions(j).measure_level.to_string2());' || chr(10) ||
            '                        ELSE' || chr(10) ||
            '                            rollup_table := NULL;' || chr(10) ||
            '                        END IF;' || chr(10) ||
            '                    ELSE' || chr(10) ||
            '                        rollup_table := NULL;' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    -- if there is no rollup table for this level' || chr(10) ||
            '                    -- then create a new one' || chr(10) ||
            '                    IF rollup_table IS NULL THEN' || chr(10) ||
            '                        -- create a unique table name for the rollup table' || chr(10) ||
            '                        rollup_table :=' || chr(10) ||
            '                            identifiers.get_unique_short_name(30,' || chr(10) ||
            '                                                              UPPER(units_cube_obj.id || ''_rollup_'' || measure_descriptions(j).measure_level.to_string()),' || chr(10) ||
            '                                                              ''user_tab_columns'',' || chr(10) ||
            '                                                              ''table_name'');' || chr(10) ||
            '                        ' || chr(10) ||
            '                        -- save the table name' || chr(10) ||
            '                        rollup_tables_by_cube(units_cname)(measure_descriptions(j).measure_level.to_string2()) :=' || chr(10) ||
            '                            rollup_table;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        rollup_table_args := NULL;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        -- get the rollup levels' || chr(10) ||
            '                        k := units_cube_dimension_names.FIRST;' || chr(10) ||
            '                        WHILE k IS NOT NULL LOOP' || chr(10) ||
            '                            IF unit_dname = units_cube_dimension_names(k) THEN' || chr(10) ||
            '                                rollup_table_args := rollup_table_args || ' || chr(10) ||
            '                                    '', '''''' || unit_obj.top_level || '''''''';' || chr(10) ||
                                         queryview_2rollup_elsifs ||
            '                            ELSE' || chr(10) ||
            '                                -- get the top level of the dimension' || chr(10) ||
            '                                SELECT h.lvl INTO rollup_level' || chr(10) ||
            '                                FROM   dimensions d, TABLE(d.level_hierarchy) h' || chr(10) ||
            '                                WHERE  d.dname = units_cube_dimension_names(k) AND' || chr(10) ||
            '                                       h.parent_level IS NULL;' || chr(10) ||
            '                                ' || chr(10) ||
            '                                rollup_table_args := rollup_table_args || ' || chr(10) ||
            '                                    '', '''''' || rollup_level || '''''''';' || chr(10) ||
            '                            END IF;' || chr(10) ||
            '                            ' || chr(10) ||
            '                            k := units_cube_dimension_names.NEXT(k);' || chr(10) ||
            '                        END LOOP;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        -- ' || chr(10) ||
            '                        EXECUTE IMMEDIATE' || chr(10) ||
            '                            ''DECLARE'' || chr(10) ||' || chr(10) ||
            '                            ''    mcube_ref REF mcube_ty := :1;'' || chr(10) ||' || chr(10) ||
            '                            ''    mcube_obj mcube_ty;'' || chr(10) ||' || chr(10) ||
            '                            ''    mcube_#_obj '' || units_cube_obj.mcube_#_ty || '';'' || chr(10) ||' || chr(10) ||
            '                            ''BEGIN'' || chr(10) ||' || chr(10) ||
            '                            ''    utl_ref.select_object(mcube_ref, mcube_obj);'' || chr(10) ||' || chr(10) ||
            '                            ''    mcube_#_obj := TREAT(mcube_obj AS '' || units_cube_obj.mcube_#_ty || '');'' || chr(10) ||' || chr(10) ||
            '                            ''    '' || chr(10) ||' || chr(10) ||
            '                            ''    mcube_#_obj.rollup(:2, FALSE '' || rollup_table_args || '');'' || chr(10) ||' || chr(10) ||
            '                            ''END;''' || chr(10) ||
            '                            USING units_cube_ref, rollup_table;' || chr(10) ||
            '                        ' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    j := measure_descriptions.NEXT(j);' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            -- reset' || chr(10) ||
            '            select_rollup := NULL;' || chr(10) ||
            '            select_rollup_select := NULL;' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_name := measure_list(i);' || chr(10) ||
            '            measure_descriptions :=' || chr(10) ||
            '                mc.get_measure_descriptions(measure_name);' || chr(10) ||
            '            ' || chr(10) ||
            '            -- check if this measure should be converted' || chr(10) ||
            '            SELECT COUNT(*) INTO convert_measure' || chr(10) ||
            '            FROM   TABLE(measure_units)' || chr(10) ||
            '            WHERE  UPPER(measure_name) = UPPER(measure_list(i));' || chr(10) ||
            '            ' || chr(10) ||
            '            IF convert_measure = 1 THEN' || chr(10) ||
            '                -- get the target unit and the units cube used for conversion' || chr(10) ||
            '                -- the following raised a generic error!' || chr(10) ||
            '                -- SELECT measure_unit, conversion_rule INTO unit_any, units_cube_any' || chr(10) ||
            '                -- FROM   TABLE(measure_units)' || chr(10) ||
            '                -- WHERE  UPPER(measure_name) = UPPER(measure_list(i));' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the target unit and the units cube used for conversion' || chr(10) ||
            '                j := measure_units.FIRST;' || chr(10) ||
            '                WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                    IF UPPER(measure_units(j).measure_name) = UPPER(measure_list(i)) THEN' || chr(10) ||
            '                        unit_any := measure_units(j).measure_unit;' || chr(10) ||
            '                        units_cube_any := measure_units(j).conversion_rule;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        j := NULL;' || chr(10) ||
            '                    ELSE' || chr(10) ||
            '                        j := measure_units.NEXT(j);' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the references' || chr(10) ||
            '                status := ANYDATA.getRef(unit_any, unit_ref);' || chr(10) ||
            '                status := ANYDATA.getRef(units_cube_any, units_cube_ref);' || chr(10)||
            '                ' || chr(10) ||
            '                -- select the objects' || chr(10) ||
            '                utl_ref.select_object(unit_ref, unit_obj);' || chr(10) ||
            '                utl_ref.select_object(units_cube_ref, units_cube_obj);' || chr(10) ||
            '                ' || chr(10) ||
            '                units_cname := units_cube_obj.cname;' || chr(10) ||
            '                ' || chr(10) ||
            '                dim_ref := unit_obj.dim;' || chr(10) ||
            '                utl_ref.select_object(dim_ref, dim_obj);' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            BEGIN' || chr(10) ||
            '                SELECT DISTINCT ANYDATA.accessVarchar2(meta.measure_value) INTO aggregation_function' || chr(10) ||
            '                FROM   ' || mrel_table || ' mr, TABLE(mr.measure_metadata) meta' || chr(10) ||
            '                WHERE  UPPER(meta.measure_name) = UPPER(measure_list(i)) AND meta.metalevel = ''function'';' || chr(10) ||
            '            EXCEPTION' || chr(10) ||
            '                WHEN NO_DATA_FOUND THEN' || chr(10) ||
            '                    aggregation_function := ''SUM'';' || chr(10) ||
            '            END;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF convert_measure = 0 THEN' || chr(10) ||
            '                select_rollup_select := aggregation_function || ''(m.'' || measure_name || '') AS '' || measure_name;' || chr(10) ||
            '            ELSE' || chr(10) ||
            '                -- if the measure should be converted, multiply with a factor' || chr(10) ||
            '                select_rollup_select := aggregation_function || ''(m.'' || measure_name || '' * r.'' || unit_obj.oname || '') AS '' || measure_name;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            j := measure_descriptions.FIRST;' || chr(10) ||
            '            WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                IF j > 1 THEN' || chr(10) ||
            '                    select_rollup := select_rollup || chr(10) || '' UNION '' || chr(10) || chr(10);' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                select_rollup_from := NULL;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- ' || chr(10) ||
            '                measure_table := measure_descriptions(j).table_name;' || chr(10) ||
            '                ' || chr(10) ||
                             queryview_rollup2_from1 ||
            '                ' || chr(10) ||
            '                IF convert_measure > 0 THEN' || chr(10) ||
            '                    conlevel_string := measure_descriptions(j).measure_level.to_string2();' || chr(10) ||
            '                    rollup_table := rollup_tables_by_cube(units_cname)(conlevel_string);' || chr(10) ||
            '                    ' || chr(10) ||
            '                    -- ' || chr(10) ||
            '                    select_rollup_from1 := ' || chr(10) ||
            '                        ''            '' || rollup_table || '' r,'' || chr(10);' || chr(10) ||
            '                    ' || chr(10) ||
            '                    -- get the dimension names of the units cube' || chr(10) ||
            '                    units_cube_dimension_names :=' || chr(10) ||
            '                        units_cube_obj.get_dimension_names();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    k := units_cube_dimension_names.FIRST;' || chr(10) ||
            '                    WHILE k IS NOT NULL LOOP' || chr(10) ||
            '                        IF units_cube_dimension_names(k) = dim_obj.dname THEN' || chr(10) ||
            '                            select_rollup_where := select_rollup_where ||' || chr(10) ||
            '                                '' AND UPPER(m.'' || measure_name || ''_unit) = UPPER(r.'' || dim_obj.dname || '')'';' || chr(10) ||
                                     queryview_rollup2_from2 ||
            '                        END IF;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        k := units_cube_dimension_names.NEXT(k);' || chr(10) ||
            '                    END LOOP;' || chr(10) ||
            '                    ' || chr(10) ||
            '                ELSE' || chr(10) ||
            '                    select_rollup_from1 := NULL;' || chr(10) ||
            '                    select_rollup_where := NULL;' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                select_rollup := select_rollup || ' || chr(10) ||
            '                    ''  (SELECT   ' || queryview_rollup2_select || ''' || select_rollup_select || chr(10) ||' || chr(10) ||
            '                    ''   FROM '' || chr(10) || select_rollup_from || chr(10) ||' || chr(10) ||
            '                                  select_rollup_from1 || ' || chr(10) ||
            '                    ''            '' || measure_table || '' m'' || chr(10) ||' || chr(10) ||
            '                    ''   WHERE    ' || queryview_rollup2_where || ' AND m.mrel IN (SELECT * FROM TABLE(:mrelationship_set))'' || select_rollup_where || chr(10) ||' || chr(10) ||
            '                    ''   GROUP BY ' || queryview_rollup2_group_by || ')'' || chr(10);' || chr(10) ||
            '                ' || chr(10) ||
            '                j := measure_descriptions.NEXT(j);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_rollup_select := aggregation_function || ''('' || measure_name || '') AS '' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_rollup := ' || chr(10) ||
            '                ''SELECT ' || queryview_rollup2_select_union || ''' || select_rollup_select || chr(10) ||' || chr(10) ||
            '                ''FROM ('' || chr(10) ||' || chr(10) ||
            '                   select_rollup ||' || chr(10) ||
            '                '') GROUP BY ' || queryview_rollup2_grp_by_union || ''' || chr(10);' || chr(10) ||
            '            ' || chr(10) ||
            '            select_measures(measure_name) := select_rollup;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            measure_name := measure_list(i);' || chr(10) ||
            '            ' || chr(10) ||
            '            IF i > 1 THEN' || chr(10) ||
            '                select_join := select_join || chr(10) || chr(10) || '' FULL OUTER JOIN '' || chr(10) || chr(10);' || chr(10) ||
            '                create_table1 := create_table1 || '', '' || chr(10);' || chr(10) ||
            '                select_join_select_measures := select_join_select_measures || '', '';' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_descriptions :=' || chr(10) ||
            '                mc.get_measure_descriptions(measure_name);' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_datatype := ' || chr(10) ||
            '                measure_descriptions(measure_descriptions.FIRST).data_type;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- TODO: Make sure VARCHAR2 works as a data type' || chr(10) ||
            '            create_table1 :=  create_table1 || ''    '' || measure_name || '' '' || measure_datatype;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- find out if the data type of the measure is a collection' || chr(10) ||
            '            BEGIN' || chr(10) ||
            '                SELECT ut.typecode INTO typecode' || chr(10) ||
            '                FROM   user_types ut' || chr(10) ||
            '                WHERE  ut.type_name = UPPER(measure_datatype);' || chr(10) ||
            '            EXCEPTION' || chr(10) ||
            '                WHEN NO_DATA_FOUND THEN' || chr(10) ||
            '                    typecode := ''BUILT_IN'';' || chr(10) ||
            '            END;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF typecode = ''COLLECTION'' THEN ' || chr(10) ||
            '                nested_table_name1 := table_name || ''_'' || measure_name;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get a unique table name that is at most 30 bytes long.' || chr(10) ||
            '                nested_table_name := ' || chr(10) ||
            '                    identifiers.get_unique_short_name(30,' || chr(10) ||
            '                                                      UPPER(nested_table_name1),' || chr(10) ||
            '                                                      ''user_tab_columns'',' || chr(10) ||
            '                                                      ''table_name'');' || chr(10) ||
            '                ' || chr(10) ||
            '                create_table2 := create_table2 ||' || chr(10) ||
            '                    ''NESTED TABLE '' || measure_name || '' STORE AS '' || nested_table_name || chr(10);' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_join_select_measures := select_join_select_measures || measure_name || ''.'' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_join := select_join || '' ('' || select_measures(measure_name) || '') '' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF i > 1 THEN' || chr(10) ||
            '                select_join := select_join || '' ON ('' ' || queryview_rollup2_join_attr || ' || '')'';' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        create_table := ' || chr(10) ||
            '            ''CREATE TABLE '' || table_name || ''('' || chr(10) ||' || chr(10) ||
                             queryview_rollup2_create_tab ||
            '                create_table1 || chr(10) ||' || chr(10) ||
            '            '') '' || chr(10) || create_table2;' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_output.put_line(create_table);' || chr(10) ||
            '        EXECUTE IMMEDIATE create_table;' || chr(10) ||
            '        ' || chr(10) ||
            '        select_join := ''SELECT '' || ' || queryview_rollup2_join_select || ' select_join_select_measures || chr(10) ||' || chr(10) ||
            '                       ''FROM   '' || chr(10) || select_join;' || chr(10) ||
            '        ' || chr(10) ||
            '        --dbms_output.put_line(select_join);' || chr(10) ||
            '        ' || chr(10) ||
            '        insert_data := ' || chr(10) ||
            '            ''INSERT INTO '' || table_name || '' ('' || select_join || '')'';' || chr(10) ||
            '        ' || chr(10) ||
            '        --dbms_output.put_line(insert_data);' || chr(10) ||
            '        ' || chr(10) ||
            '        sql_cursor := dbms_sql.open_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_sql.parse(sql_cursor,' || chr(10) ||
            '                       insert_data,' || chr(10) ||
            '                       dbms_sql.native);' || chr(10) ||
            '        ' || chr(10) ||
                     queryview_rollup2_bind_var ||
            '        ' || chr(10) ||
            '        dbms_sql.bind_variable(sql_cursor, ''mrelationship_set'', SELF.mrelationship_set);' || chr(10) ||
            '        ' || chr(10) ||
            '        rows_processed := dbms_sql.execute(sql_cursor);' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_sql.close_cursor(sql_cursor);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- drop the rollup tables' || chr(10) ||
            '        units_cname := rollup_tables_by_cube.FIRST;' || chr(10) ||
            '        WHILE units_cname IS NOT NULL LOOP' || chr(10) ||
            '            conlevel_string := rollup_tables_by_cube(units_cname).FIRST;' || chr(10) ||
            '            WHILE conlevel_string IS NOT NULL LOOP' || chr(10) ||
            '                EXECUTE IMMEDIATE' || chr(10) ||
            '                    ''DROP TABLE '' || rollup_tables_by_cube(units_cname)(conlevel_string);' || chr(10) ||
            '                ' || chr(10) ||
            '                --dbms_output.put_line(''DROP TABLE '' || rollup_tables_by_cube(units_cname)(conlevel_string));' || chr(10) ||
            '                ' || chr(10) ||
            '                conlevel_string := rollup_tables_by_cube(units_cname).NEXT(conlevel_string);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '            ' || chr(10) ||
            '            units_cname := rollup_tables_by_cube.NEXT(units_cname);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '    END;' || chr(10);
        
        queryview_rollup :=
            '    MEMBER PROCEDURE rollup(table_name VARCHAR2, include_non_dimension_attr BOOLEAN, ' || queryview_rollup1 || ') IS' || chr(10) ||
            '        ' || chr(10) ||
            '        mc ' || mcube_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        TYPE string_tty IS TABLE OF VARCHAR2(10000) INDEX BY VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_measures string_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        create_table  VARCHAR2(2000);' || chr(10) ||
            '        create_table1 VARCHAR2(1000);' || chr(10) ||
            '        create_table2 VARCHAR2(2000);' || chr(10) ||
            '        insert_data   VARCHAR2(30000);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_join                 VARCHAR2(20000);' || chr(10) ||
            '        select_join_select_dim      VARCHAR2(5000);' || chr(10) ||
            '        select_join_select_measures VARCHAR2(5000);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_rollup        VARCHAR2(10000);' || chr(10) ||
            '        select_rollup_from   VARCHAR2(5000);' || chr(10) ||
            '        select_rollup_select VARCHAR2(4000);' || chr(10) ||
            '        ' || chr(10) ||
                     queryview_rollup_var_decl ||
            '        ' || chr(10) ||
                     queryview_rollup_join_sel_var ||
            '        ' || chr(10) ||
            '        measure_list names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_name         VARCHAR2(30);' || chr(10) ||
            '        measure_descriptions ' || measure_#_tty || ';' || chr(10) ||
            '        measure_table        VARCHAR2(30);' || chr(10) ||
            '        measure_datatype     VARCHAR2(30);' || chr(10) ||
            '        typecode             VARCHAR2(100);' || chr(10) ||
            '        ' || chr(10) ||
            '        nested_table_name  VARCHAR2(30);' || chr(10) ||
            '        nested_table_name1 VARCHAR2(60);' || chr(10) ||
            '        ' || chr(10) ||
            '        aggregation_function VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_level_pos INTEGER;' || chr(10) ||
            '        rollup_level_pos INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '        j INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        sql_cursor INTEGER;' || chr(10) ||
            '        rows_processed INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        utl_ref.select_object(SELF.mcube, mc);' || chr(10) ||
            '        ' || chr(10) ||
                     queryview_rollup_from || chr(10) ||
            '        ' || chr(10) ||
            '        -- get all measures from the query view' || chr(10) ||
            '        measure_list := SELF.measure_set;' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.LAST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            IF i = measure_list.LAST THEN' || chr(10) ||
                             queryview_rollup_join_select1 ||
            '            ELSE' || chr(10) ||
                             queryview_rollup_join_select2 ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.PRIOR(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            -- reset' || chr(10) ||
            '            select_rollup := NULL;' || chr(10) ||
            '            select_rollup_select := NULL;' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_name := measure_list(i);' || chr(10) ||
            '            measure_descriptions :=' || chr(10) ||
            '                mc.get_measure_descriptions(measure_name);' || chr(10) ||
            '            ' || chr(10) ||
            '            BEGIN' || chr(10) ||
            '                SELECT DISTINCT ANYDATA.accessVarchar2(meta.measure_value) INTO aggregation_function' || chr(10) ||
            '                FROM   ' || mrel_table || ' mr, TABLE(mr.measure_metadata) meta' || chr(10) ||
            '                WHERE  UPPER(meta.measure_name) = UPPER(measure_list(i)) AND meta.metalevel = ''function'';' || chr(10) ||
            '            EXCEPTION' || chr(10) ||
            '                WHEN NO_DATA_FOUND THEN' || chr(10) ||
            '                    aggregation_function := ''SUM'';' || chr(10) ||
            '            END;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_rollup_select := aggregation_function || ''(m.'' || measure_name || '') AS '' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            j := measure_descriptions.FIRST;' || chr(10) ||
            '            WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                IF j > 1 THEN' || chr(10) ||
            '                    select_rollup := select_rollup || chr(10) || '' UNION '' || chr(10) || chr(10);' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                select_rollup_from := NULL;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- ' || chr(10) ||
            '                measure_table := measure_descriptions(j).table_name;' || chr(10) ||
            '                ' || chr(10) ||
                             queryview_rollup_from1 ||
            '                ' || chr(10) ||
            '                select_rollup := select_rollup || ' || chr(10) ||
            '                    ''  (SELECT   ' || queryview_rollup_select || ''' || select_rollup_select || chr(10) ||' || chr(10) ||
            '                    ''   FROM '' || chr(10) || select_rollup_from || chr(10) ||' || chr(10) ||
            '                    ''            '' || measure_table || '' m'' || chr(10) ||' || chr(10) ||
            '                    ''   WHERE    ' || queryview_rollup_where || ' AND m.mrel IN (SELECT * FROM TABLE(:mrelationship_set))'' || chr(10) ||' || chr(10) ||
            '                    ''   GROUP BY ' || queryview_rollup_group_by || ')'' || chr(10);' || chr(10) ||
            '                ' || chr(10) ||
            '                j := measure_descriptions.NEXT(j);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_rollup_select := aggregation_function || ''('' || measure_name || '') AS '' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_rollup := ' || chr(10) ||
            '                ''SELECT ' || queryview_rollup_select_union || ''' || select_rollup_select || chr(10) ||' || chr(10) ||
            '                ''FROM ('' || chr(10) ||' || chr(10) ||
            '                   select_rollup ||' || chr(10) ||
            '                '') GROUP BY ' || queryview_rollup_grp_by_union || ''' || chr(10);' || chr(10) ||
            '            ' || chr(10) ||
            '            select_measures(measure_name) := select_rollup;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            measure_name := measure_list(i);' || chr(10) ||
            '            ' || chr(10) ||
            '            IF i > 1 THEN' || chr(10) ||
            '                select_join := select_join || chr(10) || chr(10) || '' FULL OUTER JOIN '' || chr(10) || chr(10);' || chr(10) ||
            '                create_table1 := create_table1 || '', '' || chr(10);' || chr(10) ||
            '                select_join_select_measures := select_join_select_measures || '', '';' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_descriptions :=' || chr(10) ||
            '                mc.get_measure_descriptions(measure_name);' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_datatype := ' || chr(10) ||
            '                measure_descriptions(measure_descriptions.FIRST).data_type;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- TODO: Make sure VARCHAR2 works as a data type' || chr(10) ||
            '            create_table1 :=  create_table1 || ''    '' || measure_name || '' '' || measure_datatype;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- find out if the data type of the measure is a collection' || chr(10) ||
            '            BEGIN' || chr(10) ||
            '                SELECT ut.typecode INTO typecode' || chr(10) ||
            '                FROM   user_types ut' || chr(10) ||
            '                WHERE  ut.type_name = UPPER(measure_datatype);' || chr(10) ||
            '            EXCEPTION' || chr(10) ||
            '                WHEN NO_DATA_FOUND THEN' || chr(10) ||
            '                    typecode := ''BUILT_IN'';' || chr(10) ||
            '            END;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF typecode = ''COLLECTION'' THEN ' || chr(10) ||
            '                nested_table_name1 := table_name || ''_'' || measure_name;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get a unique table name that is at most 30 bytes long.' || chr(10) ||
            '                nested_table_name := ' || chr(10) ||
            '                    identifiers.get_unique_short_name(30,' || chr(10) ||
            '                                                      UPPER(nested_table_name1),' || chr(10) ||
            '                                                      ''user_tab_columns'',' || chr(10) ||
            '                                                      ''table_name'');' || chr(10) ||
            '                ' || chr(10) ||
            '                create_table2 := create_table2 ||' || chr(10) ||
            '                    ''NESTED TABLE '' || measure_name || '' STORE AS '' || nested_table_name || chr(10);' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_join_select_measures := select_join_select_measures || measure_name || ''.'' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_join := select_join || '' ('' || select_measures(measure_name) || '') '' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF i > 1 THEN' || chr(10) ||
            '                select_join := select_join || '' ON ('' ' || queryview_rollup_join_attr || ' || '')'';' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        create_table := ' || chr(10) ||
            '            ''CREATE TABLE '' || table_name || ''('' || chr(10) ||' || chr(10) ||
                             queryview_rollup_create_tab ||
            '                create_table1 || chr(10) ||' || chr(10) ||
            '            '') '' || chr(10) || create_table2;' || chr(10) ||
            '        ' || chr(10) ||
            '        --dbms_output.put_line(create_table);' || chr(10) ||
            '        EXECUTE IMMEDIATE create_table;' || chr(10) ||
            '        ' || chr(10) ||
            '        select_join := ''SELECT '' || ' || queryview_rollup_join_select || ' select_join_select_measures || chr(10) ||' || chr(10) ||
            '                       ''FROM   '' || chr(10) || select_join;' || chr(10) ||
            '        ' || chr(10) ||
            '        --dbms_output.put_line(select_join);' || chr(10) ||
            '        ' || chr(10) ||
            '        insert_data := ' || chr(10) ||
            '            ''INSERT INTO '' || table_name || '' ('' || select_join || '')'';' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_output.put_line(insert_data);' || chr(10) ||
            '        ' || chr(10) ||
            '        sql_cursor := dbms_sql.open_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_sql.parse(sql_cursor,' || chr(10) ||
            '                       insert_data,' || chr(10) ||
            '                       dbms_sql.native);' || chr(10) ||
            '        ' || chr(10) ||
                     queryview_rollup_bind_var ||
            '        ' || chr(10) ||
            '        dbms_sql.bind_variable(sql_cursor, ''mrelationship_set'', SELF.mrelationship_set);' || chr(10) ||
            '        ' || chr(10) ||
            '        rows_processed := dbms_sql.execute(sql_cursor);' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_sql.close_cursor(sql_cursor);' || chr(10) ||
            '    EXCEPTION' || chr(10) ||
            '        WHEN OTHERS THEN EXECUTE IMMEDIATE ''DROP '' || table_name || '';'';' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10);
        
        queryview_body :=
            'CREATE OR REPLACE TYPE BODY ' || queryview_#_ty || ' IS' || chr(10) ||
                 queryview_constructor ||
                 queryview_dice ||
                 queryview_slice ||
                 queryview_project ||
                 queryview_eval ||
                 queryview_eval_dice ||
                 queryview_eval_slice ||
                 queryview_eval_project ||
                 queryview_rollup ||
                 queryview_rollup_unit_aware ||
            '    ' || chr(10) ||
            'END;';
        
        --dbms_output.put_line(queryview_body);
        EXECUTE IMMEDIATE queryview_body;
    END;
    
    PROCEDURE create_mcube_body(cname                 VARCHAR2,
                                mcube_#_ty            VARCHAR2,
                                mrel_#_ty             VARCHAR2,
                                mrel_#_trty           VARCHAR2,
                                mrel_table            VARCHAR2,
                                mrel_id_seq           VARCHAR2,
                                coordinate_#_ty       VARCHAR2,
                                coordinate_#_tty      VARCHAR2,
                                conlevel_#_ty         VARCHAR2,
                                conlevel_#_tty        VARCHAR2,
                                measure_#_ty          VARCHAR2,
                                measure_#_tty         VARCHAR2,
                                measure_meta_#_ty     VARCHAR2,
                                measure_meta_#_tty    VARCHAR2,
                                measure_table_#_ty    VARCHAR2,
                                measure_table_#_tty   VARCHAR2,
                                measure_#_collections VARCHAR2,
                                conlvl_ancestor_#_ty  VARCHAR2,
                                conlvl_ancestor_#_tty VARCHAR2,
                                queryview_#_ty        VARCHAR2,
                                expr_#_ty             VARCHAR2,
                                expr_#_tty            VARCHAR2,
                                dice_expr_#_ty        VARCHAR2,
                                slice_expr_#_ty       VARCHAR2,
                                project_expr_#_ty     VARCHAR2,
                                mrel_#_value_ty       VARCHAR2,
                                mrel_#_value_tty      VARCHAR2,
                                dimensions            names_tty,
                                dim_ids               names_tty,
                                dimension_#_ty        names_tty,
                                mobject_#_ty          names_tty,
                                mobject_tables        names_tty) IS
        
        mcube_body CLOB;
        
        mcube_constructor       CLOB;
        mcube_constr_dimensions CLOB;
        
        mcube_constructor_alt       CLOB;
        mcube_constructor_alt1      CLOB;
        mcube_constr_alt_dimensions CLOB;
        
        mcube_create_mrel       CLOB;
        mcube_create_mrel1      CLOB;
        mcube_create_mrel_coord CLOB;
        
        mcube_create2_mrel         CLOB;
        mcube_create2_mrel1        CLOB;
        mcube_create2_mrel_obj_def CLOB;
        mcube_create2_mrel_obj     CLOB;
        mcube_create2_mrel_coord   CLOB;
        
        mcube_create3_mrel CLOB;
        
        mcube_get_measure_descr CLOB;
        mcube_get_measure_descr1 CLOB;
        mcube_get_measure_descr_where CLOB;
        mcube_get_measure_descr_where1 CLOB;
        
        mcube_get_measure_descrs CLOB;
        
        mcube_get_measure_unit  CLOB;
        mcube_get_measure_unit1 CLOB;
        
        mcube_get_measure_funct CLOB;
        
        mcube_bulk_create_mrel       CLOB;
        mcube_bulk_create_mrel_where CLOB;
        mcube_get_mrel_ref   CLOB;
        mcube_get_mrel_ref1  CLOB;
        
        mcube_bulk_create_mrel_heu  CLOB;
        mcube_bulk_create_mrel_heu1 CLOB;
        
        mcube_get2_mrel_ref         CLOB;
        mcube_get2_mrel_ref1        CLOB;
        mcube_get2_mrel_ref2        CLOB;
        mcube_get2_mrel_ref_obj_def CLOB;
        mcube_get2_mrel_ref_obj     CLOB;
        
        mcube_get3_mrel_ref  CLOB;
        mcube_get3_mrel_ref1 CLOB;
        mcube_get3_mrel_ref2 CLOB;
        
        mcube_bulk_set_measure CLOB;
        mcube_bulk_set_measure_mrel CLOB;
        mcube_bulk_set_measure_mrel1 CLOB;
        mcube_bulk_set_measure_mrel2 CLOB;
        mcube_bulk_set_measure_mrel3 CLOB;
        mcube_bulk_set_measure_mrel4 CLOB;
        mcube_bulk_set_measure_mrel5 CLOB;
        
        mcube_bulk_set_measure_heu    CLOB;
        mcube_bulk_set_measure_heu1   CLOB;
        mcube_bulk_set_measure_heu_mr CLOB;
        
        mcube_refresh_meas_unit_cache CLOB;
        mcube_re_meas_unit_cache_where CLOB;
        mcube_re_meas_unit_cache_bind CLOB;
        
        mcube_new_queryview  CLOB;
        
        mcube_persist CLOB;
        
        mcube_get_dimension_names  CLOB;
        mcube_get_dimension_names1 CLOB;
        
        mcube_get_dimension_ids  CLOB;
        mcube_get_dimension_ids1 CLOB;
        
        mcube_export_star              CLOB;
        mcube_export_star1             CLOB;
        mcube_export_star_create_stmt1 CLOB;
        mcube_export_star_create_stmt2 CLOB;
        mcube_export_star_create_stmt3 CLOB;
        mcube_export_star_insert_stmt1 CLOB;
        mcube_export_star_insert_stmt2 CLOB;
        mcube_export_star_update_stmt1 CLOB;
        mcube_export_star_update_where CLOB;
        
        mcube_rollup              CLOB;
        mcube_rollup1             CLOB;
        mcube_rollup_var_decl     CLOB;
        mcube_rollup_select       CLOB;
        mcube_rollup_select_union CLOB;
        mcube_rollup_from         CLOB;
        mcube_rollup_from1        CLOB;
        mcube_rollup_where        CLOB;
        mcube_rollup_join_attr    CLOB;
        mcube_rollup_join_sel_var CLOB;
        mcube_rollup_join_select  CLOB;
        mcube_rollup_join_select1 CLOB;
        mcube_rollup_join_select2 CLOB;
        mcube_rollup_group_by     CLOB;
        mcube_rollup_grp_by_union CLOB;
        mcube_rollup_create_tab   CLOB;
        mcube_rollup_bind_var     CLOB;
        
        mcube_rollup_unit_aware    CLOB;
        mcube_rollup2_var_decl     CLOB;
        mcube_rollup2_select       CLOB;
        mcube_rollup2_select_union CLOB;
        mcube_rollup2_from         CLOB;
        mcube_rollup2_from1        CLOB;
        mcube_rollup2_from2        CLOB;
        mcube_rollup2_where        CLOB;
        mcube_rollup2_join_attr    CLOB;
        mcube_rollup2_join_sel_var CLOB;
        mcube_rollup2_join_select  CLOB;
        mcube_rollup2_join_select1 CLOB;
        mcube_rollup2_join_select2 CLOB;
        mcube_rollup2_group_by     CLOB;
        mcube_rollup2_grp_by_union CLOB;
        mcube_rollup2_create_tab   CLOB;
        mcube_rollup2_bind_var     CLOB;
        mcube_2rollup_elsifs       CLOB;
        
        mcube_get_nearest_mrel  CLOB;
        mcube_get_nearest_mrel1 CLOB;
        mcube_get_nearest_mrel2 CLOB;
        mcube_get_nearest_mrel3 CLOB;
        mcube_get_nearest_mrel4 CLOB;
        mcube_get_nearest_mrel5 CLOB;
        mcube_get_nearest_mrel6 CLOB;
        
        -- cursor variables
        i INTEGER;
    BEGIN        
        -- loop through the names of the dimensions
        i := dimensions.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                mcube_constructor_alt1 := mcube_constructor_alt1 || ', ';
                mcube_create_mrel1 := mcube_create_mrel1 || ', ';
                mcube_create_mrel_coord := mcube_create_mrel_coord || ', ';
                mcube_create2_mrel1 := mcube_create2_mrel1 || ', ';
                mcube_create2_mrel_coord := mcube_create2_mrel_coord || ', ';
                mcube_bulk_create_mrel_where := mcube_bulk_create_mrel_where || ' AND ';
                mcube_get_mrel_ref1 := mcube_get_mrel_ref1 || ', ';
                mcube_get2_mrel_ref1 := mcube_get2_mrel_ref1 || ', ';
                mcube_get2_mrel_ref2 := mcube_get2_mrel_ref2 || ', ';
                mcube_get3_mrel_ref1 := mcube_get3_mrel_ref1 || ', ';
                mcube_get3_mrel_ref2 := mcube_get3_mrel_ref2 || ' AND ';
                mcube_get_measure_descr1 := mcube_get_measure_descr1 || ', ';
                mcube_get_measure_unit1 := mcube_get_measure_unit1 || ', ';
                mcube_get_dimension_names1 := mcube_get_dimension_names1 || ', ';
                mcube_get_dimension_ids1 := mcube_get_dimension_ids1 || ', ';
                mcube_bulk_set_measure_heu1 := mcube_bulk_set_measure_heu1 || ', ';
                mcube_bulk_set_measure_heu_mr := mcube_bulk_set_measure_heu_mr || ', ';
                mcube_bulk_set_measure_mrel := mcube_bulk_set_measure_mrel || ', ';
                mcube_bulk_set_measure_mrel1 := mcube_bulk_set_measure_mrel1 || ' AND ';
                mcube_bulk_set_measure_mrel2 := mcube_bulk_set_measure_mrel2 || ' AND ';
                mcube_bulk_set_measure_mrel3 := mcube_bulk_set_measure_mrel3 || ', ';
                mcube_bulk_set_measure_mrel4 := mcube_bulk_set_measure_mrel4 || ' AND ';
                mcube_bulk_set_measure_mrel5 := mcube_bulk_set_measure_mrel5 || ', ';
                mcube_bulk_create_mrel_heu1 := mcube_bulk_create_mrel_heu1 || ', ';
                mcube_get_measure_descr_where := mcube_get_measure_descr_where || ' AND ';
                mcube_get_measure_descr_where1 := mcube_get_measure_descr_where1 || ' AND ';
                
                mcube_rollup1 := mcube_rollup1 || ', ';
                mcube_rollup_where := mcube_rollup_where || ' AND ';
                mcube_rollup_group_by := mcube_rollup_group_by || ', ';
                mcube_rollup_grp_by_union := mcube_rollup_grp_by_union || ', ';
                mcube_rollup_join_attr := mcube_rollup_join_attr || ' || '' AND ''';
                
                mcube_rollup2_where := mcube_rollup2_where || ' AND ';
                mcube_rollup2_group_by := mcube_rollup2_group_by || ', ';
                mcube_rollup2_grp_by_union := mcube_rollup2_grp_by_union || ', ';
                mcube_rollup2_join_attr := mcube_rollup2_join_attr || ' || '' AND ''';
                
                mcube_re_meas_unit_cache_where := mcube_re_meas_unit_cache_where || ' AND ';
                mcube_re_meas_unit_cache_bind := mcube_re_meas_unit_cache_bind || ', ';
                
                mcube_export_star1 := mcube_export_star1 || ', ';
                mcube_export_star_create_stmt1 := mcube_export_star_create_stmt1 || ' || chr(10) || ' || chr(10);
                mcube_export_star_create_stmt2 := mcube_export_star_create_stmt2 || ', ';
                mcube_export_star_create_stmt3 := mcube_export_star_create_stmt3 || ','' || chr(10) ||' || chr(10);
                mcube_export_star_insert_stmt1 := mcube_export_star_insert_stmt1 || ', ';
                mcube_export_star_insert_stmt2 := mcube_export_star_insert_stmt2 || ', ';
                mcube_export_star_update_stmt1 := mcube_export_star_update_stmt1 || ' AND ';
                mcube_export_star_update_where := mcube_export_star_update_where || ', ';
                
                mcube_get_nearest_mrel1 := mcube_get_nearest_mrel1 || ', ';
                mcube_get_nearest_mrel5 := mcube_get_nearest_mrel5 || ' AND ';
            END IF;
                
            mcube_re_meas_unit_cache_where := mcube_re_meas_unit_cache_where ||
                'r.coordinate.' || dim_ids(i) || '_oname = (:' || (i+1) || ').' || dim_ids(i) || '_oname';
            
            mcube_re_meas_unit_cache_bind := mcube_re_meas_unit_cache_bind ||
                'mrel_units(j).coordinate';
            
            mcube_constr_dimensions := mcube_constr_dimensions ||
                '        utl_ref.select_object(TREAT(root_coordinate.' || dim_ids(i) || '_obj AS REF mobject_ty), obj);' || chr(10) ||
                '        SELF.' || dimensions(i) || ' := TREAT(obj.dim AS REF ' || dimension_#_ty(i) || ');' || chr(10) ||
                '        ' || chr(10);
            
            mcube_constructor_alt1 := mcube_constructor_alt1 ||
                dimensions(i) || ' REF ' || dimension_#_ty(i);
            
            mcube_constr_alt_dimensions := mcube_constr_alt_dimensions ||
                '        SELF.' || dimensions(i) || ' := ' || dimensions(i) || ';' || chr(10) ||
                '        ' || chr(10);
            
            mcube_create_mrel1 := mcube_create_mrel1 ||
                dim_ids(i) || '_obj REF ' || mobject_#_ty(i);
            
            mcube_get_dimension_names1 := mcube_get_dimension_names1 ||
                '''' || dimensions(i) || '''';
            
            mcube_get_dimension_ids1 := mcube_get_dimension_ids1 ||
                '''' || dim_ids(i) || '''';
            
            mcube_create_mrel_coord := mcube_create_mrel_coord ||
                dim_ids(i) || '_obj';
            
            mcube_create2_mrel1 := mcube_create2_mrel1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            mcube_create2_mrel_obj_def := mcube_create2_mrel_obj_def ||
                '        ' || dim_ids(i) || '_obj REF ' || mobject_#_ty(i) || ';' || chr(10);
            
            mcube_create2_mrel_obj := mcube_create2_mrel_obj ||
                '        utl_ref.select_object(TREAT(SELF.' || dimensions(i) || ' AS REF dimension_ty), dim);' || chr(10) ||
                '        ' || dim_ids(i) || '_obj := TREAT(dim.get_mobject_ref(' || dim_ids(i) || '_oname) AS REF ' || mobject_#_ty(i) || ');' || chr(10) ||
                '        ' || chr(10);
            
            mcube_create2_mrel_coord := mcube_create2_mrel_coord ||
                dim_ids(i) || '_obj, ' || dim_ids(i) || '_oname';
            
            mcube_bulk_create_mrel_where := mcube_bulk_create_mrel_where ||
                'mr.coordinate.' || dim_ids(i) || '_oname = coordinates(i).' || dim_ids(i) || '_oname';
            
            mcube_get_mrel_ref1 := mcube_get_mrel_ref1 ||
                'coordinate.' || dim_ids(i) || '_oname';
            
            mcube_get2_mrel_ref1 := mcube_get2_mrel_ref1 ||
                dim_ids(i) || '_obj REF ' || mobject_#_ty(i);
            
            mcube_get2_mrel_ref2 := mcube_get2_mrel_ref2 ||
                dim_ids(i) || '_mobject.oname';
            
            mcube_get2_mrel_ref_obj_def := mcube_get2_mrel_ref_obj_def ||
                '        ' || dim_ids(i) || '_mobject ' || mobject_#_ty(i) || ';' || chr(10);
                
            mcube_get2_mrel_ref_obj := mcube_get2_mrel_ref_obj ||
                '        utl_ref.select_object(' || dim_ids(i) || '_obj, ' || dim_ids(i) || '_mobject);' || chr(10);
            
            mcube_get3_mrel_ref1 := mcube_get3_mrel_ref1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            mcube_get3_mrel_ref2 := mcube_get3_mrel_ref2 ||
                'mr.coordinate.' || dim_ids(i) || '_oname = ' || dim_ids(i) || '_oname';
            
            mcube_get_measure_descr1 := mcube_get_measure_descr1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            mcube_get_measure_descr_where := mcube_get_measure_descr_where ||
                'm.coordinate.' || dim_ids(i) || '_oname = ' || dim_ids(i) || '_oname';
            
            mcube_get_measure_descr_where1 := mcube_get_measure_descr_where1 ||
                'x.coordinate.' || dim_ids(i) || '_oname = ' || dim_ids(i) || '_oname';
            
            mcube_get_measure_unit1 := mcube_get_measure_unit1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            mcube_bulk_set_measure_heu1 := mcube_bulk_set_measure_heu1 ||
                dim_ids(i) || '_oname VARCHAR2';
                
            mcube_bulk_set_measure_heu_mr := mcube_bulk_set_measure_heu_mr ||
                dim_ids(i) || '_oname';
                
            mcube_bulk_set_measure_mrel := mcube_bulk_set_measure_mrel ||
                'measure_values(i).' || dim_ids(i) || '_oname';
            
            mcube_bulk_set_measure_mrel1 := mcube_bulk_set_measure_mrel1 ||
                'mr.coordinate.' || dim_ids(i) || '_oname = :' || i;
            
            mcube_bulk_set_measure_mrel2 := mcube_bulk_set_measure_mrel2 ||
                't.coordinate.' || dim_ids(i) || '_oname = mr.coordinate.' || dim_ids(i) || '_oname';
            
            --mcube_bulk_set_measure_mrel3 := mcube_bulk_set_measure_mrel3 ||
            --    'measure_tables(table_name)(i).' || dim_ids(i) || '_oname';
            mcube_bulk_set_measure_mrel3 := mcube_bulk_set_measure_mrel3 ||
                'measure_values(i).' || dim_ids(i) || '_oname';
                
            mcube_bulk_set_measure_mrel4 := mcube_bulk_set_measure_mrel4 ||
                't.coordinate.' || dim_ids(i) || '_oname = :' || i;
            
            mcube_bulk_set_measure_mrel5 := mcube_bulk_set_measure_mrel5 ||
                'measure_values(i).' || dim_ids(i) || '_oname';
            
            mcube_bulk_create_mrel_heu1 := mcube_bulk_create_mrel_heu1 ||
                dim_ids(i) || '_oname VARCHAR2';
            
            mcube_rollup1 := mcube_rollup1 ||
                dim_ids(i) || '_level VARCHAR2';
            
            mcube_rollup_select := mcube_rollup_select ||
                dim_ids(i) || '_anc.ancestor AS ' || dim_ids(i) || ', ';
            
            mcube_rollup_select_union := mcube_rollup_select_union ||
                dim_ids(i) || ', ';
            
            mcube_rollup_group_by := mcube_rollup_group_by ||
                dim_ids(i) || '_anc.ancestor';
            
            mcube_rollup_grp_by_union := mcube_rollup_grp_by_union ||
                dim_ids(i);
            
            mcube_rollup_var_decl := mcube_rollup_var_decl ||
                '        select_subquery1_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        select_subquery2_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        select_subquery3_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        ' || chr(10) ||
                '        ' || dim_ids(i) || '_dname VARCHAR2(30) := ''' || dimensions(i) || ''';' || chr(10) ||
                '        ' || chr(10);
            
            mcube_rollup_from := mcube_rollup_from || 
                '        select_subquery1_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, anc.ancestor.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) anc'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  anc.lvl = :' || dim_ids(i) || '_level'' || chr(10) ||' || chr(10) ||
                '            ''                 UNION'' || chr(10) ||' || chr(10) ||
                '            ''             SELECT o.oname AS oname, ''''Other: '''' || anc.ancestor.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) anc'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  NOT EXISTS(SELECT * FROM TABLE(o.ancestors) WHERE lvl = :' || dim_ids(i) || '_level) AND'' || chr(10) ||' || chr(10) ||
                '            ''                    ('' || chr(10) ||' || chr(10) ||
                '            ''                     ('' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.parents)) = 1 AND'' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT p.COLUMN_VALUE.oname FROM TABLE(o.parents) p) = anc.ancestor.oname'' || chr(10) ||' || chr(10) ||
                '            ''                     ) OR'' || chr(10) ||' || chr(10) ||
                '            ''                     ('' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.parents)) > 1 AND'' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.dim.level_positions) p WHERE p.position = (SELECT p.position FROM TABLE(o.dim.level_positions) p WHERE p.lvl = anc.ancestor.top_level)) = 1'' || chr(10) ||' || chr(10) ||
                '            ''                     )'' || chr(10) ||' || chr(10) ||
                '            ''                    )'' || chr(10) ||' || chr(10) ||
                '            ''            ) ' || dim_ids(i) || '_anc, '' || chr(10);' || chr(10) ||
                '        ' || chr(10) ||
                '        select_subquery2_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, o.oname || '''' ('''' || o.top_level || '''')'''' AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  NOT EXISTS(SELECT * FROM TABLE(o.ancestors) WHERE lvl = :' || dim_ids(i) || '_level) AND'' || chr(10) ||' || chr(10) ||
                '            ''                    NOT EXISTS(SELECT * FROM TABLE(o.level_hierarchy) WHERE lvl = :' || dim_ids(i) || '_level)) ' || dim_ids(i) || '_anc, '' || chr(10); ' || chr(10) ||
                '        ' || chr(10) ||
                '        select_subquery3_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, o.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  o.top_level = :' || dim_ids(i) || '_level) ' || dim_ids(i) || '_anc, '' || chr(10); ' || chr(10) ||
                '        ' || chr(10);
            
            mcube_rollup_from1 := mcube_rollup_from1 ||
                '                ' || chr(10) ||
                '                SELECT p.position INTO measure_level_pos' || chr(10) ||
                '                FROM   dimensions d, TABLE(d.level_positions) p' || chr(10) ||
                '                WHERE  d.dname = ' || dim_ids(i) || '_dname AND' || chr(10) ||
                '                       p.lvl = measure_descriptions(j).measure_level.' || dim_ids(i) || '_level;' || chr(10) ||
                '                ' || chr(10) ||
                '                SELECT p.position INTO rollup_level_pos' || chr(10) ||
                '                FROM   dimensions d, TABLE(d.level_positions) p' || chr(10) ||
                '                WHERE  d.dname = ' || dim_ids(i) || '_dname AND' || chr(10) ||
                '                       p.lvl = ' || dim_ids(i) || '_level;' || chr(10) ||
                '                ' || chr(10) ||
                '                IF measure_descriptions(j).measure_level.' || dim_ids(i) || '_level <> ' || dim_ids(i) || '_level AND ' || chr(10) ||
                '                   measure_level_pos >= rollup_level_pos THEN' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery1_' || dim_ids(i) || ';' || chr(10) ||
                '                ELSIF measure_descriptions(j).measure_level.' || dim_ids(i) || '_level <> ' || dim_ids(i) || '_level AND ' || chr(10) ||
                '                   measure_level_pos < rollup_level_pos THEN' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery2_' || dim_ids(i) || ';' || chr(10) ||
                '                ELSE' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery3_' || dim_ids(i) || ';' || chr(10) ||
                '                END IF;' || chr(10) ||
                '                ' || chr(10);
            
            mcube_rollup_where := mcube_rollup_where ||
                'm.coordinate.' || dim_ids(i) || '_oname = ' || dim_ids(i) || '_anc.oname';
            
            mcube_rollup_join_attr := mcube_rollup_join_attr ||
                '|| measure_name || ''.' || dim_ids(i) || ' = '' || measure_list(measure_list.PRIOR(i)) || ''.' || dim_ids(i) || '''';
            
            mcube_rollup_create_tab := mcube_rollup_create_tab ||
                '            ''    ' || dimensions(i) || ' VARCHAR2(63),'' || chr(10) ||' || chr(10);
            
            mcube_rollup_bind_var := mcube_rollup_bind_var ||
                '        dbms_sql.bind_variable(sql_cursor, ''' || dim_ids(i) || '_level'', ' || dim_ids(i) || '_level);' || chr(10);
            
            mcube_rollup_join_sel_var := mcube_rollup_join_sel_var ||
                '        select_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10);
            
            mcube_rollup_join_select1 := mcube_rollup_join_select1 ||
                '                select_' || dim_ids(i) || ' := measure_list(i) || ''.' || dim_ids(i) || ''' ;' || chr(10);
            
            mcube_rollup_join_select2 := mcube_rollup_join_select2 ||
                '                select_' || dim_ids(i) || ' := ' || chr(10) ||
                '                    ''NVL('' || measure_list(i) || ''.' || dim_ids(i) || ', '' || select_' || dim_ids(i) || ' || '')'';' || chr(10);
            
            mcube_rollup_join_select := mcube_rollup_join_select ||
                'select_' || dim_ids(i) || ' || '', '' ||';
            
            
            /*****/
            
            mcube_rollup2_select := mcube_rollup2_select ||
                dim_ids(i) || '_anc.ancestor AS ' || dim_ids(i) || ', ';
            
            mcube_rollup2_select_union := mcube_rollup2_select_union ||
                dim_ids(i) || ', ';
            
            mcube_rollup2_group_by := mcube_rollup2_group_by ||
                dim_ids(i) || '_anc.ancestor';
            
            mcube_rollup2_grp_by_union := mcube_rollup2_grp_by_union ||
                dim_ids(i);
            
            mcube_rollup2_var_decl := mcube_rollup2_var_decl ||
                '        select_subquery1_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        select_subquery2_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        select_subquery3_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10) ||
                '        ' || chr(10) ||
                '        ' || dim_ids(i) || '_dname VARCHAR2(30) := ''' || dimensions(i) || ''';' || chr(10) ||
                '        ' || chr(10);
            
            mcube_rollup2_from := mcube_rollup2_from || 
                '        select_subquery1_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, anc.ancestor.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) anc'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  anc.lvl = :' || dim_ids(i) || '_level'' || chr(10) ||' || chr(10) ||
                '            ''                 UNION'' || chr(10) ||' || chr(10) ||
                '            ''             SELECT o.oname AS oname, ''''Other: '''' || anc.ancestor.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) anc'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  NOT EXISTS(SELECT * FROM TABLE(o.ancestors) WHERE lvl = :' || dim_ids(i) || '_level) AND'' || chr(10) ||' || chr(10) ||
                '            ''                    ('' || chr(10) ||' || chr(10) ||
                '            ''                     ('' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.parents)) = 1 AND'' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT p.COLUMN_VALUE.oname FROM TABLE(o.parents) p) = anc.ancestor.oname'' || chr(10) ||' || chr(10) ||
                '            ''                     ) OR'' || chr(10) ||' || chr(10) ||
                '            ''                     ('' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.parents)) > 1 AND'' || chr(10) ||' || chr(10) ||
                '            ''                      (SELECT COUNT(*) FROM TABLE(o.dim.level_positions) p WHERE p.position = (SELECT p.position FROM TABLE(o.dim.level_positions) p WHERE p.lvl = anc.ancestor.top_level)) = 1'' || chr(10) ||' || chr(10) ||
                '            ''                     )'' || chr(10) ||' || chr(10) ||
                '            ''                    )'' || chr(10) ||' || chr(10) ||
                '            ''            ) ' || dim_ids(i) || '_anc, '' || chr(10);' || chr(10) ||
                '        ' || chr(10) ||
                '        select_subquery2_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, o.oname || '''' ('''' || o.top_level || '''')'''' AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  NOT EXISTS(SELECT * FROM TABLE(o.ancestors) WHERE lvl = :' || dim_ids(i) || '_level) AND'' || chr(10) ||' || chr(10) ||
                '            ''                    NOT EXISTS(SELECT * FROM TABLE(o.level_hierarchy) WHERE lvl = :' || dim_ids(i) || '_level)) ' || dim_ids(i) || '_anc, '' || chr(10); ' || chr(10) ||
                '        ' || chr(10) ||
                '        select_subquery3_' || dim_ids(i) || ' := ' || chr(10) ||
                '            ''            (SELECT o.oname AS oname, o.oname AS ancestor'' || chr(10) ||' || chr(10) ||
                '            ''             FROM   ' || mobject_tables(i) || ' o'' || chr(10) ||' || chr(10) ||
                '            ''             WHERE  o.top_level = :' || dim_ids(i) || '_level) ' || dim_ids(i) || '_anc, '' || chr(10); ' || chr(10) ||
                '        ' || chr(10);
            
            mcube_rollup2_from1 := mcube_rollup2_from1 ||
                '                ' || chr(10) ||
                '                SELECT p.position INTO measure_level_pos' || chr(10) ||
                '                FROM   dimensions d, TABLE(d.level_positions) p' || chr(10) ||
                '                WHERE  d.dname = ' || dim_ids(i) || '_dname AND' || chr(10) ||
                '                       p.lvl = measure_descriptions(j).measure_level.' || dim_ids(i) || '_level;' || chr(10) ||
                '                ' || chr(10) ||
                '                SELECT p.position INTO rollup_level_pos' || chr(10) ||
                '                FROM   dimensions d, TABLE(d.level_positions) p' || chr(10) ||
                '                WHERE  d.dname = ' || dim_ids(i) || '_dname AND' || chr(10) ||
                '                       p.lvl = ' || dim_ids(i) || '_level;' || chr(10) ||
                '                ' || chr(10) ||
                '                IF measure_descriptions(j).measure_level.' || dim_ids(i) || '_level <> ' || dim_ids(i) || '_level AND ' || chr(10) ||
                '                   measure_level_pos >= rollup_level_pos THEN' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery1_' || dim_ids(i) || ';' || chr(10) ||
                '                ELSIF measure_descriptions(j).measure_level.' || dim_ids(i) || '_level <> ' || dim_ids(i) || '_level AND ' || chr(10) ||
                '                   measure_level_pos < rollup_level_pos THEN' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery2_' || dim_ids(i) || ';' || chr(10) ||
                '                ELSE' || chr(10) ||
                '                    select_rollup_from := select_rollup_from || ' || chr(10) ||
                '                        select_subquery3_' || dim_ids(i) || ';' || chr(10) ||
                '                END IF;' || chr(10) ||
                '                ' || chr(10);
            
            mcube_rollup2_where := mcube_rollup2_where ||
                'm.coordinate.' || dim_ids(i) || '_oname = ' || dim_ids(i) || '_anc.oname';
            
            mcube_rollup2_join_attr := mcube_rollup2_join_attr ||
                '|| measure_name || ''.' || dim_ids(i) || ' = '' || measure_list(measure_list.PRIOR(i)) || ''.' || dim_ids(i) || '''';
            
            mcube_rollup2_create_tab := mcube_rollup2_create_tab ||
                '            ''    ' || dimensions(i) || ' VARCHAR2(63),'' || chr(10) ||' || chr(10);
            
            mcube_rollup2_bind_var := mcube_rollup2_bind_var ||
                '        dbms_sql.bind_variable(sql_cursor, ''' || dim_ids(i) || '_level'', ' || dim_ids(i) || '_level);' || chr(10);
            
            mcube_rollup2_join_sel_var := mcube_rollup2_join_sel_var ||
                '        select_' || dim_ids(i) || ' VARCHAR2(5000);' || chr(10);
            
            mcube_rollup2_join_select1 := mcube_rollup2_join_select1 ||
                '                select_' || dim_ids(i) || ' := measure_list(i) || ''.' || dim_ids(i) || ''' ;' || chr(10);
            
            mcube_rollup2_join_select2 := mcube_rollup2_join_select2 ||
                '                select_' || dim_ids(i) || ' := ' || chr(10) ||
                '                    ''NVL('' || measure_list(i) || ''.' || dim_ids(i) || ', '' || select_' || dim_ids(i) || ' || '')'';' || chr(10);
            
            mcube_rollup2_join_select := mcube_rollup2_join_select ||
                'select_' || dim_ids(i) || ' || '', '' ||';
            
            mcube_2rollup_elsifs := mcube_2rollup_elsifs ||
                '                            ELSIF units_cube_dimension_names(k) = ''' || dimensions(i) || ''' THEN' || chr(10) ||
                '                                rollup_table_args := rollup_table_args || ' || chr(10) ||
                '                                    '', '' ||  '''''''' || measure_descriptions(j).measure_level. ' || dim_ids(i) ||  '_level || '''''''';' || chr(10);
            
            
            mcube_export_star1 := mcube_export_star1 ||
                dim_ids(i) || '_star_table VARCHAR2';
            
            mcube_export_star_create_stmt1 := mcube_export_star_create_stmt1 ||
                '            ''    ' || dimensions(i) || ' VARCHAR2(30),''';
            
            mcube_export_star_create_stmt2 := mcube_export_star_create_stmt2 ||
                dimensions(i);
            
            mcube_export_star_create_stmt3 := mcube_export_star_create_stmt3 ||
                '            ''    FOREIGN KEY (' || dimensions(i) || ') REFERENCES '' || ' || dim_ids(i) || '_star_table || ''(id)';
            
            mcube_export_star_insert_stmt1 := mcube_export_star_insert_stmt1 ||
                dimensions(i);
            
            mcube_export_star_insert_stmt2 := mcube_export_star_insert_stmt2 ||
                'r.coordinate.' || dim_ids(i) || '_oname';
            
            mcube_export_star_update_stmt1 := mcube_export_star_update_stmt1 ||
                'm.coordinate.' || dim_ids(i) || '_oname = '' || table_name || ''.' || dimensions(i);
            
            mcube_export_star_update_where := mcube_export_star_update_where ||
                dimensions(i);
                
            mcube_get_nearest_mrel1 := mcube_get_nearest_mrel1 ||
                dim_ids(i) || '_oname VARCHAR2';
                
            mcube_get_nearest_mrel2 := mcube_get_nearest_mrel2 ||
                '        ' || dim_ids(i) || '_ancestors names_tty;' || chr(10) ||
                '        ' || dim_ids(i) || '_top_levels names_tty;' || chr(10) ||
                '        ' || dim_ids(i) || '_iterator INTEGER;' || chr(10) ||
                '        ' || chr(10);
                
            mcube_get_nearest_mrel3 := mcube_get_nearest_mrel3 ||
                '        SELECT DEREF(a.ancestor).oname, DEREF(o.dim).get_level_position(DEREF(a.ancestor).top_level) BULK COLLECT INTO ' || dim_ids(i) || '_ancestors, ' || dim_ids(i) || '_top_levels' || chr(10) ||
                '        FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) a' || chr(10) ||
                '        WHERE  o.oname = ' || dim_ids(i) || '_oname' || chr(10) ||
                '        ORDER BY DEREF(o.dim).get_level_position(DEREF(a.ancestor).top_level) ASC;' || chr(10) ||
                '        ' || chr(10) ||
                '        ' || dim_ids(i) || '_ancestors.EXTEND;' || chr(10) ||
                '        ' || dim_ids(i) || '_ancestors(' || dim_ids(i) || '_ancestors.LAST) := ' || dim_ids(i) || '_oname;' || chr(10) ||
                '        ' || chr(10);
            
            mcube_get_nearest_mrel4 := mcube_get_nearest_mrel4 ||
                '        ' || dim_ids(i) || '_iterator := ' || dim_ids(i) || '_ancestors.LAST;' || chr(10) ||
                '        WHILE ' || dim_ids(i) || '_iterator IS NOT NULL LOOP' || chr(10);
            
            mcube_get_nearest_mrel5 := mcube_get_nearest_mrel5 ||
                'r.coordinate.' || dim_ids(i) || '_oname = ' || dim_ids(i) || '_ancestors(' || dim_ids(i) || '_iterator)';
            
            mcube_get_nearest_mrel6 := 
                '            ' || dim_ids(i) || '_iterator := ' || dim_ids(i) || '_ancestors.PRIOR(' || dim_ids(i) || '_iterator);' || chr(10) ||
                '            EXIT WHEN cnt > 0;' || chr(10) ||
                '        END LOOP;' || chr(10) ||
                mcube_get_nearest_mrel6;
            
            -- increment cursor variable
            i := dimensions.NEXT(i);
        END LOOP;
        
        mcube_constructor :=
            '    CONSTRUCTOR FUNCTION ' || mcube_#_ty || '(cname VARCHAR2, id VARCHAR2, root_coordinate ' || coordinate_#_ty || ')' || chr(10) ||
            '        RETURN SELF AS RESULT IS' || chr(10) ||
            '        ' || chr(10) ||
            '        obj mobject_ty;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        SELF.cname := cname;' || chr(10) ||
            '        SELF.id := id;' || chr(10) ||
            '        SELF.root_coordinate := root_coordinate;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- store the names of the cube-specific object types' || chr(10) ||
            '        SELF.mcube_#_ty := ''' || mcube_#_ty || ''';' || chr(10) ||
            '        SELF.mrel_#_ty := ''' || mrel_#_ty || ''';' || chr(10) ||
            '        SELF.mrel_#_trty := ''' || mrel_#_trty || ''';' || chr(10) ||
            '        SELF.mrel_table := ''' || mrel_table || ''';' || chr(10) ||
            '        SELF.mrel_id_seq := ''' || mrel_id_seq || ''';' || chr(10) ||
            '        SELF.coordinate_#_ty := ''' || coordinate_#_ty || ''';' || chr(10) ||
            '        SELF.coordinate_#_tty := ''' || coordinate_#_tty || ''';' || chr(10) ||
            '        SELF.conlevel_#_ty := ''' || conlevel_#_ty || ''';' || chr(10) ||
            '        SELF.conlevel_#_tty := ''' || conlevel_#_tty || ''';' || chr(10) ||
            '        SELF.measure_#_ty := ''' || measure_#_ty || ''';' || chr(10) ||
            '        SELF.measure_#_tty := ''' || measure_#_tty || ''';' || chr(10) ||
            '        SELF.measure_meta_#_ty := ''' || measure_meta_#_ty || ''';' || chr(10) ||
            '        SELF.measure_meta_#_tty := ''' || measure_meta_#_tty || ''';' || chr(10) ||
            '        SELF.measure_table_#_ty := ''' || measure_table_#_ty || ''';' || chr(10) ||
            '        SELF.measure_table_#_tty := ''' || measure_table_#_tty || ''';' || chr(10) ||
            '        SELF.measure_#_collections := ''' || measure_#_collections || ''';' || chr(10) ||
            '        SELF.conlvl_ancestor_#_ty := ''' || conlvl_ancestor_#_ty || ''';' || chr(10) ||
            '        SELF.conlvl_ancestor_#_tty := ''' || conlvl_ancestor_#_tty || ''';' || chr(10) ||
            '        SELF.queryview_#_ty := ''' || queryview_#_ty || ''';' || chr(10) ||
            '        SELF.expr_#_ty := ''' || expr_#_ty || ''';' || chr(10) ||
            '        SELF.expr_#_tty := ''' || expr_#_tty || ''';' || chr(10) ||
            '        SELF.dice_expr_#_ty := ''' || dice_expr_#_ty || ''';' || chr(10) ||
            '        SELF.slice_expr_#_ty := ''' || slice_expr_#_ty || ''';' || chr(10) ||
            '        SELF.project_expr_#_ty := ''' || project_expr_#_ty || ''';' || chr(10) ||
            '        SELF.mrel_#_value_ty := ''' || mrel_#_value_ty || ''';' || chr(10) ||
            '        SELF.mrel_#_value_tty := ''' || mrel_#_value_tty || ''';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- by default, consistency is checked' || chr(10) || 
            '        SELF.enforce_consistency := 1;' || chr(10) || 
            '        ' || chr(10) ||
            '        -- by default, units are caches' || chr(10) || 
            '        SELF.enable_measure_unit_cache := 1;' || chr(10) || 
            '        ' || chr(10) || 
            '        -- get the dimension references from the root coordinate' || chr(10) ||
                     mcube_constr_dimensions ||
            '        RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
            
        mcube_constructor_alt :=
            '    CONSTRUCTOR FUNCTION ' || mcube_#_ty || '(cname VARCHAR2, id VARCHAR2, ' || mcube_constructor_alt1 || ')' || chr(10) ||
            '        RETURN SELF AS RESULT IS' || chr(10) ||
            '        ' || chr(10) ||
            '        obj mobject_ty;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        SELF.cname := cname;' || chr(10) ||
            '        SELF.id := id;' || chr(10) ||
            '        SELF.root_coordinate := NULL;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- store the names of the cube-specific object types' || chr(10) ||
            '        SELF.mcube_#_ty := ''' || mcube_#_ty || ''';' || chr(10) ||
            '        SELF.mrel_#_ty := ''' || mrel_#_ty || ''';' || chr(10) ||
            '        SELF.mrel_#_trty := ''' || mrel_#_trty || ''';' || chr(10) ||
            '        SELF.mrel_table := ''' || mrel_table || ''';' || chr(10) ||
            '        SELF.mrel_id_seq := ''' || mrel_id_seq || ''';' || chr(10) ||
            '        SELF.coordinate_#_ty := ''' || coordinate_#_ty || ''';' || chr(10) ||
            '        SELF.coordinate_#_tty := ''' || coordinate_#_tty || ''';' || chr(10) ||
            '        SELF.conlevel_#_ty := ''' || conlevel_#_ty || ''';' || chr(10) ||
            '        SELF.conlevel_#_tty := ''' || conlevel_#_tty || ''';' || chr(10) ||
            '        SELF.measure_#_ty := ''' || measure_#_ty || ''';' || chr(10) ||
            '        SELF.measure_#_tty := ''' || measure_#_tty || ''';' || chr(10) ||
            '        SELF.measure_meta_#_ty := ''' || measure_meta_#_ty || ''';' || chr(10) ||
            '        SELF.measure_meta_#_tty := ''' || measure_meta_#_tty || ''';' || chr(10) ||
            '        SELF.measure_table_#_ty := ''' || measure_table_#_ty || ''';' || chr(10) ||
            '        SELF.measure_table_#_tty := ''' || measure_table_#_tty || ''';' || chr(10) ||
            '        SELF.measure_#_collections := ''' || measure_#_collections || ''';' || chr(10) ||
            '        SELF.conlvl_ancestor_#_ty := ''' || conlvl_ancestor_#_ty || ''';' || chr(10) ||
            '        SELF.conlvl_ancestor_#_tty := ''' || conlvl_ancestor_#_tty || ''';' || chr(10) ||
            '        SELF.queryview_#_ty := ''' || queryview_#_ty || ''';' || chr(10) ||
            '        SELF.expr_#_ty := ''' || expr_#_ty || ''';' || chr(10) ||
            '        SELF.expr_#_tty := ''' || expr_#_tty || ''';' || chr(10) ||
            '        SELF.dice_expr_#_ty := ''' || dice_expr_#_ty || ''';' || chr(10) ||
            '        SELF.slice_expr_#_ty := ''' || slice_expr_#_ty || ''';' || chr(10) ||
            '        SELF.project_expr_#_ty := ''' || project_expr_#_ty || ''';' || chr(10) ||
            '        SELF.mrel_#_value_ty := ''' || mrel_#_value_ty || ''';' || chr(10) ||
            '        SELF.mrel_#_value_tty := ''' || mrel_#_value_tty || ''';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- by default, consistency is checked' || chr(10) || 
            '        SELF.enforce_consistency := 1;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- by default, units are caches' || chr(10) || 
            '        SELF.enable_measure_unit_cache := 1;' || chr(10) || 
            '        ' || chr(10) ||
            '        -- set the mcube''s dimension references' || chr(10) ||
                     mcube_constr_alt_dimensions ||
            '        RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_create_mrel :=
            '    MEMBER FUNCTION create_mrel(' || mcube_create_mrel1 || ') RETURN REF ' || mrel_#_ty || ' IS' || chr(10) ||
            '        new_mrel ' || mrel_#_ty || ';' || chr(10) ||
            '        new_mrel_ref REF ' || mrel_#_ty || ';' || chr(10) ||
            '        coordinate ' || coordinate_#_ty || ';' || chr(10) ||
            '        id VARCHAR2(10);' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- create a coordinate object' || chr(10) ||
            '        coordinate := ' || coordinate_#_ty || '(' || mcube_create_mrel_coord || ');' || chr(10) ||
            '        ' || chr(10) ||
            '        -- get an id for the coordinate.' || chr(10) ||
            '        id := ''r'' || ' || mrel_id_seq || '.NEXTVAL;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- create the m-relationship' || chr(10) ||
            '        new_mrel := ' || mrel_#_ty || '(coordinate, id);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- insert the new m-relationship into the m-relationship table' || chr(10) ||
            '        INSERT INTO ' || mrel_table || ' VALUES new_mrel;' || chr(10) ||
            '        ' || chr(10) ||
            '        new_mrel_ref := SELF.get_mrel_ref(coordinate);' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN new_mrel_ref;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_create2_mrel :=
            '    MEMBER FUNCTION create_mrel(' || mcube_create2_mrel1 || ') RETURN REF ' || mrel_#_ty || ' IS' || chr(10) ||
            '        new_mrel ' || mrel_#_ty || ';' || chr(10) ||
            '        new_mrel_ref REF ' || mrel_#_ty || ';' || chr(10) ||
            '        coordinate ' || coordinate_#_ty || ';' || chr(10) ||
            '        id VARCHAR2(10);' || chr(10) ||
            '        ' || chr(10) ||
            '        dim dimension_ty;' || chr(10) ||
                     mcube_create2_mrel_obj_def ||
            '    BEGIN' || chr(10) ||
                     mcube_create2_mrel_obj ||
            '        -- call the alternative constructor that is more efficient when the' || chr(10) ||
            '        -- the names are already known (needn''t retrieve twice).' || chr(10) ||
            '        coordinate := ' || coordinate_#_ty || '(' || mcube_create2_mrel_coord || ');' || chr(10) ||
            '        ' || chr(10) ||
            '        -- get an id for the coordinate.' || chr(10) ||
            '        id := ''r'' || ' || mrel_id_seq || '.NEXTVAL;' || chr(10) ||
            '        ' || chr(10) ||
            '        ' || chr(10) ||
            '        -- create the m-relationship' || chr(10) ||
            '        new_mrel := ' || mrel_#_ty || '(coordinate, id);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- insert the new m-relationship into the m-relationship table' || chr(10) ||
            '        INSERT INTO ' || mrel_table || ' VALUES new_mrel;' || chr(10) ||
            '        ' || chr(10) ||
            '        new_mrel_ref := SELF.get_mrel_ref(coordinate);' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN new_mrel_ref;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_create3_mrel :=
            '    MEMBER FUNCTION create_mrel(coordinate ' || coordinate_#_ty || ') RETURN REF ' || mrel_#_ty || ' IS' || chr(10) ||
            '        new_mrel ' || mrel_#_ty || ';' || chr(10) ||
            '        new_mrel_ref REF ' || mrel_#_ty || ';' || chr(10) ||
            '        id VARCHAR2(10);' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- get an id for the coordinate.' || chr(10) ||
            '        id := ''r'' || ' || mrel_id_seq || '.NEXTVAL;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- create the m-relationship' || chr(10) ||
            '        new_mrel := ' || mrel_#_ty || '(coordinate, id);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- insert the new m-relationship into the m-relationship table' || chr(10) ||
            '        INSERT INTO ' || mrel_table || ' VALUES new_mrel;' || chr(10) ||
            '        ' || chr(10) ||
            '        new_mrel_ref := SELF.get_mrel_ref(coordinate);' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN new_mrel_ref;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_bulk_create_mrel :=
            '    MEMBER PROCEDURE bulk_create_mrel(coordinates ' || coordinate_#_tty || ') IS' || chr(10) ||
            '        self_ref REF mcube_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        TYPE mrel_ancestors_tty IS TABLE OF ' || conlvl_ancestor_#_tty || ';' || chr(10) ||
            '        mrel_ancestors mrel_ancestors_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        mrel_ids names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        mrel ' || mrel_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        specializes INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        chunk_size INTEGER := 50000;' || chr(10) ||
            '        chunk_count INTEGER;' || chr(10) ||
            '        chunk_mod INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '        j INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '         -- get the reference to the mcube' || chr(10) ||
            '        SELECT REF(mc) INTO self_ref' || chr(10) ||
            '        FROM   mcubes mc' || chr(10) ||
            '        WHERE  mc.cname = SELF.cname;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- obtain an id for each coordinate' || chr(10) ||
            '        mrel_ids := names_tty();' || chr(10) ||
            '        ' || chr(10) ||
            '        i := coordinates.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            mrel_ids.EXTEND;' || chr(10) ||
            '            mrel_ids(mrel_ids.LAST) := ''r'' || ' || mrel_id_seq || '.NEXTVAL;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := coordinates.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- TODO: some cases maybe not covered.' || chr(10) ||
            '        specializes := 0;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- bulk insert the m-relationships into the table.' || chr(10) ||
            '        FORALL i IN INDICES OF coordinates' || chr(10) ||
            '            INSERT INTO ' || mrel_table || ' (mcube, id, coordinate, measure_tables, measure_metadata, specializes) VALUES' || chr(10) ||
            '                (self_ref, mrel_ids(i), coordinates(i), ' || measure_table_#_tty || '(), ' || measure_meta_#_tty || '(), specializes);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- TODO: calculate the ancestors in chunks' || chr(10) ||
            '        mrel_ancestors := mrel_ancestors_tty();' || chr(10) ||
            '        dbms_output.put_line(''Calculate ancestors starts: '' || to_char(SYSDATE, ''yyyy/mm/dd hh:MI:ss''));' || chr(10) ||
            '        i := coordinates.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            SELECT VALUE(mr) INTO mrel' || chr(10) ||
            '            FROM   ' || mrel_table || ' mr' || chr(10) ||
            '            WHERE  ' || mcube_bulk_create_mrel_where  || ';' || chr(10) ||
            '            ' || chr(10) ||
            '            mrel_ancestors.EXTEND;' || chr(10) ||
            '            mrel_ancestors(mrel_ancestors.LAST) := mrel.calculate_ancestors();' || chr(10) ||
            '            ' || chr(10) ||
            '            i := coordinates.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        dbms_output.put_line(''Calculate ancestors ends: '' || to_char(SYSDATE, ''yyyy/mm/dd hh:MI:ss''));' || chr(10) ||
            '        ' || chr(10) ||
            '        -- bulk update the ancestor collections of the new m-relationships.' || chr(10) ||
            '        -- @tricky: bulk update in chunks, otherwise ORA-21780' || chr(10) ||
            '        chunk_count := CEIL(mrel_ancestors.COUNT / chunk_size);' || chr(10) ||
            '        chunk_mod := mrel_ancestors.COUNT MOD chunk_size;' || chr(10) ||
            '        ' || chr(10) ||
            '        FOR j IN 1 .. chunk_count LOOP' || chr(10) ||
            '            IF j < chunk_count OR chunk_mod = 0 THEN' || chr(10) ||
            '                --dbms_output.put_line(TO_CHAR((j-1) * chunk_size + 1) || ''..'' ||  TO_CHAR(j * chunk_size));' || chr(10) ||
            '                FORALL i IN ((j-1) * chunk_size + 1) .. (j * chunk_size)' || chr(10) ||
            '                    UPDATE ' || mrel_table || ' mr SET mr.ancestors = mrel_ancestors(i)' || chr(10) ||
            '                        WHERE ' || mcube_bulk_create_mrel_where || ';' || chr(10) ||
            '            ELSE' || chr(10) ||
            '                -- the last chunk is usually smaller than chunk_size, so we only iterate to the end of the collection' || chr(10) ||
            '                --dbms_output.put_line(TO_CHAR((j-1) * chunk_size + 1) || ''..'' ||  TO_CHAR(mrel_ancestors.COUNT));' || chr(10) ||
            '                FORALL i IN ((j-1) * chunk_size + 1) .. mrel_ancestors.COUNT' || chr(10) ||
            '                    UPDATE ' || mrel_table || ' mr SET mr.ancestors = mrel_ancestors(i)' || chr(10) ||
            '                        WHERE ' || mcube_bulk_create_mrel_where || ';' || chr(10) ||
            '            END IF;' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_bulk_create_mrel_heu :=
            '    MEMBER PROCEDURE bulk_create_mrel(coordinates ' || coordinate_#_tty || ', parents ' || mrel_#_trty || ') IS' || chr(10) ||
            '        self_ref REF mcube_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        mrel_ancestors ' || conlvl_ancestor_#_tty || ' := ' || conlvl_ancestor_#_tty || '();' || chr(10) ||
            '        ' || chr(10) ||
            '        mrel_ids names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        mrel ' || mrel_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        specializes INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        chunk_size INTEGER := 50000;' || chr(10) ||
            '        chunk_count INTEGER;' || chr(10) ||
            '        chunk_mod INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        i PLS_INTEGER;' || chr(10) ||
            '        j PLS_INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        already_contains PLS_INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '         -- get the reference to the mcube' || chr(10) ||
            '        SELECT REF(mc) INTO self_ref' || chr(10) ||
            '        FROM   mcubes mc' || chr(10) ||
            '        WHERE  mc.cname = SELF.cname;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- obtain an id for each coordinate' || chr(10) ||
            '        mrel_ids := names_tty();' || chr(10) ||
            '        ' || chr(10) ||
            '        i := coordinates.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            mrel_ids.EXTEND;' || chr(10) ||
            '            mrel_ids(mrel_ids.LAST) := ''r'' || ' || mrel_id_seq || '.NEXTVAL;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := coordinates.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- calculate the ancestors' || chr(10) ||
            '        i := parents.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            utl_ref.select_object(parents(i), mrel);' || chr(10) ||
            '            ' || chr(10) ||
            '            IF mrel.ancestors IS NOT NULL THEN' || chr(10) ||
            '                j := mrel.ancestors.FIRST;' || chr(10) ||
            '                WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                    -- check if the ancestor is already in the collection' || chr(10) ||
            '                    SELECT COUNT(*) INTO already_contains' || chr(10) ||
            '                    FROM   TABLE(mrel_ancestors) anc' || chr(10) ||
            '                    WHERE  anc.ancestor = mrel.ancestors(j).ancestor;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    IF already_contains = 0 THEN' || chr(10) ||
            '                        mrel_ancestors.EXTEND;' || chr(10) ||
            '                        mrel_ancestors(mrel_ancestors.LAST) := ' || chr(10) ||
            '                            ' || conlvl_ancestor_#_ty || '(mrel.top_level(), parents(i));' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    j := mrel.ancestors.NEXT(j);' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- add the parent to the ancestors collection' || chr(10) ||
            '            mrel_ancestors.EXTEND;' || chr(10) ||
            '            mrel_ancestors(mrel_ancestors.LAST) := ' || chr(10) ||
            '                ' || conlvl_ancestor_#_ty || '(mrel.top_level(), parents(i));' || chr(10) ||
            '            ' || chr(10) ||
            '            i := parents.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- TODO: some cases maybe not covered' || chr(10) ||
            '        specializes := 0;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- bulk insert the m-relationships into the table.' || chr(10) ||
            '        FORALL i IN INDICES OF coordinates' || chr(10) ||
            '            INSERT INTO ' || mrel_table || ' (mcube, id, coordinate, measure_tables, measure_metadata, ancestors, specializes) VALUES' || chr(10) ||
            '                (self_ref, mrel_ids(i), coordinates(i), ' || measure_table_#_tty || '(), ' || measure_meta_#_tty || '(), mrel_ancestors, specializes);' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_get_mrel_ref :=
            '    MEMBER FUNCTION get_mrel_ref(coordinate ' || coordinate_#_ty || ') RETURN REF ' || mrel_#_ty || ' IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        RETURN SELF.get_mrel_ref(' || mcube_get_mrel_ref1 || ');' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_get2_mrel_ref :=
            '    MEMBER FUNCTION get_mrel_ref(' || mcube_get2_mrel_ref1 || ') RETURN REF ' || mrel_#_ty || ' IS' || chr(10) ||
            '        ' || chr(10) ||
                     mcube_get2_mrel_ref_obj_def ||
            '    BEGIN' || chr(10) ||
                     mcube_get2_mrel_ref_obj ||
            '        ' || chr(10) ||
            '        RETURN SELF.get_mrel_ref(' || mcube_get2_mrel_ref2 || ');' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_get3_mrel_ref :=
            '    MEMBER FUNCTION get_mrel_ref(' || mcube_get3_mrel_ref1 || ') RETURN REF ' || mrel_#_ty || ' IS' || chr(10) ||
            '        mrel_ref REF ' || mrel_#_ty || ';' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        SELECT REF(mr) INTO mrel_ref' || chr(10) ||
            '        FROM   ' || mrel_table || ' mr' || chr(10) ||
            '        WHERE  ' || mcube_get3_mrel_ref2 || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN mrel_ref;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_get_measure_descr :=
            '    MEMBER FUNCTION get_measure_description(measure_name VARCHAR2, ' || mcube_get_measure_descr1 || ') RETURN ' || measure_#_ty || ' IS' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_descriptions ' || measure_#_tty || ';' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- order by coordinate; the first tuple contains the coordinate that is' || chr(10) ||
            '        -- nearest to the m-relationship subcube.' || chr(10) ||
            '        SELECT ' || measure_#_ty || '(measure_name, t.conlevel, t.table_name, utc.data_type, utc.data_length, utc.data_scale) BULK COLLECT INTO measure_descriptions' || chr(10) ||
            '        FROM   ' || mrel_table || ' m, TABLE(m.measure_tables) t,' || chr(10) ||
            '               user_tab_columns utc' || chr(10) ||
            '        WHERE ((' || mcube_get_measure_descr_where || ') OR ' || chr(10) ||
            '               REF(m) IN (SELECT anc.ancestor FROM '|| mrel_table || ' x, TABLE(x.ancestors) anc WHERE ' || mcube_get_measure_descr_where1 || ')) AND' || chr(10) ||
            '              t.table_name = utc.table_name AND' || chr(10) ||
            '              utc.column_name = UPPER(measure_name)' || chr(10) ||
            '        ORDER BY m.coordinate ASC;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- take the first measure description, since this' || chr(10) ||
            '        -- comes from the m-relationship that is nearest in the hierarchy.' || chr(10) ||
            '        RETURN measure_descriptions(measure_descriptions.FIRST);' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
            
        mcube_get_measure_descrs :=
            '    MEMBER FUNCTION get_measure_descriptions(measure_name VARCHAR2) RETURN ' || measure_#_tty || ' IS' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_descriptions ' || measure_#_tty || ';' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- order by coordinate; the first tuple contains the coordinate that is' || chr(10) ||
            '        -- nearest to the m-relationship subcube.' || chr(10) ||
            '        SELECT ' || measure_#_ty || '(measure_name, t.conlevel, t.table_name, utc.data_type, utc.data_length, utc.data_scale) BULK COLLECT INTO measure_descriptions' || chr(10) ||
            '        FROM   ' || mrel_table || ' m, TABLE(m.measure_tables) t,' || chr(10) ||
            '               user_tab_columns utc' || chr(10) ||
            '        WHERE t.table_name = utc.table_name AND' || chr(10) ||
            '              utc.column_name = UPPER(measure_name)' || chr(10) ||
            '        ORDER BY m.coordinate ASC;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN measure_descriptions;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_get_measure_unit :=
            '    /**' || chr(10) ||
            '     * This method returns the unit of a measure as valid at a' || chr(10) ||
            '     * particular coordinate.' || chr(10) ||
            '     */ ' || chr(10) ||
            '    MEMBER FUNCTION get_measure_unit(measure_name VARCHAR2, ' || mcube_get_measure_unit1 ||  ') RETURN ANYDATA IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        ' || chr(10) ||
            '        ' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN NULL;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_get_measure_funct :=
            '    /**' || chr(10) ||
            '     * This method returns the function of a measure as valid at a' || chr(10) ||
            '     * particular coordinate.' || chr(10) ||
            '     */ ' || chr(10) ||
            '    MEMBER FUNCTION get_measure_function(measure_name VARCHAR2) RETURN ANYDATA IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        ' || chr(10) ||
            '        ' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN NULL;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_bulk_set_measure :=
            '    MEMBER PROCEDURE bulk_set_measure(measure_name VARCHAR2, measure_values ' || mrel_#_value_tty || ') IS' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_descr ' || measure_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        update_data VARCHAR2(1000);' || chr(10) ||
            '        ' || chr(10) ||
            '        TYPE number_tty IS TABLE OF NUMBER;' || chr(10) ||
            '        TYPE varchar2_tty IS TABLE OF VARCHAR2(4000); --> 4000 bytes since this is the limit in the database' || chr(10) ||
            '        TYPE elem_tty_tty IS TABLE OF elem_tty;' || chr(10) ||
            '        TYPE anydata_tty IS TABLE OF ANYDATA;' || chr(10) ||
            '        ' || chr(10) ||
            '        number_value   NUMBER;' || chr(10) ||
            '        varchar2_value VARCHAR2(4000);' || chr(10) ||
            '        elem_tty_value elem_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        number_values number_tty;' || chr(10) ||
            '        varchar2_values varchar2_tty;' || chr(10) ||
            '        elem_tty_values elem_tty_tty;' || chr(10) ||
            '        anydata_values anydata_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        status PLS_INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- determine which index in the measure_values collection' || chr(10) ||
            '        -- maps to which measure table_name' || chr(10) ||
            '        /*i := measure_values.LAST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            measure_descr := SELF.get_measure_description(measure_name, ' || mcube_bulk_set_measure_mrel || ');' || chr(10) ||
            '            ' || chr(10) ||
            '            IF NOT measure_tables.EXISTS(measure_descr.table_name) THEN' || chr(10) ||
            '                measure_tables(measure_descr.table_name) := ' || chr(10) ||
            '                    names_tty(i);' || chr(10) ||
            '                ' || chr(10) ||
            '                -- store the measure description for this table' || chr(10) ||
            '                measure_descriptions(measure_descr.table_name) := measure_descr;' || chr(10) ||
            '            ELSE' || chr(10) ||
            '                measure_tables(measure_descr.table_name).EXTEND;' || chr(10) ||
            '                measure_tables(measure_descr.table_name)(measure_tables(measure_descr.table_name).LAST) := i;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF i MOD 1000 = 0 THEN' || chr(10) ||
            '                dbms_output.put_line(i || '': '' || to_char(sysdate, ''yyyy/mm/dd hh:MI:ss''));' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_values.PRIOR(i);' || chr(10) ||
            '        END LOOP;*/' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_values.LAST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            measure_descr := SELF.get_measure_description(measure_name, ' || mcube_bulk_set_measure_mrel || ');' || chr(10) ||
            '            ' || chr(10) ||
            '            -- insert rows for those m-objects where no tuple exists already' || chr(10) ||
            '            EXECUTE IMMEDIATE' || chr(10) ||
            '                ''INSERT INTO '' || measure_descr.table_name || '' (mrel, coordinate) ('' || chr(10) ||' || chr(10) ||
            '                ''    SELECT REF(mr), mr.coordinate'' || chr(10) ||' || chr(10) ||
            '                ''    FROM '' || mrel_table || '' mr'' || chr(10) ||' || chr(10) ||
            '                ''    WHERE ' || mcube_bulk_set_measure_mrel1 || ' AND'' || chr(10) ||' || chr(10) ||
            '                ''          NOT EXISTS (SELECT t.coordinate'' || chr(10) ||' || chr(10) ||
            '                ''                      FROM   '' || measure_descr.table_name || '' t'' || chr(10) ||' || chr(10) ||
            '                ''                      WHERE  ' || mcube_bulk_set_measure_mrel2 || ')'' || chr(10) ||' || chr(10) ||
            '                '')''' || chr(10) ||
            '            USING ' || mcube_bulk_set_measure_mrel3 || ';' || chr(10) ||
            '            ' || chr(10) ||
            '            update_data :=' || chr(10) ||
            '                ''UPDATE '' || measure_descr.table_name || '' t'' || chr(10) ||' || chr(10) ||
            '                ''SET    t.'' || measure_name || '' = :1'' || chr(10) ||' || chr(10) ||
            '                ''WHERE  ' || mcube_bulk_set_measure_mrel4 || ''';' || chr(10) ||
            '            ' || chr(10) ||
            '            CASE measure_descr.data_type' || chr(10) ||
            '                WHEN ''NUMBER'' THEN' || chr(10) ||
            '                    number_value := ' || chr(10) ||
            '                        measure_values(i).measure_value.accessNumber();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                    USING number_value, ' || mcube_bulk_set_measure_mrel5 || ';' || chr(10) ||
            '                WHEN ''VARCHAR2'' THEN' || chr(10) ||
            '                    varchar2_value := ' || chr(10) ||
            '                        measure_values(i).measure_value.accessVarchar2();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                    USING varchar2_value, ' || mcube_bulk_set_measure_mrel5 || ';' || chr(10) ||
            '                WHEN ''ELEM_TTY'' THEN' || chr(10) ||
            '                    status := ' || chr(10) ||
            '                        measure_values(i).measure_value.getCollection(elem_tty_value);' || chr(10) ||
            '                    ' || chr(10) ||
            '                    EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                    USING elem_tty_value, ' || mcube_bulk_set_measure_mrel5 || ';' || chr(10) ||
            '            END CASE;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF i MOD 1000 = 0 THEN' || chr(10) ||
            '                dbms_output.put_line(i || '': '' || to_char(sysdate, ''yyyy/mm/dd hh:MI:ss''));' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_values.PRIOR(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        /*table_name := measure_tables.FIRST;' || chr(10) ||
            '        WHILE table_name IS NOT NULL LOOP' || chr(10) ||
            '            -- insert rows for those m-objects where no tuple exists already' || chr(10) ||
            '            FORALL i IN INDICES OF measure_tables(table_name)' || chr(10) ||
            '                EXECUTE IMMEDIATE' || chr(10) ||
            '                    ''INSERT INTO '' || table_name || '' (mrel, coordinate) ('' || chr(10) ||' || chr(10) ||
            '                    ''    SELECT REF(mr), mr.coordinate'' || chr(10) ||' || chr(10) ||
            '                    ''    FROM '' || mrel_table || '' mr'' || chr(10) ||' || chr(10) ||
            '                    ''    WHERE ' || mcube_bulk_set_measure_mrel1 || ' AND'' || chr(10) ||' || chr(10) ||
            '                    ''          NOT EXISTS (SELECT t.coordinate'' || chr(10) ||' || chr(10) ||
            '                    ''                      FROM   '' || table_name || '' t'' || chr(10) ||' || chr(10) ||
            '                    ''                      WHERE  ' || mcube_bulk_set_measure_mrel2 || ')'' || chr(10) ||' || chr(10) ||
            '                    '')''' || chr(10) ||
            '                USING ' || mcube_bulk_set_measure_mrel3 || ';' || chr(10) ||
            '            ' || chr(10) ||
            '            update_data :=' || chr(10) ||
            '                ''UPDATE '' || table_name || '' t'' || chr(10) ||' || chr(10) ||
            '                ''SET    t.'' || measure_name || '' = :1'' || chr(10) ||' || chr(10) ||
            '                ''WHERE  ' || mcube_bulk_set_measure_mrel4 || ''';' || chr(10) ||
            '            ' || chr(10) ||
            '            -- update the rows (insert the attribute value)' || chr(10) ||
            '            CASE measure_descriptions(table_name).data_type' || chr(10) ||
            '                WHEN ''NUMBER'' THEN' || chr(10) ||
            '                    -- reset nested table' || chr(10) ||
            '                    number_values := number_tty();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    -- have to convert it before inserting, since PLS-00801' || chr(10) ||
            '                    i := measure_tables(table_name).FIRST;' || chr(10) ||
            '                    WHILE i IS NOT NULL LOOP' || chr(10) ||
            '                        number_values.EXTEND;' || chr(10) ||
            '                        number_values(number_values.LAST) := ' || chr(10) ||
            '                            measure_tables(table_name)(i).measure_value.accessNumber();' || chr(10) ||
            '                        ' || chr(10) ||
            '                        i := measure_tables(table_name).NEXT(i);' || chr(10) ||
            '                    END LOOP;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    FORALL i IN INDICES OF measure_tables(table_name)' || chr(10) ||
            '                        EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                        USING number_values(i), ' || mcube_bulk_set_measure_mrel5 || ';' || chr(10) ||
            '                WHEN ''VARCHAR2'' THEN' || chr(10) ||
            '                    -- reset nested table' || chr(10) ||
            '                    varchar2_values := varchar2_tty();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    -- have to convert it before inserting, since PLS-00801' || chr(10) ||
            '                    i := measure_tables(table_name).FIRST;' || chr(10) ||
            '                    WHILE i IS NOT NULL LOOP' || chr(10) ||
            '                        varchar2_values.EXTEND;' || chr(10) ||
            '                        varchar2_values(varchar2_values.LAST) := ' || chr(10) ||
            '                            measure_tables(table_name)(i).measure_value.accessVarchar2();' || chr(10) ||
            '                        ' || chr(10) ||
            '                        i := measure_tables(table_name).NEXT(i);' || chr(10) ||
            '                    END LOOP;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    FORALL i IN INDICES OF measure_tables(table_name)' || chr(10) ||
            '                        EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                        USING varchar2_values(i), ' || mcube_bulk_set_measure_mrel5 || ';' || chr(10) ||
            '                WHEN ''ELEM_TTY'' THEN' || chr(10) ||
            '                    -- reset nested table' || chr(10) ||
            '                    elem_tty_values := elem_tty_tty();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    -- have to convert it before inserting, since PLS-00801' || chr(10) ||
            '                    i := measure_tables(table_name).FIRST;' || chr(10) ||
            '                    WHILE i IS NOT NULL LOOP' || chr(10) ||
            '                        elem_tty_values.EXTEND;' || chr(10) ||
            '                        status := measure_tables(table_name)(i).measure_value.getCollection(elem_tty_values(elem_tty_values.LAST));' || chr(10) ||
            '                        ' || chr(10) ||
            '                        i := measure_tables(table_name).NEXT(i);' || chr(10) ||
            '                    END LOOP;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    FORALL i IN INDICES OF measure_tables(table_name)' || chr(10) ||
            '                        EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                        USING elem_tty_values(i), ' || mcube_bulk_set_measure_mrel5 || ';' || chr(10) ||
            '                ELSE' || chr(10) ||
            '                    -- reset nested table' || chr(10) ||
            '                    anydata_values := anydata_tty();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    i := measure_tables(table_name).FIRST;' || chr(10) ||
            '                    WHILE i IS NOT NULL LOOP' || chr(10) ||
            '                        anydata_values.EXTEND;' || chr(10) ||
            '                        anydata_values(anydata_values.LAST) := ' || chr(10) ||
            '                            measure_tables(table_name)(i).measure_value;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        i := measure_tables(table_name).NEXT(i);' || chr(10) ||
            '                    END LOOP;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    FORALL i IN INDICES OF measure_tables(table_name)' || chr(10) ||
            '                        EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                        USING anydata_values(i), ' || mcube_bulk_set_measure_mrel5 || ';' || chr(10) ||
            '            END CASE;' || chr(10) ||
            '            ' || chr(10) ||
            '            table_name := measure_tables.NEXT(table_name);' || chr(10) ||
            '        END LOOP;*/' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_bulk_set_measure_heu :=
            '    MEMBER PROCEDURE bulk_set_measure(measure_name VARCHAR2, measure_values ' || mrel_#_value_tty || ', ' || mcube_bulk_set_measure_heu1 || ') IS' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_descr ' || measure_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        mrel_ref REF ' || mrel_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        update_data VARCHAR2(1000);' || chr(10) ||
            '        ' || chr(10) ||
            '        TYPE number_tty IS TABLE OF NUMBER;' || chr(10) ||
            '        TYPE varchar2_tty IS TABLE OF VARCHAR2(4000); --> 4000 bytes since this is the limit in the database' || chr(10) ||
            '        TYPE elem_tty_tty IS TABLE OF elem_tty;' || chr(10) ||
            '        TYPE anydata_tty IS TABLE OF ANYDATA;' || chr(10) ||
            '        ' || chr(10) ||
            '        number_values number_tty;' || chr(10) ||
            '        varchar2_values varchar2_tty;' || chr(10) ||
            '        elem_tty_values elem_tty_tty;' || chr(10) ||
            '        anydata_values anydata_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        status PLS_INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        measure_descr := SELF.get_measure_description(measure_name, ' || mcube_bulk_set_measure_heu_mr || ');' || chr(10) ||
            '        ' || chr(10) ||
            '        -- insert rows for those m-objects where no tuple exists already' || chr(10) ||
            '        FORALL i IN INDICES OF measure_values' || chr(10) ||
            '            EXECUTE IMMEDIATE' || chr(10) ||
            '                ''INSERT INTO '' || measure_descr.table_name || '' (mrel, coordinate) ('' || chr(10) ||' || chr(10) ||
            '                ''    SELECT REF(mr), mr.coordinate'' || chr(10) ||' || chr(10) ||
            '                ''    FROM '' || mrel_table || '' mr'' || chr(10) ||' || chr(10) ||
            '                ''    WHERE ' || mcube_bulk_set_measure_mrel1 || ' AND'' || chr(10) ||' || chr(10) ||
            '                ''          NOT EXISTS (SELECT t.coordinate'' || chr(10) ||' || chr(10) ||
            '                ''                      FROM   '' || measure_descr.table_name || '' t'' || chr(10) ||' || chr(10) ||
            '                ''                      WHERE  ' || mcube_bulk_set_measure_mrel2 || ')'' || chr(10) ||' || chr(10) ||
            '                '')''' || chr(10) ||
            '            USING ' || mcube_bulk_set_measure_mrel3 || ';' || chr(10) ||
            '        ' || chr(10) ||'            ' || chr(10) ||
            '        update_data :=' || chr(10) ||
            '            ''UPDATE '' || measure_descr.table_name || '' t'' || chr(10) ||' || chr(10) ||
            '            ''SET    t.'' || measure_name || '' = :1'' || chr(10) ||' || chr(10) ||
            '            ''WHERE  ' || mcube_bulk_set_measure_mrel4 || ''';' || chr(10) ||
            '        ' || chr(10) ||'            ' || chr(10) ||
            '        CASE measure_descr.data_type' || chr(10) ||
            '            WHEN ''NUMBER'' THEN' || chr(10) ||
            '                -- reset nested table' || chr(10) ||
            '                number_values := number_tty();' || chr(10) ||
            '                ' || chr(10) ||
            '                -- have to convert it before inserting, since PLS-00801' || chr(10) ||
            '                i := measure_values.FIRST;' || chr(10) ||
            '                WHILE i IS NOT NULL LOOP' || chr(10) ||
            '                    number_values.EXTEND;' || chr(10) ||
            '                    number_values(number_values.LAST) := ' || chr(10) ||
            '                        measure_values(i).measure_value.accessNumber();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    i := measure_values.NEXT(i);' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '                FORALL i IN INDICES OF number_values' || chr(10) ||
            '                    EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                    USING number_values(i), ' || mcube_bulk_set_measure_mrel5 || ';' || chr(10) ||
            '            WHEN ''VARCHAR2'' THEN' || chr(10) ||
            '                -- reset nested table' || chr(10) ||
            '                varchar2_values := varchar2_tty();' || chr(10) ||
            '                ' || chr(10) ||
            '                -- have to convert it before inserting, since PLS-00801' || chr(10) ||
            '                i := measure_values.FIRST;' || chr(10) ||
            '                WHILE i IS NOT NULL LOOP' || chr(10) ||
            '                    varchar2_values.EXTEND;' || chr(10) ||
            '                    varchar2_values(varchar2_values.LAST) := ' || chr(10) ||
            '                        measure_values(i).measure_value.accessVarchar2();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    i := measure_values.NEXT(i);' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '                FORALL i IN INDICES OF varchar2_values' || chr(10) ||
            '                    EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                    USING varchar2_values(i), ' || mcube_bulk_set_measure_mrel5 || ';' || chr(10) ||
            '            WHEN ''ELEM_TTY'' THEN' || chr(10) ||
            '                -- reset nested table' || chr(10) ||
            '                elem_tty_values := elem_tty_tty();' || chr(10) ||
            '                ' || chr(10) ||
            '                -- have to convert it before inserting, since PLS-00801' || chr(10) ||
            '                i := measure_values.FIRST;' || chr(10) ||
            '                WHILE i IS NOT NULL LOOP' || chr(10) ||
            '                    elem_tty_values.EXTEND;' || chr(10) ||
            '                    status := ' || chr(10) ||
            '                        measure_values(i).measure_value.getCollection(elem_tty_values(number_values.LAST));' || chr(10) ||
            '                    ' || chr(10) ||
            '                    i := measure_values.NEXT(i);' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '                FORALL i IN INDICES OF elem_tty_values' || chr(10) ||
            '                    EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                    USING elem_tty_values(i), ' || mcube_bulk_set_measure_mrel5 || ';' || chr(10) ||
            '        END CASE;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
            
        
        
        mcube_refresh_meas_unit_cache :=
            '    MEMBER PROCEDURE refresh_measure_unit_cache(measure_name VARCHAR2, mrel_refs ' || mrel_#_trty || ') IS' || chr(10) ||
            '        ' || chr(10) ||
            '        mrel ' || mrel_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        unit_any     ANYDATA;' || chr(10) ||
            '        unit_obj_ref REF mobject_ty;' || chr(10) ||
            '        unit_obj     mobject_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        TYPE mrel_unit_ty IS RECORD (coordinate ' || coordinate_#_ty || ', unit VARCHAR2(30));' || chr(10) ||
            '        TYPE mrel_unit_tty IS TABLE OF mrel_unit_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        mrel_units mrel_unit_tty := mrel_unit_tty();' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_descriptions ' || measure_#_tty || ';' || chr(10) ||
            '        measure_tables names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        i PLS_INTEGER;' || chr(10) ||
            '        j PLS_INTEGER;' || chr(10) ||
            '        status PLS_INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        i := mrel_refs.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            utl_ref.select_object(mrel_refs(i), mrel);' || chr(10) ||
            '            ' || chr(10) ||
            '            unit_any := mrel.get_measure_unit(measure_name);' || chr(10) ||
            '            ' || chr(10) ||
            '            IF unit_any IS NOT NULL THEN' || chr(10) ||
            '                -- TODO: check if unit was really stored as m-object' || chr(10) ||
            '                status := ANYDATA.getRef(unit_any, unit_obj_ref);' || chr(10) ||
            '                utl_ref.select_object(unit_obj_ref, unit_obj);' || chr(10) ||
            '                ' || chr(10) ||
            '                mrel_units.EXTEND;' || chr(10) ||
            '                mrel_units(mrel_units.LAST).coordinate := mrel.coordinate;' || chr(10) ||
            '                mrel_units(mrel_units.LAST).unit := unit_obj.oname;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := mrel_refs.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- get the measure tables for this measure' || chr(10) ||
            '        measure_descriptions := SELF.get_measure_descriptions(measure_name);' || chr(10) ||
            '        ' || chr(10) ||
            '        SELECT DISTINCT x.table_name BULK COLLECT INTO measure_tables' || chr(10) ||
            '        FROM   TABLE(measure_descriptions) x;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- TODO: check if there even is a column for the units, i.e., is the cache enabled.' || chr(10) ||
            '        ' || chr(10) ||
            '        -- bulk update the measure tables' || chr(10) ||
            '        i := measure_tables.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            FORALL j IN INDICES OF mrel_units' || chr(10) ||
            '                EXECUTE IMMEDIATE' || chr(10) ||
            '                    ''UPDATE '' || measure_tables(i) || '' r'' || chr(10) ||' || chr(10) ||
            '                    ''    SET r.'' || measure_name || ''_unit = :1 '' || chr(10) ||' || chr(10) ||
            '                    ''    WHERE ' || mcube_re_meas_unit_cache_where || '''' || chr(10) ||
            '                USING mrel_units(j).unit, ' || mcube_re_meas_unit_cache_bind || ';' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_tables.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_new_queryview :=
            '    MEMBER FUNCTION new_queryview RETURN ' || queryview_#_ty || ' IS' || chr(10) ||
            '        self_ref REF ' || mcube_#_ty || ';' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- get the reference to SELF' || chr(10) ||
            '        SELECT TREAT(REF(mc) AS REF ' || mcube_#_ty || ') INTO self_ref' || chr(10) ||
            '        FROM   mcubes mc' || chr(10) ||
            '        WHERE  mc.cname = SELF.cname;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN ' || queryview_#_ty || '(self_ref);' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_persist :=
            '    OVERRIDING MEMBER PROCEDURE persist IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        UPDATE mcubes mc' || chr(10) ||
            '        SET    mc = SELF' || chr(10) ||
            '        WHERE  mc.cname = SELF.cname;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_get_dimension_names :=
            '    OVERRIDING MEMBER FUNCTION get_dimension_names RETURN names_tty IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        RETURN names_tty(' || mcube_get_dimension_names1 || ');' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_get_dimension_ids :=
            '    OVERRIDING MEMBER FUNCTION get_dimension_ids RETURN names_tty IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        RETURN names_tty(' || mcube_get_dimension_ids1 || ');' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_export_star := 
            '   MEMBER PROCEDURE export_star(table_name VARCHAR2, ' || mcube_export_star1 || ') IS' || chr(10) ||
            '        ' || chr(10) ||
            '        create_statement CLOB;' || chr(10) ||
            '        ' || chr(10) ||
            '        insert_statement   CLOB;' || chr(10) ||
            '        insert_stmt_select CLOB;' || chr(10) ||
            '        ' || chr(10) ||
            '        update_statement CLOB;' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_names_and_types ' || measure_#_tty || ';' || chr(10) ||
            '        measure_names names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_tables names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        data_type   VARCHAR2(30);' || chr(10) ||
            '        data_length NUMBER;' || chr(10) ||
            '        data_scale  NUMBER;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '        j INTEGER;' || chr(10) ||
            '   BEGIN' || chr(10) ||
            '        -- get all measures and their types' || chr(10) ||
            '        SELECT ' || measure_#_ty || '(utc.column_name, x.lvl, utc.table_name, utc.data_type, utc.data_length, utc.data_scale) BULK COLLECT INTO measure_names_and_types' || chr(10) ||
            '        FROM   user_tab_columns utc,' || chr(10) ||
            '               (SELECT UPPER(t.table_name) AS table_name, t.conlevel AS lvl' || chr(10) ||
            '                FROM   ' || mrel_table || ' r, TABLE(r.measure_tables) t) x' || chr(10) ||
            '        WHERE  utc.table_name IN x.table_name AND utc.column_name <> ''COORDINATE'' AND utc.column_name <> ''MREL'' AND utc.column_name NOT LIKE ''%_UNIT'';' || chr(10) ||
            '        ' || chr(10) ||
            '        SELECT DISTINCT m.measure_name BULK COLLECT INTO measure_names' || chr(10) ||
            '        FROM   TABLE(measure_names_and_types) m;' || chr(10) ||
            '        ' || chr(10) ||
            '        SELECT DISTINCT m.table_name BULK COLLECT INTO measure_tables' || chr(10) ||
            '        FROM   TABLE(measure_names_and_types) m;' || chr(10) ||
            '        ' || chr(10) ||
            '        create_statement := ''CREATE TABLE '' || table_name || ''('' || chr(10) || ' || chr(10) ||
                     mcube_export_star_create_stmt1 || ' || chr(10);' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_names.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            IF i > 1 THEN' || chr(10) ||
            '                create_statement := create_statement || '', '' || chr(10);' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- get data type' || chr(10) ||
            '            SELECT DISTINCT x.data_type, x.data_length, x.data_scale INTO data_type, data_length, data_scale' || chr(10) ||
            '            FROM   TABLE(measure_names_and_types) x' || chr(10) ||
            '            WHERE  x.measure_name = measure_names(i);' || chr(10) ||
            '            ' || chr(10) ||
            '            create_statement := create_statement || ' || chr(10) ||
            '                ''    '' || measure_names(i) || '' '' || data_types.to_string(data_type, data_length, data_scale);' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_names.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        create_statement := create_statement || '', '' || chr(10) || ' || chr(10) ||
            '            ''    PRIMARY KEY(' || mcube_export_star_create_stmt2 || '),'' || chr(10) ||' || chr(10) ||
                         mcube_export_star_create_stmt3 || ''' || chr(10) || ' || chr(10) ||
            '            '')'';' || chr(10) ||
            '        ' || chr(10) ||
            '        --dbms_output.put_line(create_statement);' || chr(10) ||
            '        EXECUTE IMMEDIATE create_statement;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- INSERT' || chr(10) ||
            '        i := measure_tables.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            IF i > 1 THEN' || chr(10) ||
            '                insert_stmt_select := insert_stmt_select || chr(10) || '' UNION '' || chr(10);' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            insert_stmt_select := insert_stmt_select ||' || chr(10) ||
            '                ''SELECT ' || mcube_export_star_insert_stmt2 || ' FROM '' || measure_tables(i) || '' r'';' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_tables.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        insert_statement := ''INSERT INTO '' || table_name || ''(' || mcube_export_star_insert_stmt1 || ') ('' || chr(10) || insert_stmt_select || chr(10) || '')'';' || chr(10) ||
            '        ' || chr(10) ||
            '        --dbms_output.put_line(insert_statement);' || chr(10) ||
            '        EXECUTE IMMEDIATE insert_statement;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- UPDATE' || chr(10) ||
            '        i := measure_tables.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            SELECT m.measure_name BULK COLLECT INTO measure_names' || chr(10) ||
            '            FROM   TABLE(measure_names_and_types) m' || chr(10) ||
            '            WHERE  m.table_name = measure_tables(i);' || chr(10) ||
            '            ' || chr(10) ||
            '            update_statement := ''UPDATE '' || table_name || '' SET '';' || chr(10) ||
            '            ' || chr(10) ||
            '            j := measure_names.FIRST;' || chr(10) ||
            '            WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                IF j > 1 THEN' || chr(10) ||
            '                    update_statement := update_statement || '', '';' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- TODO: Possibly suboptimal solution. More than one update' || chr(10) ||
            '                --       per table. Also, not sure what happens if pre-calculated' || chr(10) ||
            '                --       values are stored. Then wrong result could stay.' || chr(10) ||
            '                --       Solution will do for now.' || chr(10) ||
            '                ' || chr(10) ||
            '                update_statement := ''UPDATE '' || table_name || '' SET '';' || chr(10) ||
            '                ' || chr(10) ||
            '                update_statement := update_statement || ' || chr(10) ||
            '                    measure_names(j) || '' = (SELECT '' || measure_names(j) || '' FROM '' || measure_tables(i) || '' m WHERE ' || mcube_export_star_update_stmt1 || ')'';' || chr(10) ||
            '                ' || chr(10) ||
            '                update_statement := update_statement || chr(10) ||' || chr(10) ||
            '                    ''WHERE '' || measure_names(j) || '' IS NULL'';' || chr(10) ||
            '                ' || chr(10) ||
            '                --dbms_output.put_line(update_statement);' || chr(10) ||
            '                EXECUTE IMMEDIATE update_statement;' || chr(10) ||
            '                ' || chr(10) ||
            '                j := measure_names.NEXT(j);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_tables.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '   END;' || chr(10);
        
        mcube_rollup_unit_aware :=
            '    MEMBER PROCEDURE rollup(table_name VARCHAR2, include_non_dimension_attr BOOLEAN, measure_units measure_unit_tty, ' || mcube_rollup1 || ') IS' || chr(10) ||
            '        ' || chr(10) ||
            '        mc ' || mcube_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        TYPE string_tty IS TABLE OF VARCHAR2(10000) INDEX BY VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- this type is used to store table names of the rollup tables,' || chr(10) ||
            '        -- indexed by their aggregation level.' || chr(10) ||
            '        TYPE table_names_tty IS TABLE OF VARCHAR2(30) INDEX BY VARCHAR2(' || dimensions.COUNT * 30 || ');' || chr(10) ||
            '        ' || chr(10) ||
            '        -- this type stores the rollup table names by rollup cube' || chr(10) ||
            '        TYPE table_names_ttty IS TABLE OF table_names_tty INDEX BY VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_measures string_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        rollup_tables_by_cube table_names_ttty;' || chr(10) ||
            '        rollup_table  VARCHAR2(30);' || chr(10) ||
            '        rollup_level  VARCHAR2(30);' || chr(10) ||
            '        rollup_table_args VARCHAR2(5000);' || chr(10) ||
            '        ' || chr(10) ||
            '        conlevel_string VARCHAR2(' || dimensions.COUNT * 30 || ');' || chr(10) ||
            '        ' || chr(10) ||
            '        create_table  VARCHAR2(2000);' || chr(10) ||
            '        create_table1 VARCHAR2(1000);' || chr(10) ||
            '        create_table2 VARCHAR2(2000);' || chr(10) ||
            '        insert_data   VARCHAR2(30000);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_join                 VARCHAR2(20000);' || chr(10) ||
            '        select_join_select_dim      VARCHAR2(5000);' || chr(10) ||
            '        select_join_select_measures VARCHAR2(5000);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_rollup        VARCHAR2(10000);' || chr(10) ||
            '        select_rollup_from   VARCHAR2(5000);' || chr(10) ||
            '        select_rollup_from1  VARCHAR2(5000);' || chr(10) ||
            '        select_rollup_select VARCHAR2(4000);' || chr(10) ||
            '        select_rollup_where  VARCHAR2(4000);' || chr(10) ||
            '        ' || chr(10) ||
                     mcube_rollup2_var_decl ||
            '        ' || chr(10) ||
                     mcube_rollup2_join_sel_var ||
            '        ' || chr(10) ||
            '        measure_list names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_name         VARCHAR2(30);' || chr(10) ||
            '        measure_descriptions ' || measure_#_tty || ';' || chr(10) ||
            '        measure_table        VARCHAR2(30);' || chr(10) ||
            '        measure_datatype     VARCHAR2(30);' || chr(10) ||
            '        typecode             VARCHAR2(100);' || chr(10) ||
            '        ' || chr(10) ||
            '        nested_table_name  VARCHAR2(30);' || chr(10) ||
            '        nested_table_name1 VARCHAR2(60);' || chr(10) ||
            '        ' || chr(10) ||
            '        aggregation_function VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_level_pos INTEGER;' || chr(10) ||
            '        rollup_level_pos INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        convert_measure INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        unit_any ANYDATA;' || chr(10) ||
            '        unit_ref REF mobject_ty;' || chr(10) ||
            '        unit_obj mobject_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        dim_ref  REF dimension_ty;' || chr(10) ||
            '        dim_obj  dimension_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        unit_dname VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        units_cube_any ANYDATA;' || chr(10) ||
            '        units_cube_ref REF mcube_ty;' || chr(10) ||
            '        units_cube_obj mcube_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        units_cname   VARCHAR2(30);' || chr(10) ||
            '        units_cube_dimension_names names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '        j INTEGER;' || chr(10) ||
            '        k INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        sql_cursor INTEGER;' || chr(10) ||
            '        rows_processed INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        status PLS_INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        ' || chr(10) ||
                     mcube_rollup2_from || chr(10) ||
            '        ' || chr(10) ||
            '        -- get all measures' || chr(10) ||
            '        SELECT DISTINCT utc.column_name BULK COLLECT INTO measure_list' || chr(10) ||
            '        FROM   user_tab_columns utc, ' || chr(10) ||
            '               (SELECT UPPER(t.table_name) AS table_name' || chr(10) ||
            '                FROM   ' || mrel_table || ' o, TABLE(o.measure_tables) t) x' || chr(10) ||
            '        WHERE  utc.table_name IN x.table_name AND ' || chr(10) ||
            '               utc.column_name <> ''COORDINATE'' AND ' || chr(10) ||
            '               utc.column_name <> ''MREL'' AND utc.column_name NOT LIKE ''%_UNIT'';' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.LAST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            IF i = measure_list.LAST THEN' || chr(10) ||
                             mcube_rollup2_join_select1 ||
            '            ELSE' || chr(10) ||
                             mcube_rollup2_join_select2 ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.PRIOR(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- rollup the unit cube of every measure that should be converted' || chr(10) ||
            '        i := measure_list.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            -- check if the current measure is to be converted' || chr(10) ||
            '            SELECT COUNT(*) INTO convert_measure' || chr(10) ||
            '            FROM   TABLE(measure_units)' || chr(10) ||
            '            WHERE  UPPER(measure_name) = UPPER(measure_list(i));' || chr(10) ||
            '            ' || chr(10) ||
            '            -- only convert if the count is (exactly) one,' || chr(10) ||
            '            -- otherwise conversion doesn''t make sense' || chr(10) ||
            '            IF convert_measure = 1 THEN' || chr(10) ||
            '                -- get the target unit and the units cube used for conversion' || chr(10) ||
            '                -- the following raised a generic error!' || chr(10) ||
            '                -- SELECT measure_unit, conversion_rule INTO unit_any, units_cube_any' || chr(10) ||
            '                -- FROM   TABLE(measure_units)' || chr(10) ||
            '                -- WHERE  UPPER(measure_name) = UPPER(measure_list(i));' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the target unit and the units cube used for conversion' || chr(10) ||
            '                j := measure_units.FIRST;' || chr(10) ||
            '                WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                    IF UPPER(measure_units(j).measure_name) = UPPER(measure_list(i)) THEN' || chr(10) ||
            '                        unit_any := measure_units(j).measure_unit;' || chr(10) ||
            '                        units_cube_any := measure_units(j).conversion_rule;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        j := NULL;' || chr(10) ||
            '                    ELSE' || chr(10) ||
            '                        j := measure_units.NEXT(j);' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the references' || chr(10) ||
            '                status := ANYDATA.getRef(unit_any, unit_ref);' || chr(10) ||
            '                status := ANYDATA.getRef(units_cube_any, units_cube_ref);' || chr(10) ||
            '                ' || chr(10) ||
            '                -- select the objects' || chr(10) ||
            '                utl_ref.select_object(unit_ref, unit_obj);' || chr(10) ||
            '                utl_ref.select_object(units_cube_ref, units_cube_obj);' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the name of the unit''s dimension' || chr(10) ||
            '                SELECT d.dname INTO unit_dname' || chr(10) ||
            '                FROM   dimensions d' || chr(10) ||
            '                WHERE  REF(d) = unit_obj.dim;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the name of the current measure' || chr(10) ||
            '                measure_name := measure_list(i);' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the name of the units cube' || chr(10) ||
            '                units_cname := units_cube_obj.cname;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the dimension names of the units cube' || chr(10) ||
            '                units_cube_dimension_names :=' || chr(10) ||
            '                    units_cube_obj.get_dimension_names();' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the different connection levels of this measure' || chr(10) ||
            '                measure_descriptions :=' || chr(10) ||
            '                    SELF.get_measure_descriptions(measure_name);' || chr(10) ||
            '                ' || chr(10) ||
            '                -- a rollup of the units cube has to be done for' || chr(10) ||
            '                -- every connection level.' || chr(10) ||
            '                j := measure_descriptions.FIRST;' || chr(10) ||
            '                WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                    IF rollup_tables_by_cube.EXISTS(units_cname) THEN' || chr(10) ||
            '                        IF rollup_tables_by_cube(units_cname).EXISTS(measure_descriptions(j).measure_level.to_string2()) THEN' || chr(10) ||
            '                            rollup_table := ' || chr(10) ||
            '                                 rollup_tables_by_cube(units_cname)(measure_descriptions(j).measure_level.to_string2());' || chr(10) ||
            '                        ELSE' || chr(10) ||
            '                            rollup_table := NULL;' || chr(10) ||
            '                        END IF;' || chr(10) ||
            '                    ELSE' || chr(10) ||
            '                        rollup_table := NULL;' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    -- if there is no rollup table for this level' || chr(10) ||
            '                    -- then create a new one' || chr(10) ||
            '                    IF rollup_table IS NULL THEN' || chr(10) ||
            '                        -- create a unique table name for the rollup table' || chr(10) ||
            '                        rollup_table :=' || chr(10) ||
            '                            identifiers.get_unique_short_name(30,' || chr(10) ||
            '                                                              UPPER(units_cube_obj.id || ''_rollup_'' || measure_descriptions(j).measure_level.to_string()),' || chr(10) ||
            '                                                              ''user_tab_columns'',' || chr(10) ||
            '                                                              ''table_name'');' || chr(10) ||
            '                        ' || chr(10) ||
            '                        -- save the table name' || chr(10) ||
            '                        rollup_tables_by_cube(units_cname)(measure_descriptions(j).measure_level.to_string2()) :=' || chr(10) ||
            '                            rollup_table;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        rollup_table_args := NULL;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        -- get the rollup levels' || chr(10) ||
            '                        k := units_cube_dimension_names.FIRST;' || chr(10) ||
            '                        WHILE k IS NOT NULL LOOP' || chr(10) ||
            '                            IF unit_dname = units_cube_dimension_names(k) THEN' || chr(10) ||
            '                                rollup_table_args := rollup_table_args || ' || chr(10) ||
            '                                    '', '''''' || unit_obj.top_level || '''''''';' || chr(10) ||
                                         mcube_2rollup_elsifs ||
            '                            ELSE' || chr(10) ||
            '                                -- get the top level of the dimension' || chr(10) ||
            '                                SELECT h.lvl INTO rollup_level' || chr(10) ||
            '                                FROM   dimensions d, TABLE(d.level_hierarchy) h' || chr(10) ||
            '                                WHERE  d.dname = units_cube_dimension_names(k) AND' || chr(10) ||
            '                                       h.parent_level IS NULL;' || chr(10) ||
            '                                ' || chr(10) ||
            '                                rollup_table_args := rollup_table_args || ' || chr(10) ||
            '                                    '', '''''' || rollup_level || '''''''';' || chr(10) ||
            '                            END IF;' || chr(10) ||
            '                            ' || chr(10) ||
            '                            k := units_cube_dimension_names.NEXT(k);' || chr(10) ||
            '                        END LOOP;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        -- ' || chr(10) ||
            '                        EXECUTE IMMEDIATE' || chr(10) ||
            '                            ''DECLARE'' || chr(10) ||' || chr(10) ||
            '                            ''    mcube_ref REF mcube_ty := :1;'' || chr(10) ||' || chr(10) ||
            '                            ''    mcube_obj mcube_ty;'' || chr(10) ||' || chr(10) ||
            '                            ''    mcube_#_obj '' || units_cube_obj.mcube_#_ty || '';'' || chr(10) ||' || chr(10) ||
            '                            ''BEGIN'' || chr(10) ||' || chr(10) ||
            '                            ''    utl_ref.select_object(mcube_ref, mcube_obj);'' || chr(10) ||' || chr(10) ||
            '                            ''    mcube_#_obj := TREAT(mcube_obj AS '' || units_cube_obj.mcube_#_ty || '');'' || chr(10) ||' || chr(10) ||
            '                            ''    '' || chr(10) ||' || chr(10) ||
            '                            ''    mcube_#_obj.rollup(:2, FALSE '' || rollup_table_args || '');'' || chr(10) ||' || chr(10) ||
            '                            ''END;''' || chr(10) ||
            '                            USING units_cube_ref, rollup_table;' || chr(10) ||
            '                        ' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    j := measure_descriptions.NEXT(j);' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            -- reset' || chr(10) ||
            '            select_rollup := NULL;' || chr(10) ||
            '            select_rollup_select := NULL;' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_name := measure_list(i);' || chr(10) ||
            '            measure_descriptions :=' || chr(10) ||
            '                SELF.get_measure_descriptions(measure_name);' || chr(10) ||
            '            ' || chr(10) ||
            '            -- check if this measure should be converted' || chr(10) ||
            '            SELECT COUNT(*) INTO convert_measure' || chr(10) ||
            '            FROM   TABLE(measure_units)' || chr(10) ||
            '            WHERE  UPPER(measure_name) = UPPER(measure_list(i));' || chr(10) ||
            '            ' || chr(10) ||
            '            IF convert_measure = 1 THEN' || chr(10) ||
            '                -- get the target unit and the units cube used for conversion' || chr(10) ||
            '                -- the following raised a generic error!' || chr(10) ||
            '                -- SELECT measure_unit, conversion_rule INTO unit_any, units_cube_any' || chr(10) ||
            '                -- FROM   TABLE(measure_units)' || chr(10) ||
            '                -- WHERE  UPPER(measure_name) = UPPER(measure_list(i));' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the target unit and the units cube used for conversion' || chr(10) ||
            '                j := measure_units.FIRST;' || chr(10) ||
            '                WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                    IF UPPER(measure_units(j).measure_name) = UPPER(measure_list(i)) THEN' || chr(10) ||
            '                        unit_any := measure_units(j).measure_unit;' || chr(10) ||
            '                        units_cube_any := measure_units(j).conversion_rule;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        j := NULL;' || chr(10) ||
            '                    ELSE' || chr(10) ||
            '                        j := measure_units.NEXT(j);' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get the references' || chr(10) ||
            '                status := ANYDATA.getRef(unit_any, unit_ref);' || chr(10) ||
            '                status := ANYDATA.getRef(units_cube_any, units_cube_ref);' || chr(10)||
            '                ' || chr(10) ||
            '                -- select the objects' || chr(10) ||
            '                utl_ref.select_object(unit_ref, unit_obj);' || chr(10) ||
            '                utl_ref.select_object(units_cube_ref, units_cube_obj);' || chr(10) ||
            '                ' || chr(10) ||
            '                units_cname := units_cube_obj.cname;' || chr(10) ||
            '                ' || chr(10) ||
            '                dim_ref := unit_obj.dim;' || chr(10) ||
            '                utl_ref.select_object(dim_ref, dim_obj);' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            BEGIN' || chr(10) ||
            '                SELECT DISTINCT ANYDATA.accessVarchar2(meta.measure_value) INTO aggregation_function' || chr(10) ||
            '                FROM   ' || mrel_table || ' mr, TABLE(mr.measure_metadata) meta' || chr(10) ||
            '                WHERE  UPPER(meta.measure_name) = UPPER(measure_list(i)) AND meta.metalevel = ''function'';' || chr(10) ||
            '            EXCEPTION' || chr(10) ||
            '                WHEN NO_DATA_FOUND THEN' || chr(10) ||
            '                    aggregation_function := ''SUM'';' || chr(10) ||
            '            END;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF convert_measure = 0 THEN' || chr(10) ||
            '                select_rollup_select := aggregation_function || ''(m.'' || measure_name || '') AS '' || measure_name;' || chr(10) ||
            '            ELSE' || chr(10) ||
            '                -- if the measure should be converted, multiply with a factor' || chr(10) ||
            '                select_rollup_select := aggregation_function || ''(m.'' || measure_name || '' * r.'' || unit_obj.oname || '') AS '' || measure_name;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            j := measure_descriptions.FIRST;' || chr(10) ||
            '            WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                IF j > 1 THEN' || chr(10) ||
            '                    select_rollup := select_rollup || chr(10) || '' UNION '' || chr(10) || chr(10);' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                select_rollup_from := NULL;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- ' || chr(10) ||
            '                measure_table := measure_descriptions(j).table_name;' || chr(10) ||
            '                ' || chr(10) ||
                             mcube_rollup2_from1 ||
            '                ' || chr(10) ||
            '                IF convert_measure > 0 THEN' || chr(10) ||
            '                    conlevel_string := measure_descriptions(j).measure_level.to_string2();' || chr(10) ||
            '                    rollup_table := rollup_tables_by_cube(units_cname)(conlevel_string);' || chr(10) ||
            '                    ' || chr(10) ||
            '                    -- ' || chr(10) ||
            '                    select_rollup_from1 := ' || chr(10) ||
            '                        ''            '' || rollup_table || '' r,'' || chr(10);' || chr(10) ||
            '                    ' || chr(10) ||
            '                    -- get the dimension names of the units cube' || chr(10) ||
            '                    units_cube_dimension_names :=' || chr(10) ||
            '                        units_cube_obj.get_dimension_names();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    k := units_cube_dimension_names.FIRST;' || chr(10) ||
            '                    WHILE k IS NOT NULL LOOP' || chr(10) ||
            '                        IF units_cube_dimension_names(k) = dim_obj.dname THEN' || chr(10) ||
            '                            select_rollup_where := select_rollup_where ||' || chr(10) ||
            '                                '' AND UPPER(m.'' || measure_name || ''_unit) = UPPER(r.'' || dim_obj.dname || '')'';' || chr(10) ||
                                     mcube_rollup2_from2 ||
            '                        END IF;' || chr(10) ||
            '                        ' || chr(10) ||
            '                        k := units_cube_dimension_names.NEXT(k);' || chr(10) ||
            '                    END LOOP;' || chr(10) ||
            '                    ' || chr(10) ||
            '                ELSE' || chr(10) ||
            '                    select_rollup_from1 := NULL;' || chr(10) ||
            '                    select_rollup_where := NULL;' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                select_rollup := select_rollup || ' || chr(10) ||
            '                    ''  (SELECT   ' || mcube_rollup2_select || ''' || select_rollup_select || chr(10) ||' || chr(10) ||
            '                    ''   FROM '' || chr(10) || select_rollup_from || chr(10) ||' || chr(10) ||
            '                                  select_rollup_from1 || ' || chr(10) ||
            '                    ''            '' || measure_table || '' m'' || chr(10) ||' || chr(10) ||
            '                    ''   WHERE    ' || mcube_rollup2_where || ''' || select_rollup_where || chr(10) ||' || chr(10) ||
            '                    ''   GROUP BY ' || mcube_rollup2_group_by || ')'' || chr(10);' || chr(10) ||
            '                ' || chr(10) ||
            '                j := measure_descriptions.NEXT(j);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_rollup_select := aggregation_function || ''('' || measure_name || '') AS '' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_rollup := ' || chr(10) ||
            '                ''SELECT ' || mcube_rollup2_select_union || ''' || select_rollup_select || chr(10) ||' || chr(10) ||
            '                ''FROM ('' || chr(10) ||' || chr(10) ||
            '                   select_rollup ||' || chr(10) ||
            '                '') GROUP BY ' || mcube_rollup2_grp_by_union || ''' || chr(10);' || chr(10) ||
            '            ' || chr(10) ||
            '            select_measures(measure_name) := select_rollup;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            measure_name := measure_list(i);' || chr(10) ||
            '            ' || chr(10) ||
            '            IF i > 1 THEN' || chr(10) ||
            '                select_join := select_join || chr(10) || chr(10) || '' FULL OUTER JOIN '' || chr(10) || chr(10);' || chr(10) ||
            '                create_table1 := create_table1 || '', '' || chr(10);' || chr(10) ||
            '                select_join_select_measures := select_join_select_measures || '', '';' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_descriptions :=' || chr(10) ||
            '                SELF.get_measure_descriptions(measure_name);' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_datatype := ' || chr(10) ||
            '                measure_descriptions(measure_descriptions.FIRST).data_type;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- TODO: Make sure VARCHAR2 works as a data type' || chr(10) ||
            '            create_table1 :=  create_table1 || ''    '' || measure_name || '' '' || measure_datatype;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- find out if the data type of the measure is a collection' || chr(10) ||
            '            BEGIN' || chr(10) ||
            '                SELECT ut.typecode INTO typecode' || chr(10) ||
            '                FROM   user_types ut' || chr(10) ||
            '                WHERE  ut.type_name = UPPER(measure_datatype);' || chr(10) ||
            '            EXCEPTION' || chr(10) ||
            '                WHEN NO_DATA_FOUND THEN' || chr(10) ||
            '                    typecode := ''BUILT_IN'';' || chr(10) ||
            '            END;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF typecode = ''COLLECTION'' THEN ' || chr(10) ||
            '                nested_table_name1 := table_name || ''_'' || measure_name;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get a unique table name that is at most 30 bytes long.' || chr(10) ||
            '                nested_table_name := ' || chr(10) ||
            '                    identifiers.get_unique_short_name(30,' || chr(10) ||
            '                                                      UPPER(nested_table_name1),' || chr(10) ||
            '                                                      ''user_tab_columns'',' || chr(10) ||
            '                                                      ''table_name'');' || chr(10) ||
            '                ' || chr(10) ||
            '                create_table2 := create_table2 ||' || chr(10) ||
            '                    ''NESTED TABLE '' || measure_name || '' STORE AS '' || nested_table_name || chr(10);' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_join_select_measures := select_join_select_measures || measure_name || ''.'' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_join := select_join || '' ('' || select_measures(measure_name) || '') '' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF i > 1 THEN' || chr(10) ||
            '                select_join := select_join || '' ON ('' ' || mcube_rollup2_join_attr || ' || '')'';' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        create_table := ' || chr(10) ||
            '            ''CREATE TABLE '' || table_name || ''('' || chr(10) ||' || chr(10) ||
                             mcube_rollup2_create_tab ||
            '                create_table1 || chr(10) ||' || chr(10) ||
            '            '') '' || chr(10) || create_table2;' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_output.put_line(create_table);' || chr(10) ||
            '        EXECUTE IMMEDIATE create_table;' || chr(10) ||
            '        ' || chr(10) ||
            '        select_join := ''SELECT '' || ' || mcube_rollup2_join_select || ' select_join_select_measures || chr(10) ||' || chr(10) ||
            '                       ''FROM   '' || chr(10) || select_join;' || chr(10) ||
            '        ' || chr(10) ||
            '        --dbms_output.put_line(select_join);' || chr(10) ||
            '        ' || chr(10) ||
            '        insert_data := ' || chr(10) ||
            '            ''INSERT INTO '' || table_name || '' ('' || select_join || '')'';' || chr(10) ||
            '        ' || chr(10) ||
            '        --dbms_output.put_line(insert_data);' || chr(10) ||
            '        ' || chr(10) ||
            '        sql_cursor := dbms_sql.open_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_sql.parse(sql_cursor,' || chr(10) ||
            '                       insert_data,' || chr(10) ||
            '                       dbms_sql.native);' || chr(10) ||
            '        ' || chr(10) ||
                     mcube_rollup2_bind_var ||
            '        ' || chr(10) ||
            '        rows_processed := dbms_sql.execute(sql_cursor);' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_sql.close_cursor(sql_cursor);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- drop the rollup tables' || chr(10) ||
            '        units_cname := rollup_tables_by_cube.FIRST;' || chr(10) ||
            '        WHILE units_cname IS NOT NULL LOOP' || chr(10) ||
            '            conlevel_string := rollup_tables_by_cube(units_cname).FIRST;' || chr(10) ||
            '            WHILE conlevel_string IS NOT NULL LOOP' || chr(10) ||
            '                EXECUTE IMMEDIATE' || chr(10) ||
            '                    ''DROP TABLE '' || rollup_tables_by_cube(units_cname)(conlevel_string);' || chr(10) ||
            '                ' || chr(10) ||
            '                --dbms_output.put_line(''DROP TABLE '' || rollup_tables_by_cube(units_cname)(conlevel_string));' || chr(10) ||
            '                ' || chr(10) ||
            '                conlevel_string := rollup_tables_by_cube(units_cname).NEXT(conlevel_string);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '            ' || chr(10) ||
            '            units_cname := rollup_tables_by_cube.NEXT(units_cname);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mcube_rollup :=
            '    MEMBER PROCEDURE rollup(table_name VARCHAR2, include_non_dimension_attr BOOLEAN, ' || mcube_rollup1 || ') IS' || chr(10) ||
            '        ' || chr(10) ||
            '        mc ' || mcube_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        TYPE string_tty IS TABLE OF VARCHAR2(10000) INDEX BY VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_measures string_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        create_table  VARCHAR2(2000);' || chr(10) ||
            '        create_table1 VARCHAR2(1000);' || chr(10) ||
            '        create_table2 VARCHAR2(2000);' || chr(10) ||
            '        insert_data   VARCHAR2(30000);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_join                 VARCHAR2(20000);' || chr(10) ||
            '        select_join_select_dim      VARCHAR2(5000);' || chr(10) ||
            '        select_join_select_measures VARCHAR2(5000);' || chr(10) ||
            '        ' || chr(10) ||
            '        select_rollup        VARCHAR2(10000);' || chr(10) ||
            '        select_rollup_from   VARCHAR2(5000);' || chr(10) ||
            '        select_rollup_select VARCHAR2(4000);' || chr(10) ||
            '        ' || chr(10) ||
                     mcube_rollup_var_decl ||
            '        ' || chr(10) ||
                     mcube_rollup_join_sel_var ||
            '        ' || chr(10) ||
            '        measure_list names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_name         VARCHAR2(30);' || chr(10) ||
            '        measure_descriptions ' || measure_#_tty || ';' || chr(10) ||
            '        measure_table        VARCHAR2(30);' || chr(10) ||
            '        measure_datatype     VARCHAR2(30);' || chr(10) ||
            '        typecode             VARCHAR2(100);' || chr(10) ||
            '        ' || chr(10) ||
            '        nested_table_name  VARCHAR2(30);' || chr(10) ||
            '        nested_table_name1 VARCHAR2(60);' || chr(10) ||
            '        ' || chr(10) ||
            '        aggregation_function VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_level_pos INTEGER;' || chr(10) ||
            '        rollup_level_pos INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '        j INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        sql_cursor INTEGER;' || chr(10) ||
            '        rows_processed INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        ' || chr(10) ||
                     mcube_rollup_from || chr(10) ||
            '        ' || chr(10) ||
            '        -- get all measures' || chr(10) ||
            '        SELECT DISTINCT utc.column_name BULK COLLECT INTO measure_list' || chr(10) ||
            '        FROM   user_tab_columns utc, ' || chr(10) ||
            '               (SELECT UPPER(t.table_name) AS table_name' || chr(10) ||
            '                FROM   ' || mrel_table || ' o, TABLE(o.measure_tables) t) x' || chr(10) ||
            '        WHERE  utc.table_name IN x.table_name AND ' || chr(10) ||
            '               utc.column_name <> ''COORDINATE'' AND ' || chr(10) ||
            '               utc.column_name <> ''MREL'' AND utc.column_name NOT LIKE ''%_UNIT'';' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.LAST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            IF i = measure_list.LAST THEN' || chr(10) ||
                             mcube_rollup_join_select1 ||
            '            ELSE' || chr(10) ||
                             mcube_rollup_join_select2 ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.PRIOR(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            -- reset' || chr(10) ||
            '            select_rollup := NULL;' || chr(10) ||
            '            select_rollup_select := NULL;' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_name := measure_list(i);' || chr(10) ||
            '            measure_descriptions :=' || chr(10) ||
            '                SELF.get_measure_descriptions(measure_name);' || chr(10) ||
            '            ' || chr(10) ||
            '            BEGIN' || chr(10) ||
            '                SELECT DISTINCT ANYDATA.accessVarchar2(meta.measure_value) INTO aggregation_function' || chr(10) ||
            '                FROM   ' || mrel_table || ' mr, TABLE(mr.measure_metadata) meta' || chr(10) ||
            '                WHERE  UPPER(meta.measure_name) = UPPER(measure_list(i)) AND meta.metalevel = ''function'';' || chr(10) ||
            '            EXCEPTION' || chr(10) ||
            '                WHEN NO_DATA_FOUND THEN' || chr(10) ||
            '                    aggregation_function := ''SUM'';' || chr(10) ||
            '            END;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_rollup_select := aggregation_function || ''(m.'' || measure_name || '') AS '' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            j := measure_descriptions.FIRST;' || chr(10) ||
            '            WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                IF j > 1 THEN' || chr(10) ||
            '                    select_rollup := select_rollup || chr(10) || '' UNION '' || chr(10) || chr(10);' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                select_rollup_from := NULL;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- ' || chr(10) ||
            '                measure_table := measure_descriptions(j).table_name;' || chr(10) ||
            '                ' || chr(10) ||
                             mcube_rollup_from1 ||
            '                ' || chr(10) ||
            '                select_rollup := select_rollup || ' || chr(10) ||
            '                    ''  (SELECT   ' || mcube_rollup_select || ''' || select_rollup_select || chr(10) ||' || chr(10) ||
            '                    ''   FROM '' || chr(10) || select_rollup_from || chr(10) ||' || chr(10) ||
            '                    ''            '' || measure_table || '' m'' || chr(10) ||' || chr(10) ||
            '                    ''   WHERE    ' || mcube_rollup_where || ''' || chr(10) ||' || chr(10) ||
            '                    ''   GROUP BY ' || mcube_rollup_group_by || ')'' || chr(10);' || chr(10) ||
            '                ' || chr(10) ||
            '                j := measure_descriptions.NEXT(j);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_rollup_select := aggregation_function || ''('' || measure_name || '') AS '' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_rollup := ' || chr(10) ||
            '                ''SELECT ' || mcube_rollup_select_union || ''' || select_rollup_select || chr(10) ||' || chr(10) ||
            '                ''FROM ('' || chr(10) ||' || chr(10) ||
            '                   select_rollup ||' || chr(10) ||
            '                '') GROUP BY ' || mcube_rollup_grp_by_union || ''' || chr(10);' || chr(10) ||
            '            ' || chr(10) ||
            '            select_measures(measure_name) := select_rollup;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        i := measure_list.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            measure_name := measure_list(i);' || chr(10) ||
            '            ' || chr(10) ||
            '            IF i > 1 THEN' || chr(10) ||
            '                select_join := select_join || chr(10) || chr(10) || '' FULL OUTER JOIN '' || chr(10) || chr(10);' || chr(10) ||
            '                create_table1 := create_table1 || '', '' || chr(10);' || chr(10) ||
            '                select_join_select_measures := select_join_select_measures || '', '';' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_descriptions :=' || chr(10) ||
            '                SELF.get_measure_descriptions(measure_name);' || chr(10) ||
            '            ' || chr(10) ||
            '            measure_datatype := ' || chr(10) ||
            '                measure_descriptions(measure_descriptions.FIRST).data_type;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- TODO: Make sure VARCHAR2 works as a data type' || chr(10) ||
            '            create_table1 :=  create_table1 || ''    '' || measure_name || '' '' || measure_datatype;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- find out if the data type of the measure is a collection' || chr(10) ||
            '            BEGIN' || chr(10) ||
            '                SELECT ut.typecode INTO typecode' || chr(10) ||
            '                FROM   user_types ut' || chr(10) ||
            '                WHERE  ut.type_name = UPPER(measure_datatype);' || chr(10) ||
            '            EXCEPTION' || chr(10) ||
            '                WHEN NO_DATA_FOUND THEN' || chr(10) ||
            '                    typecode := ''BUILT_IN'';' || chr(10) ||
            '            END;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF typecode = ''COLLECTION'' THEN ' || chr(10) ||
            '                nested_table_name1 := table_name || ''_'' || measure_name;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get a unique table name that is at most 30 bytes long.' || chr(10) ||
            '                nested_table_name := ' || chr(10) ||
            '                    identifiers.get_unique_short_name(30,' || chr(10) ||
            '                                                      UPPER(nested_table_name1),' || chr(10) ||
            '                                                      ''user_tab_columns'',' || chr(10) ||
            '                                                      ''table_name'');' || chr(10) ||
            '                ' || chr(10) ||
            '                create_table2 := create_table2 ||' || chr(10) ||
            '                    ''NESTED TABLE '' || measure_name || '' STORE AS '' || nested_table_name || chr(10);' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_join_select_measures := select_join_select_measures || measure_name || ''.'' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            select_join := select_join || '' ('' || select_measures(measure_name) || '') '' || measure_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF i > 1 THEN' || chr(10) ||
            '                select_join := select_join || '' ON ('' ' || mcube_rollup_join_attr || ' || '')'';' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := measure_list.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        create_table := ' || chr(10) ||
            '            ''CREATE TABLE '' || table_name || ''('' || chr(10) ||' || chr(10) ||
                             mcube_rollup_create_tab ||
            '                create_table1 || chr(10) ||' || chr(10) ||
            '            '') '' || chr(10) || create_table2;' || chr(10) ||
            '        ' || chr(10) ||
            '        --dbms_output.put_line(create_table);' || chr(10) ||
            '        EXECUTE IMMEDIATE create_table;' || chr(10) ||
            '        ' || chr(10) ||
            '        select_join := ''SELECT '' || ' || mcube_rollup_join_select || ' select_join_select_measures || chr(10) ||' || chr(10) ||
            '                       ''FROM   '' || chr(10) || select_join;' || chr(10) ||
            '        ' || chr(10) ||
            '        --dbms_output.put_line(select_join);' || chr(10) ||
            '        ' || chr(10) ||
            '        insert_data := ' || chr(10) ||
            '            ''INSERT INTO '' || table_name || '' ('' || select_join || '')'';' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_output.put_line(insert_data);' || chr(10) ||
            '        ' || chr(10) ||
            '        sql_cursor := dbms_sql.open_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_sql.parse(sql_cursor,' || chr(10) ||
            '                       insert_data,' || chr(10) ||
            '                       dbms_sql.native);' || chr(10) ||
            '        ' || chr(10) ||
                     mcube_rollup_bind_var ||
            '        ' || chr(10) ||
            '        rows_processed := dbms_sql.execute(sql_cursor);' || chr(10) ||
            '        ' || chr(10) ||
            '        dbms_sql.close_cursor(sql_cursor);' || chr(10) ||
            '    EXCEPTION' || chr(10) ||
            '        WHEN OTHERS THEN EXECUTE IMMEDIATE ''DROP '' || table_name || '';'';' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10);
        
        
        mcube_get_nearest_mrel :=
            '    MEMBER FUNCTION get_nearest_mrel(' || mcube_get_nearest_mrel1 || ') RETURN ' || mrel_#_trty || ' IS ' || chr(10) ||
            '        ' || chr(10) ||
            '        cnt INTEGER := 0;' || chr(10) ||
            '        ' || chr(10) ||
                     mcube_get_nearest_mrel2 ||
            '        ' || chr(10) ||
            '        result_mrel REF ' || mrel_#_ty || ';' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        ' || chr(10) ||
                     mcube_get_nearest_mrel3 ||
            '        ' || chr(10) ||
            '        ' || chr(10) ||
                     mcube_get_nearest_mrel4 ||
            '            ' || chr(10) ||
            '            SELECT COUNT(*) INTO cnt' || chr(10) ||
            '            FROM   ' || mrel_table || ' r' || chr(10) ||
            '            WHERE  ' || mcube_get_nearest_mrel5 || ';' || chr(10) ||
            '            ' || chr(10) ||
            '            IF cnt > 0 THEN' || chr(10) ||
            '                SELECT REF(r) INTO result_mrel' || chr(10) ||
            '                FROM   ' || mrel_table || ' r' || chr(10) ||
            '                WHERE  ' || mcube_get_nearest_mrel5 || ';' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
                     mcube_get_nearest_mrel6 ||
            '        ' || chr(10) ||
            '        RETURN ' || mrel_#_trty || '(result_mrel);' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10);
        
        mcube_body :=
            'CREATE OR REPLACE TYPE BODY ' || mcube_#_ty || ' IS' || chr(10) ||
                 mcube_constructor ||
                 mcube_constructor_alt ||
                 mcube_create_mrel ||
                 mcube_create2_mrel ||
                 mcube_create3_mrel ||
                 mcube_bulk_create_mrel ||
                 mcube_bulk_create_mrel_heu ||
                 mcube_get_mrel_ref ||
                 mcube_get2_mrel_ref ||
                 mcube_get3_mrel_ref ||
                 mcube_get_measure_descr ||
                 mcube_get_measure_descrs ||
                 mcube_get_measure_unit ||
                 mcube_get_measure_funct ||
                 mcube_bulk_set_measure ||
                 mcube_bulk_set_measure_heu ||
                 mcube_refresh_meas_unit_cache ||
                 mcube_new_queryview ||
                 mcube_persist ||
                 mcube_get_dimension_names ||
                 mcube_get_dimension_ids ||
                 mcube_export_star ||
                 mcube_rollup ||
                 mcube_rollup_unit_aware ||
                 mcube_get_nearest_mrel ||
            'END;';
        
        --dbms_output.put_line(mcube_body);
        EXECUTE IMMEDIATE mcube_body;
    END;
    
    PROCEDURE create_mrel_body(mrel_#_ty             VARCHAR2,
                               mrel_#_trty           VARCHAR2,
                               dimensions            names_tty,
                               dim_ids               names_tty,
                               coordinate_#_ty       VARCHAR2,
                               conlevel_#_ty         VARCHAR2,
                               conlevel_#_tty        VARCHAR2,
                               conlvl_ancestor_#_ty  VARCHAR2,
                               conlvl_ancestor_#_tty VARCHAR2,
                               measure_#_ty          VARCHAR2,
                               measure_#_tty         VARCHAR2,
                               measure_table_#_ty    VARCHAR2,
                               measure_table_#_tty   VARCHAR2,
                               measure_meta_#_ty     VARCHAR2,
                               measure_meta_#_tty    VARCHAR2,
                               measure_#_collections VARCHAR2,
                               cname                 VARCHAR2,
                               mrel_table            VARCHAR2,
                               mobject_tables        names_tty) IS
        
        mrel_body               CLOB;
        mrel_constructor        CLOB;
        mrel_does_specialize    CLOB;
        mrel_delete             CLOB;
        mrel_calc_ancestors          CLOB;
        mrel_calc_ancestors_vars     CLOB;
        mrel_calc_ancestors_mobj_anc CLOB;
        mrel_calc_ancestors_select1  CLOB;
        mrel_calc_ancestors_where1   CLOB;
        mrel_top_level          CLOB;
        mrel_top_level1         CLOB;
        mrel_top_level_var1     CLOB;
        mrel_top_level_var2     CLOB;
        mrel_add_measure        CLOB;
        mrel_add_measure_pk     CLOB;
        mrel_set_measure        CLOB;
        mrel_set2_measure            CLOB;
        mrel_set2_measure_coord_cmp  CLOB;
        mrel_set2_measure_coord_bind CLOB;
        mrel_delete_measure     CLOB;
        mrel_get_measure        CLOB;
        mrel_get_measure_unit   CLOB;
        mrel_has_measure            CLOB;
        mrel_has_measure_order_anc1 CLOB;
        mrel_has_measure_order_anc2 CLOB;
        mrel_has_measure_order_anc3 CLOB;
        mrel_has2_measure       CLOB;
        mrel_list_measures      CLOB;
        mrel_list2_measures     CLOB;
        mrel_get_measure_tab          CLOB;
        mrel_get_measure_tab_coordcmp CLOB;
        mrel_get_measure_tab_lvlcmp   CLOB;
        mrel_has_conlevel  CLOB;
        mrel_has_conlevel1 CLOB;
        mrel_has2_conlevel        CLOB;
        mrel_has2_conlevel1       CLOB;
        mrel_has2_conlevel_select CLOB;
        mrel_persist            CLOB;
        mrel_init_measure_table CLOB;
        mrel_init_coord_cmp     CLOB;
        mrel_init_coord_bind    CLOB;
        
        i INTEGER;
    BEGIN
        -- loop through the names of the dimensions
        i := dim_ids.FIRST;
        WHILE i IS NOT NULL LOOP
            IF i > 1 THEN
                mrel_has_conlevel1 := mrel_has_conlevel1 || ', ';
                mrel_has2_conlevel1 := mrel_has2_conlevel1 || ', ';
                mrel_get_measure_tab_coordcmp := mrel_get_measure_tab_coordcmp || ' AND ';
                mrel_get_measure_tab_lvlcmp := mrel_get_measure_tab_lvlcmp || ' AND ';
                mrel_top_level1 := mrel_top_level1 || ', ';
                mrel_set2_measure_coord_cmp := mrel_set2_measure_coord_cmp || ' AND ';
                mrel_init_coord_cmp := mrel_init_coord_cmp || ' AND ';
                mrel_init_coord_bind := mrel_init_coord_bind || ', ';
                mrel_calc_ancestors_select1 := mrel_calc_ancestors_select1 || ', ';
                mrel_calc_ancestors_where1 := mrel_calc_ancestors_where1 || ' AND ';
                mrel_add_measure_pk := mrel_add_measure_pk || ', ';
                mrel_has_measure_order_anc1 := mrel_has_measure_order_anc1 || ', ' || chr(10);
                mrel_has_measure_order_anc2 := mrel_has_measure_order_anc2 || ' AND ' || chr(10);
                mrel_has_measure_order_anc3 := mrel_has_measure_order_anc3 || ' + ';
            END IF;
            
            mrel_has_measure_order_anc1 := mrel_has_measure_order_anc1 ||
                '                   TABLE(SELECT d.level_positions' || chr(10) ||
                '                         FROM   dimensions d' || chr(10) ||
                '                         WHERE  d.dname = ''' || dimensions(i) || ''') ' || dim_ids(i) || '_level' || chr(10);
            
            mrel_has_measure_order_anc2 := mrel_has_measure_order_anc2 ||
                '                   anc.conlevel.' || dim_ids(i) || '_level = ' || dim_ids(i) || '_level.lvl';
                
            mrel_has_measure_order_anc3 := mrel_has_measure_order_anc3 ||
                dim_ids(i) || '_level.position';
            
            mrel_has_conlevel1 := mrel_has_conlevel1 ||
                'conlevel.' || dim_ids(i) || '_level';  
                
            mrel_has2_conlevel1 := mrel_has2_conlevel1 ||
                dim_ids(i) || '_level VARCHAR2';       
            
            mrel_has2_conlevel_select := mrel_has2_conlevel_select ||
                '        IF cnt > 0 THEN' || chr(10) ||
                '            SELECT COUNT(*) INTO cnt' || chr(10) ||
                '            FROM   ' || mobject_tables(i) || ' o, TABLE(o.level_hierarchy) h' || chr(10) ||
                '            WHERE  o.oname = SELF.coordinate.' || dim_ids(i) || '_oname AND' || chr(10) ||
                '                   h.lvl = ' || dim_ids(i) || '_level;' || chr(10) ||
                '        END IF;' || chr(10) ||
                '        ' || chr(10);
                
            mrel_get_measure_tab_coordcmp := mrel_get_measure_tab_coordcmp ||
                'r.coordinate.' || dim_ids(i) || '_oname = SELF.coordinate.' || 
                dim_ids(i) || '_oname';
            
            mrel_get_measure_tab_lvlcmp := mrel_get_measure_tab_lvlcmp ||
                'l.conlevel.' || dim_ids(i) || 
                '_level = searched_level.' || dim_ids(i) || '_level';
            
            mrel_top_level1 := mrel_top_level1 ||
                dim_ids(i) || '_level';
            
            mrel_top_level_var1 := mrel_top_level_var1 ||
                '        ' || dim_ids(i) || '_level VARCHAR2(30);' || chr(10);
                
            mrel_top_level_var2 := mrel_top_level_var2 ||
                '        SELECT o.top_level INTO ' || dim_ids(i) || '_level' || chr(10) ||
                '        FROM   ' || mobject_tables(i) || ' o' || chr(10) ||
                '        WHERE  o.oname = SELF.coordinate.' || dim_ids(i) || '_oname;' || chr(10) ||
                '        ' || chr(10);
            
            mrel_set2_measure_coord_cmp := mrel_set2_measure_coord_cmp ||
                'mc.coordinate.' || dim_ids(i) || '_oname = :' || dim_ids(i) || '_oname';
            
            mrel_set2_measure_coord_bind := mrel_set2_measure_coord_bind ||
                '                dbms_sql.bind_variable(sql_cursor,' || chr(10) ||
                '                                       ''' || dim_ids(i) || '_oname'',' || chr(10) ||
                '                                       SELF.coordinate.' || dim_ids(i) || '_oname);' || chr(10) ||
                '                ' || chr(10);
            
            mrel_init_coord_cmp := mrel_init_coord_cmp ||
                'm.coordinate.' || dim_ids(i) || '_oname = :' || i;
            
            mrel_init_coord_bind := mrel_init_coord_bind ||
                'SELF.coordinate.' || dim_ids(i) || '_oname';
            
            mrel_calc_ancestors_vars := mrel_calc_ancestors_vars ||
                '        ' || dim_ids(i) || '_ancestor_onames names_tty;' || chr(10);
            
            mrel_calc_ancestors_mobj_anc := mrel_calc_ancestors_mobj_anc ||
                '        SELECT anc.ancestor.oname BULK COLLECT INTO ' || dim_ids(i) || '_ancestor_onames' || chr(10) ||
                '        FROM   ' || mobject_tables(i) || ' o, TABLE(o.ancestors) anc' || chr(10) ||
                '        WHERE  o.oname = SELF.coordinate.' || dim_ids(i) || '_oname;' || chr(10) ||
                '        ' || chr(10) ||
                '        ' || dim_ids(i) || '_ancestor_onames.EXTEND;' || chr(10) ||
                '        ' || dim_ids(i) || '_ancestor_onames(' || dim_ids(i) || '_ancestor_onames.LAST) :=' || chr(10) ||
                '            SELF.coordinate.' || dim_ids(i) || '_oname;' || chr(10) ||
                '        ' || chr(10);
            
            mrel_calc_ancestors_select1 := mrel_calc_ancestors_select1 ||
                'r.coordinate.' || dim_ids(i) || '_obj.top_level';
            
            
            mrel_calc_ancestors_where1 := mrel_calc_ancestors_where1 || chr(10) ||
                '               (' || chr(10) ||
                '                r.coordinate.' || dim_ids(i) || '_oname IN (SELECT x.column_value FROM TABLE(' || dim_ids(i) || '_ancestor_onames) x)' || chr(10) ||
                '               )';
            
            mrel_add_measure_pk := mrel_add_measure_pk ||
                'coordinate.' || dim_ids(i) || '_oname';            
            
            -- increment cursor variable
            i := dim_ids.NEXT(i);
        END LOOP;
        ----
        
        mrel_constructor :=
            '    CONSTRUCTOR FUNCTION ' || mrel_#_ty || '(coordinate ' || coordinate_#_ty || ', id VARCHAR2)'|| chr(10) ||
            '        RETURN SELF AS RESULT IS' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- get the reference to the mcube' || chr(10) ||
            '        SELECT REF(mc) INTO SELF.mcube' || chr(10) ||
            '        FROM   mcubes mc' || chr(10) ||
            '        WHERE  mc.cname = ''' || cname || ''';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- store the coordinate and the unique id' || chr(10) ||
            '        SELF.coordinate := coordinate;' || chr(10) ||
            '        SELF.id := id;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- initialize the nested table with the measure table names and metadata' || chr(10) ||
            '        SELF.measure_tables := ' || measure_table_#_tty || '();' || chr(10) ||
            '        SELF.measure_metadata := ' || measure_meta_#_tty || '();' || chr(10) ||
            '        ' || chr(10) ||
            '        -- calculate the ancestors' || chr(10) ||
            '        SELF.ancestors := SELF.calculate_ancestors();' || chr(10) ||
            '        ' || chr(10) ||
            '        IF SELF.ancestors IS NOT NULL AND SELF.ancestors.COUNT = 0 THEN' || chr(10) ||
            '            SELF.ancestors := NULL;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        IF SELF.does_specialize() THEN' || chr(10) ||
            '            SELF.specializes := 1;' || chr(10) ||
            '        ELSE' || chr(10) ||
            '            SELF.specializes := 0;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_delete := 
            '    OVERRIDING MEMBER PROCEDURE delete_mrel IS' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        NULL;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_calc_ancestors :=
            '    MEMBER FUNCTION calculate_ancestors RETURN ' || conlvl_ancestor_#_tty || ' IS' || chr(10) ||
            '        ancestors ' || conlvl_ancestor_#_tty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        cnt INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
                     mrel_calc_ancestors_vars ||
            '    BEGIN' || chr(10) ||
                     mrel_calc_ancestors_mobj_anc ||
            '        ' || chr(10) ||
            '        ' || chr(10) ||
            '        SELECT ' || conlvl_ancestor_#_ty || '(' || conlevel_#_ty || '(' || mrel_calc_ancestors_select1 || '), REF(r)) BULK COLLECT INTO ancestors' || chr(10) ||
            '        FROM   ' || mrel_table || ' r' || chr(10) ||
            '        WHERE  ' || mrel_calc_ancestors_where1 || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN ancestors;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
            
        mrel_top_level :=
            '    MEMBER FUNCTION top_level RETURN ' || conlevel_#_ty || ' IS' || chr(10) ||
                     mrel_top_level_var1 ||
            '    BEGIN' || chr(10) ||
                     mrel_top_level_var2 ||
            '        RETURN ' || conlevel_#_ty || '(' || mrel_top_level1 || ');' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_add_measure :=
            '    MEMBER PROCEDURE add_measure(measure_name     VARCHAR2,' || chr(10) ||
            '                                 measure_level    ' || conlevel_#_ty || ',' || chr(10) ||
            '                                 measure_datatype VARCHAR2) IS' || chr(10) ||
            '        table_name  VARCHAR2(90);' || chr(10) ||
            '        table_name1 VARCHAR2(30);' || chr(10) ||
            '        nested_table_name  VARCHAR2(90);' || chr(10) ||
            '        nested_table_name1 VARCHAR2(90);' || chr(10) ||
            '        row_count INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        mcube mcube_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        typecode VARCHAR2(100);' || chr(10) ||
            '        ' || chr(10) ||
            '        err error_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- get the m-cube' || chr(10) ||
            '        utl_ref.select_object(SELF.mcube, mcube);' || chr(10) ||
            '        ' || chr(10) ||
            '        IF mcube.enforce_consistency > 0 THEN' || chr(10) ||
            '            IF NOT SELF.has_connection_level(measure_level) THEN' || chr(10) ||
            '                SELECT VALUE(e) INTO err' || chr(10) ||
            '                FROM   errors e' || chr(10) ||
            '                WHERE  e.error_name = ''consistent_mrelationship_measure_connection_level_not_exists'';' || chr(10) ||
            '                ' || chr(10) ||
            '                err.raise_error;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- TODO: add further consistency checks!' || chr(10) ||
            '        END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '        -- check if there already is a table for the level of the newly' || chr(10) ||
            '        -- added attribute that can be used to store the attribute.' || chr(10) ||
            '        table_name := SELF.get_measure_table(measure_level);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- if the table does not exist, create the table' || chr(10) ||
            '        IF table_name IS NULL THEN' || chr(10) ||
            '            table_name := mcube.id || ''_'' || ' || chr(10) ||
            '                          SELF.id || ''_'' || ' || chr(10) ||
            '                          measure_level.to_string();' || chr(10) ||
            '           ' || chr(10) ||
            '            /* always get a unique name as the level''s string is not' || chr(10) || 
            '               guaranteed to be unique */' || chr(10) ||
            '            -- get a unique table name that is at most 30 bytes long.' || chr(10) ||
            '            table_name := ' || chr(10) ||
            '                identifiers.get_unique_short_name(30,' || chr(10) ||
            '                                                  UPPER(table_name),' || chr(10) ||
            '                                                  ''user_tab_columns'',' || chr(10) ||
            '                                                  ''table_name'');' || chr(10) ||
            '            ' || chr(10) ||
            '            -- create the table' || chr(10) ||
            '            EXECUTE IMMEDIATE' || chr(10) ||
            '                ''CREATE TABLE '' || table_name || ''('' || chr(10) ||' || chr(10) ||
            '                ''    coordinate '' || mcube.coordinate_#_ty || '','' || chr(10) ||' || chr(10) ||
            '                ''    PRIMARY KEY (' || mrel_add_measure_pk || '),'' || chr(10) ||' || chr(10) ||
            '                ''    mrel REF '' || mcube.mrel_#_ty || '' REFERENCES '' || ' || chr(10) ||
            '                 mcube.mrel_table || '' ON DELETE CASCADE '' || chr(10) ||' || chr(10) ||
            '                '')'';' || chr(10) ||
            '            ' || chr(10) ||
            '            SELF.measure_tables.extend;' || chr(10) ||
            '            SELF.measure_tables(SELF.measure_tables.LAST) :=' || chr(10) ||
            '                ' || measure_table_#_ty || '(measure_level, UPPER(table_name));' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- this is done for the query which would not work otherwise' || chr(10) ||
            '        table_name1 := table_name;' || chr(10) ||
            '        ' || chr(10) ||
            '        SELECT COUNT(*) INTO row_count ' || chr(10) ||
            '        FROM   user_tab_columns utc' || chr(10) ||
            '        WHERE  utc.table_name = UPPER(table_name1) AND ' || chr(10) ||
            '               utc.column_name = UPPER(measure_name);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- find out if the data type of the measure is a collection' || chr(10) ||
            '        BEGIN' || chr(10) ||
            '            SELECT ut.typecode INTO typecode' || chr(10) ||
            '            FROM   user_types ut' || chr(10) ||
            '            WHERE  ut.type_name = UPPER(measure_datatype);' || chr(10) ||
            '        EXCEPTION' || chr(10) ||
            '            WHEN NO_DATA_FOUND THEN' || chr(10) ||
            '                typecode := ''BUILT_IN'';' || chr(10) ||
            '        END;' || chr(10) ||
            '        ' || chr(10) ||
            '        IF (row_count = 0) THEN' || chr(10) ||
            '            IF typecode = ''COLLECTION'' THEN ' || chr(10) ||
            '                nested_table_name1 := table_name || ''_'' || measure_name;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- get a unique table name that is at most 30 bytes long.' || chr(10) ||
            '                nested_table_name := ' || chr(10) ||
            '                    identifiers.get_unique_short_name(30,' || chr(10) ||
            '                                                      UPPER(nested_table_name1),' || chr(10) ||
            '                                                      ''user_tab_columns'',' || chr(10) ||
            '                                                      ''table_name'');' || chr(10) ||
            '                ' || chr(10) ||
            '                EXECUTE IMMEDIATE' || chr(10) ||
            '                    ''ALTER TABLE '' || table_name || ' || chr(10) ||
            '                    '' ADD ('' || measure_name || '' '' || measure_datatype || '')'' || ' || chr(10) ||
            '                    '' NESTED TABLE '' || measure_name || '' STORE AS '' || nested_table_name;' || chr(10) ||
            '            ELSE' || chr(10) ||
            '                EXECUTE IMMEDIATE' || chr(10) ||
            '                    ''ALTER TABLE '' || table_name || ' || chr(10) ||
            '                    '' ADD '' || measure_name || '' '' || measure_datatype;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- make an additional column for the unit.' || chr(10) ||
            '            -- in this column, the name of the unit m-object is stored.' || chr(10) ||
            '            -- TODO: makes column name failsafe for long names' || chr(10) ||
            '            EXECUTE IMMEDIATE' || chr(10) ||
            '                ''ALTER TABLE '' || table_name || ' || chr(10) ||
            '                '' ADD '' || measure_name || ''_unit VARCHAR2(30)'';' || chr(10) ||
            '            ' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- an m-relationship specializes the schema when it adds a measure' || chr(10) ||
            '        SELF.specializes := 1;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- save the changes' || chr(10) ||
            '        SELF.persist;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_set_measure :=
            '    MEMBER PROCEDURE set_measure(measure_name  VARCHAR2,' || chr(10) ||
            '                                 metalevel     VARCHAR2,' || chr(10) ||
            '                                 default_value BOOLEAN,' || chr(10) ||
            '                                 measure_value ANYDATA) IS' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '        j INTEGER;' || chr(10) ||
            '        found BOOLEAN;' || chr(10) ||
            '        ' || chr(10) ||
            '        mc mcube_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        default_val NUMBER;' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_description ' || measure_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_lvl ' || conlevel_#_ty || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        err error_ty;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        IF (metalevel IS NULL AND NOT default_value) THEN' || chr(10) ||
            '            set_measure(measure_name, measure_value);' || chr(10) ||
            '        ELSE' || chr(10) ||
            '            -- get the m-cube object' || chr(10) ||
            '            utl_ref.select_object(SELF.mcube, mc);' || chr(10) ||
            '            ' || chr(10) ||
            '            -- check for consistency if the m-cube tells us so' || chr(10) ||
            '            IF mc.enforce_consistency > 0 THEN' || chr(10) ||
            '                -- TODO: add consistency checks' || chr(10) ||
            '                NULL;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF has_measure(measure_name, FALSE, FALSE, measure_description) THEN' || chr(10) ||
            '                measure_lvl := measure_description.measure_level;' || chr(10) ||
            '                ' || chr(10) ||
            '                IF default_value THEN' || chr(10) ||
            '                    default_val := 1;' || chr(10) ||
            '                ELSE' || chr(10) ||
            '                    default_val := 0;' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- find out if there already is a tuple' || chr(10) ||
            '                i := SELF.measure_metadata.FIRST;' || chr(10) ||
            '                found := FALSE;' || chr(10) ||
            '                WHILE i IS NOT NULL AND NOT found LOOP' || chr(10) ||
            '                    IF(SELF.measure_metadata(i).measure_level.equals(measure_lvl) > 0 AND' || chr(10) ||
            '                       SELF.measure_metadata(i).measure_name = measure_name AND' || chr(10) ||
            '                       ((SELF.measure_metadata(i).metalevel IS NULL AND' || chr(10) ||
            '                         metalevel IS NULL) OR' || chr(10) ||
            '                        SELF.measure_metadata(i).metalevel = metalevel) AND' || chr(10) ||  
            '                       SELF.measure_metadata(i).default_value = default_val) THEN' || chr(10) ||
            '                        found := TRUE;' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    IF NOT found THEN' || chr(10) ||
            '                        i := SELF.measure_metadata.NEXT(i);' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '                IF NOT found THEN' || chr(10) ||
            '                    SELF.measure_metadata.extend;' || chr(10) ||
            '                    SELF.measure_metadata(SELF.measure_metadata.LAST) :=' || chr(10) ||
            '                        ' || measure_meta_#_ty || '(measure_name, measure_lvl, metalevel, default_val, measure_value);' || chr(10) ||
            '                ELSE' || chr(10) ||
            '                    SELF.measure_metadata(i).measure_value := measure_value;' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                SELF.persist;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_set2_measure :=
            '    MEMBER PROCEDURE set_measure(measure_name  VARCHAR2,' || chr(10) ||
            '                                 measure_value ANYDATA) IS' || chr(10) ||
            '        sql_cursor INTEGER;' || chr(10) ||
            '        rows_processed INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        measure_descr ' || measure_#_ty || ';' || chr(10) ||
            '        table_name VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        data_type VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        mcube mcube_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        elem_tty_value elem_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        status PLS_INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '        found BOOLEAN := FALSE;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        IF measure_value IS NOT NULL THEN' || chr(10) ||
            '            found := has_measure(measure_name, ' || chr(10) ||
            '                                 TRUE, FALSE, ' || chr(10) ||
            '                                 measure_descr);' || chr(10) ||
            '            -- get the m-cube' || chr(10) ||
            '            utl_ref.select_object(SELF.mcube, mcube);' || chr(10) ||
            '            ' || chr(10) ||
            '            IF mcube.enforce_consistency > 0 THEN' || chr(10) ||
            '                -- TODO: add consistency checks' || chr(10) ||
            '                NULL;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF found THEN' || chr(10) ||
            '                table_name := measure_descr.table_name;' || chr(10) ||
            '                data_type := measure_descr.data_type;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- add a tuple to the measure table' || chr(10) ||
            '                init_measure_table(table_name);' || chr(10) ||
            '                ' || chr(10) ||
            '                sql_cursor := dbms_sql.open_cursor;' || chr(10) ||
            '                ' || chr(10) ||
            '                dbms_sql.parse(sql_cursor,' || chr(10) ||
            '                               ''UPDATE '' || table_name || '' mc'' || chr(10) ||' || chr(10) ||
            '                               ''SET mc.'' || measure_name || ''= :measure_value'' || chr(10) ||' || chr(10) ||
            '                               ''WHERE ' || mrel_set2_measure_coord_cmp || ''',' || chr(10) ||
            '                               dbms_sql.native);' || chr(10) ||
            '                ' || chr(10) ||
            mrel_set2_measure_coord_bind ||
            '                -- TODO: Throw error message when passed argument is of wrong type.' || chr(10) ||
            '                ' || chr(10) ||
            '                CASE data_type' || chr(10) ||
            '                    WHEN ''VARCHAR2'' THEN' || chr(10) ||
            '                        dbms_sql.bind_variable(sql_cursor, ' || chr(10) ||
            '                                               ''measure_value'', ' || chr(10) ||
            '                                               measure_value.accessVarchar2);' || chr(10) ||
            '                    WHEN ''NUMBER'' THEN' || chr(10) ||
            '                        dbms_sql.bind_variable(sql_cursor, ' || chr(10) ||
            '                                               ''measure_value'', ' || chr(10) ||
            '                                               measure_value.accessNumber);' || chr(10) ||
            '                    WHEN ''ELEM_TTY'' THEN' || chr(10) ||
            '                        status := measure_value.getCollection(elem_tty_value);' || chr(10) ||
            '                        dbms_sql.bind_variable(sql_cursor, ' || chr(10) ||
            '                                               ''measure_value'', ' || chr(10) ||
            '                                               elem_tty_value);' || chr(10) ||
            '                    ELSE' || chr(10) ||
            '                        dbms_sql.bind_variable(sql_cursor, ' || chr(10) ||
            '                                               ''measure_value'', ' || chr(10) ||
            '                                               measure_value);' || chr(10) ||
            '                END CASE;' || chr(10) ||
            '                ' || chr(10) ||
            '                rows_processed := dbms_sql.execute(sql_cursor);' || chr(10) ||
            '                ' || chr(10) ||
            '                dbms_sql.close_cursor(sql_cursor);' || chr(10) ||
            '                ' || chr(10) ||
            '                SELF.persist;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_does_specialize :=
            '    OVERRIDING MEMBER FUNCTION does_specialize RETURN BOOLEAN IS' || chr(10) ||
            '        specializes BOOLEAN := FALSE;' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        specializes := (SELF.measure_tables IS NOT NULL AND' || chr(10) ||
            '                        SELF.measure_tables.COUNT > 0);' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN specializes;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_delete_measure :=
            '    MEMBER PROCEDURE delete_measure(measure_name VARCHAR2) IS' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        NULL;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_get_measure :=
            '    MEMBER FUNCTION get_measure(measure_name VARCHAR2) RETURN ANYDATA IS' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        RETURN NULL;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_get_measure_unit :=
            '    MEMBER FUNCTION get_measure_unit(measure_name VARCHAR2) RETURN ANYDATA IS' || chr(10) ||
            '        return_value ANYDATA;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '        j INTEGER;' || chr(10) ||
            '        found BOOLEAN;' || chr(10) ||
            '        ' || chr(10) ||
            '        ancestors_ordered ' || mrel_#_trty || ';' || chr(10) ||
            '        ancestor_mrel ' || mrel_#_ty || ';' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        j := SELF.measure_metadata.FIRST;' || chr(10) ||
            '        WHILE j IS NOT NULL AND NOT found LOOP' || chr(10) ||
            '            IF UPPER(SELF.measure_metadata(j).measure_name) = UPPER(measure_name) AND' || chr(10) ||
            '               UPPER(SELF.measure_metadata(j).metalevel) = ''UNIT'' THEN' || chr(10) ||
            '                return_value := SELF.measure_metadata(j).measure_value;' || chr(10) ||
            '                found := TRUE;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            j := SELF.measure_metadata.NEXT(j);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- ancestors have to be iterated bottom-up' || chr(10) ||
            '        SELECT anc.ancestor BULK COLLECT INTO ancestors_ordered' || chr(10) ||
            '        FROM   TABLE(SELF.ancestors) anc' || chr(10) ||
            '        ORDER BY anc.ancestor.coordinate;' || chr(10) ||
            '        ' || chr(10) ||
            '        i := ancestors_ordered.FIRST;' || chr(10) ||
            '        found := FALSE;' || chr(10) ||
            '        WHILE i IS NOT NULL AND NOT found LOOP' || chr(10) ||
            '            utl_ref.select_object(ancestors_ordered(i), ancestor_mrel);' || chr(10) ||
            '            j := ancestor_mrel.measure_metadata.FIRST;' || chr(10) ||
            '            WHILE j IS NOT NULL AND NOT found LOOP' || chr(10) ||
            '                IF ancestor_mrel.measure_metadata(j).measure_name = measure_name AND' || chr(10) ||
            '                   UPPER(ancestor_mrel.measure_metadata(j).metalevel) = ''UNIT'' THEN' || chr(10) ||
            '                    return_value := ancestor_mrel.measure_metadata(j).measure_value;' || chr(10) ||
            '                    found := TRUE;' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                j := ancestor_mrel.measure_metadata.NEXT(j);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := ancestors_ordered.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN return_value;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_has_measure :=
            '    MEMBER FUNCTION has_measure(measure_name    IN  VARCHAR2,' || chr(10) ||
            '                                top_level_only  IN  BOOLEAN,' || chr(10) ||
            '                                introduced_only IN  BOOLEAN,' || chr(10) ||
            '                                description     OUT ' || measure_#_ty || ') RETURN BOOLEAN IS' || chr(10) ||          
            '        i INTEGER;' || chr(10) ||
            '        j INTEGER;' || chr(10) ||
            '        found BOOLEAN := FALSE;' || chr(10) ||
            '        ' || chr(10) ||
            '        row_count INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        table_name1 VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        ancestors_ordered ' || mrel_#_trty || ';' || chr(10) ||
            '        ancestor_mrel ' || mrel_#_ty || ';' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- search this m-relationship''s measure_tables for measures' || chr(10) ||
            '        i := SELF.measure_tables.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL AND NOT found LOOP' || chr(10) ||
            '            -- ' || chr(10) ||
            '            table_name1 := SELF.measure_tables(i).table_name;' || chr(10) ||
            '            ' || chr(10) ||
            '            SELECT COUNT(*) INTO row_count ' || chr(10) ||
            '            FROM   user_tab_columns utc' || chr(10) ||
            '            WHERE  utc.table_name = UPPER(table_name1) AND ' || chr(10) ||
            '                   utc.column_name = UPPER(measure_name);' || chr(10) ||
            '            ' || chr(10) ||
            '            IF (row_count > 0) THEN' || chr(10) ||
            '                IF NOT top_level_only OR ' || chr(10) ||
            '                   SELF.measure_tables(i).conlevel.equals(SELF.top_level()) > 0 THEN' || chr(10) ||
            '                    found := TRUE;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    -- get the attribute description' || chr(10) ||
            '                    description := ' || chr(10) ||
            '                        ' || measure_#_collections || '.get_measure_by_table(measure_name, table_name1, SELF.measure_tables(i).conlevel);' || chr(10) ||
            '                ELSE' || chr(10) ||
            '                    -- if the measure has been found, but the connection level is inconsistent,' || chr(10) ||
            '                    -- then return false and do not check any further.' || chr(10) ||
            '                    RETURN FALSE;' || chr(10) ||
            '                END IF;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := SELF.measure_tables.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- search for measures introduced by ancestor m-relationships.' || chr(10) ||
            '        IF NOT introduced_only AND SELF.ancestors IS NOT NULL THEN' || chr(10) ||
            '            -- iterates from last to first so that mixed granularities are correctly displayed' || chr(10) ||
            '            -- for this to work the ancestors must be sorted in ascending order;' || chr(10) ||
            '            -- the most direct ancestor must be first in the list.' || chr(10) ||
            '            /*SELECT anc.ancestor BULK COLLECT INTO ancestors_ordered' || chr(10) ||
            '            FROM   TABLE(SELF.ancestors) anc,' || chr(10) ||
                                mrel_has_measure_order_anc1 ||
            '            WHERE  ' || mrel_has_measure_order_anc2 || chr(10) ||
            '            ORDER BY (' || mrel_has_measure_order_anc3 || ') DESC;*/' || chr(10) ||
            '            ' || chr(10) ||
            '            SELECT anc.ancestor BULK COLLECT INTO ancestors_ordered' || chr(10) ||
            '            FROM   TABLE(SELF.ancestors) anc' || chr(10) ||
            '            ORDER BY anc.ancestor.coordinate;' || chr(10) ||
            '            ' || chr(10) ||
            '            j := ancestors_ordered.FIRST;' || chr(10) ||
            '            WHILE j IS NOT NULL AND NOT found LOOP' || chr(10) ||
            '                -- get the ancestor m-relationship' || chr(10) ||
            '                utl_ref.select_object(ancestors_ordered(j), ancestor_mrel);' || chr(10) ||
            '                ' || chr(10) ||
            '                i := ancestor_mrel.measure_tables.FIRST;' || chr(10) ||
            '                WHILE i IS NOT NULL AND NOT found LOOP ' || chr(10) ||
            '                    table_name1 := ancestor_mrel.measure_tables(i).table_name;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    SELECT COUNT(*) INTO row_count' || chr(10) ||
            '                    FROM   user_tab_columns utc' || chr(10) ||
            '                    WHERE  utc.table_name = UPPER(table_name1) AND ' || chr(10) ||
            '                           utc.column_name = UPPER(measure_name);' || chr(10) ||
            '                    ' || chr(10) ||
            '                    IF (row_count > 0) THEN' || chr(10) ||
            '                        IF ancestor_mrel.measure_tables(i).conlevel.equals(SELF.top_level()) > 0 OR ' || chr(10) ||
            '                           (NOT top_level_only AND SELF.has_connection_level(ancestor_mrel.measure_tables(i).conlevel)) THEN' || chr(10) ||
            '                            found := TRUE;' || chr(10) ||
            '                            ' || chr(10) ||
            '                            -- get the attribute description' || chr(10) ||
            '                            description :=' || chr(10) ||
            '                                ' || measure_#_collections || '.get_measure_by_table(measure_name, table_name1, ancestor_mrel.measure_tables(i).conlevel);' || chr(10) ||
            '                        ELSE' || chr(10) ||
            '                            -- if the measure has been found, but the connection level is inconsistent,' || chr(10) ||
            '                            -- then return false and do not check any further.' || chr(10) ||
            '                            RETURN FALSE;' || chr(10) ||
            '                        END IF;' || chr(10) ||
            '                    END IF;' || chr(10) ||
            '                    ' || chr(10) ||
            '                    i :=  ancestor_mrel.measure_tables.NEXT(i);' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '                j := ancestors_ordered.NEXT(j);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        IF (NOT found) THEN' || chr(10) ||
            '            description := NULL;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN found;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_has2_measure :=
            '    MEMBER FUNCTION has_measure(measure_name    VARCHAR2,' || chr(10) ||
            '                                top_level_only  INTEGER,' || chr(10) ||
            '                                introduced_only INTEGER) RETURN INTEGER IS' || chr(10) ||
            '        found BOOLEAN;' || chr(10) ||
            '        measure_descr ' || measure_#_ty || ';' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        found := SELF.has_measure(measure_name, top_level_only > 0, introduced_only > 0, measure_descr);' || chr(10) ||
            '        ' || chr(10) ||
            '        IF found THEN' || chr(10) ||
            '            RETURN 1;' || chr(10) ||
            '        ELSE' || chr(10) ||
            '            RETURN 0;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_list_measures :=
            '    MEMBER FUNCTION list_measures(top_level_only  BOOLEAN,' || chr(10) ||
            '                                  introduced_only BOOLEAN) RETURN ' || measure_#_tty || ' IS' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        RETURN NULL;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_list2_measures :=
            '    MEMBER FUNCTION list_measures(top_level_only  INTEGER,' || chr(10) ||
            '                                  introduced_only INTEGER) RETURN ' || measure_#_tty || ' IS' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        RETURN NULL;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
            
        mrel_get_measure_tab :=
            '    MEMBER FUNCTION get_measure_table(conlevel ' || conlevel_#_ty || ') RETURN VARCHAR2 IS' || chr(10) ||
            '        table_name VARCHAR2(30);' || chr(10) ||
            '        searched_level ' || conlevel_#_ty || ' := conlevel;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        BEGIN' || chr(10) ||
            '            -- ' || chr(10) ||
            '            SELECT l.table_name INTO table_name' || chr(10) ||
            '            FROM   TABLE(SELECT r.measure_tables' || chr(10) ||
            '                         FROM   ' || mrel_table || ' r' || chr(10) ||
            '                         WHERE  ' || mrel_get_measure_tab_coordcmp || ') l' || chr(10) ||
            '            WHERE  ' || mrel_get_measure_tab_lvlcmp || ';' || chr(10) ||
            '        EXCEPTION' || chr(10) ||
            '            WHEN NO_DATA_FOUND THEN' || chr(10) ||
            '                table_name := NULL;' || chr(10) ||
            '        END;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN table_name;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_has_conlevel :=
            '    MEMBER FUNCTION has_connection_level(conlevel ' || conlevel_#_ty || ') RETURN BOOLEAN IS' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        RETURN SELF.has_connection_level(' || mrel_has_conlevel1 || ') > 0;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
                
        mrel_has2_conlevel :=
            '    MEMBER FUNCTION has_connection_level(' || mrel_has2_conlevel1 || ') RETURN INTEGER IS' || chr(10) ||
            '        cnt INTEGER := 1;' || chr(10) ||
            '    BEGIN' || chr(10) ||
                     mrel_has2_conlevel_select ||
            '        ' || chr(10) ||
            '        RETURN cnt;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_persist :=
            '    MEMBER PROCEDURE persist IS' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        UPDATE ' || mrel_table || ' t' || chr(10) ||
            '        SET    t = SELF' || chr(10) ||
            '        WHERE  t.id = SELF.id;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_init_measure_table :=
            '    MEMBER PROCEDURE init_measure_table(table_name VARCHAR2) IS' || chr(10) ||
            '        row_count INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- check if there already is an entry for SELF in the specified table' || chr(10) ||
            '        EXECUTE IMMEDIATE ' || chr(10) ||
            '            ''SELECT COUNT(*)'' || chr(10) ||' || chr(10) ||
            '            ''FROM   '' || table_name || '' m'' || chr(10) ||' || chr(10) ||
            '            ''WHERE  ' || mrel_init_coord_cmp || '''' || chr(10) ||
            '            INTO  row_count' || chr(10) ||
            '            USING ' || mrel_init_coord_bind || ';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- if there is no entry yet, insert a new entry' || chr(10) ||
            '        IF row_count = 0 THEN' || chr(10) ||            
            '            EXECUTE IMMEDIATE' || chr(10) ||
            '                ''INSERT INTO '' || table_name || ''(coordinate, mrel) SELECT m.coordinate, REF(m) FROM ' || mrel_table || ' m WHERE ' || mrel_init_coord_cmp || '''' || chr(10) ||
            '                USING ' || mrel_init_coord_bind || ';' || chr(10) ||
            '        END IF;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10);
        
        mrel_body :=
            'CREATE OR REPLACE TYPE BODY ' || mrel_#_ty || ' IS' || chr(10) ||
                 mrel_constructor ||
                 mrel_does_specialize ||
                 mrel_delete ||
                 mrel_calc_ancestors ||
                 mrel_top_level ||
                 mrel_add_measure ||
                 mrel_set_measure ||
                 mrel_set2_measure ||
                 mrel_delete_measure ||
                 mrel_get_measure ||
                 mrel_get_measure_unit ||
                 mrel_has_measure ||
                 mrel_has2_measure ||
                 mrel_list_measures ||
                 mrel_list2_measures ||
                 mrel_get_measure_tab ||
                 mrel_has_conlevel ||
                 mrel_has2_conlevel ||
                 mrel_persist ||
                 mrel_init_measure_table ||
            'END;';
        
        --dbms_output.put_line(mrel_body);
        EXECUTE IMMEDIATE mrel_body;
    END;
    
    /*** ***/
END;
/
