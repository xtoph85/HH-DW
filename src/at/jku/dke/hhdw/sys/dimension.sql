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

DROP TYPE dimension_trty FORCE;

DROP PACKAGE dimension;

--------------------------------------------------------------------------------
--                               DECLARATION                                  --
--------------------------------------------------------------------------------

CREATE OR REPLACE TYPE dimension_trty AS TABLE OF REF dimension_ty;
/

CREATE OR REPLACE PACKAGE dimension AS    
    -- user interface
    FUNCTION create_dimension(dname VARCHAR2) RETURN REF dimension_ty;
    
    PROCEDURE delete_dimension(dname VARCHAR2);
    
    PROCEDURE create_sequence(pkg_name VARCHAR2,
                              seq_name VARCHAR2);
    
    -- private helpers
    /*
    PROCEDURE create_dimensions_table;
    
    PROCEDURE create_type_headers(dimension_#_ty VARCHAR2,
                                  mobject_#_ty   VARCHAR2);
    
    PROCEDURE create_dimension_#_ty(dimension_#_ty VARCHAR2,
                                    dim_id         VARCHAR2,
                                    mobject_#_ty   VARCHAR2,
                                    mobject_table  VARCHAR2,
                                    mobject_id_seq VARCHAR2);
    
    PROCEDURE create_mobject_#_ty(mobject_#_ty  VARCHAR2,
                                  mobject_table VARCHAR2,
                                  dname         VARCHAR2);
                                  
    PROCEDURE create_mobject_table(mobject_#_ty       VARCHAR2,
                                   mobject_table      VARCHAR2,
                                   dim_id             VARCHAR2,
                                   primary_key_idx    VARCHAR2,
                                   unique_id_idx      VARCHAR2,
                                   ancestors_lvl_idx  VARCHAR2,
                                   ancestors_pk_idx   VARCHAR2,
                                   attributes_lvl_idx VARCHAR2,
                                   attributes_tab_idx VARCHAR2,
                                   hierarchy_idx      VARCHAR2,
                                   metadata_idx       VARCHAR2);
                                   
    PROCEDURE create_hierarchy_triggers(mobject_table  VARCHAR2,
                                        dname          VARCHAR2,
                                        insert_trigger VARCHAR2,
                                        delete_trigger VARCHAR2);*/
END;
/


--------------------------------------------------------------------------------
--                               DIMENSIONS                                   --
--------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE BODY DIMENSION AS 
    /**
     * Create a new alphanumerical sequence.
     * @param pkg_name the name of the package which is the alphanumerical sequence.
     * @param seq_name the name of the underlying numerical sequence.
     */
    PROCEDURE create_sequence(pkg_name VARCHAR2,
                              seq_name VARCHAR2) IS
                              
    BEGIN
        EXECUTE IMMEDIATE
          'CREATE SEQUENCE ' || seq_name || '';
        
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE PACKAGE ' || pkg_name || ' AS' || chr(10) ||
          '    seq_name VARCHAR2(30) := ''' || seq_name || ''';' || chr(10) ||
          '    ' || chr(10) ||
          '    FUNCTION currval RETURN VARCHAR2;' || chr(10) ||
          '    FUNCTION nextval RETURN VARCHAR2;' || chr(10) ||
          '    FUNCTION curr_val RETURN VARCHAR2;' || chr(10) ||
          '    FUNCTION next_val RETURN VARCHAR2;' || chr(10) ||
          '    PROCEDURE delete_sequence;' || chr(10) ||
          'END;' || chr(10);
          
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE PACKAGE BODY ' || pkg_name || ' IS' || chr(10) ||
          '    PROCEDURE delete_sequence IS' || chr(10) ||
          '        ' || chr(10) ||
          '    BEGIN' || chr(10) ||
          '        EXECUTE IMMEDIATE' || chr(10) ||
          '            ''DROP SEQUENCE '' || seq_name || '''';' || chr(10) ||
          '    END;' || chr(10) ||
          '    ' || chr(10) ||
          '    FUNCTION curr_val RETURN VARCHAR2 IS' || chr(10) ||
          '    ' || chr(10) ||
          '    BEGIN' || chr(10) ||
          '        RETURN currval();' || chr(10) ||
          '    END;' || chr(10) ||
          '    ' || chr(10) ||
          '    FUNCTION next_val RETURN VARCHAR2 IS' || chr(10) ||
          '    ' || chr(10) ||
          '    BEGIN' || chr(10) ||
          '        RETURN nextval();' || chr(10) ||
          '    END;' || chr(10) ||
          '    ' || chr(10) ||
          '    FUNCTION currval RETURN VARCHAR2 IS' || chr(10) ||
          '        val VARCHAR2(10);' || chr(10) ||
          '    BEGIN' || chr(10) ||
          '        SELECT SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.CURRVAL/2821109907456),36)+1, 1) ||  -- 36^8' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.CURRVAL/78364164096),36)+1, 1) ||  -- 36^7' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.CURRVAL/2176782336),36)+1, 1) ||  -- 36^6' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.CURRVAL/60466176),36)+1, 1) ||  -- 36^5' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.CURRVAL/1679616),36)+1, 1) ||  -- 36^4' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.CURRVAL/46656),36)+1, 1) ||  -- 36^3' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.CURRVAL/1296),36)+1, 1) ||  -- 36^2' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.CURRVAL/36),36)+1, 1) ||  -- 36^1' || chr(10) ||
          '               SUBSTR(base36.val, MOD(' || seq_name || '.CURRVAL,36)+1, 1)' || chr(10) ||
          '        INTO   val' || chr(10) ||
          '        FROM   (SELECT ''0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'' AS val FROM dual) base36;' || chr(10) ||
          '        ' || chr(10) ||
          '        RETURN val;' || chr(10) ||
          '    END;' || chr(10) ||
          '    ' || chr(10) ||
          '    FUNCTION nextval RETURN VARCHAR2 IS' || chr(10) ||
          '        val VARCHAR2(10);' || chr(10) ||
          '    BEGIN' || chr(10) ||
          '        SELECT SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.NEXTVAL/2821109907456),36)+1, 1) ||  -- 36^8' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.NEXTVAL/78364164096),36)+1, 1) ||  -- 36^7' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.NEXTVAL/2176782336),36)+1, 1) ||  -- 36^6' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.NEXTVAL/60466176),36)+1, 1) ||  -- 36^5' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.NEXTVAL/1679616),36)+1, 1) ||  -- 36^4' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.NEXTVAL/46656),36)+1, 1) ||  -- 36^3' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.NEXTVAL/1296),36)+1, 1) ||  -- 36^2' || chr(10) ||
          '               SUBSTR(base36.val, MOD(TRUNC(' || seq_name || '.NEXTVAL/36),36)+1, 1) ||  -- 36^1' || chr(10) ||
          '               SUBSTR(base36.val, MOD(' || seq_name || '.NEXTVAL,36)+1, 1)' || chr(10) ||
          '        INTO   val' || chr(10) ||
          '        FROM   (SELECT ''0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'' AS val FROM dual) base36;' || chr(10) ||
          '        ' || chr(10) ||
          '        RETURN val;' || chr(10) ||
          '    END;' || chr(10) ||
          'END;';
    END;
    
    /**
     * This procedure creates the table DIMENSIONS if it does not already exist.
     */
    PROCEDURE create_dimensions_table IS
        cnt INTEGER;
    BEGIN
        -- check if the table already exists
        SELECT COUNT(*) INTO cnt
        FROM   user_tab_columns utc
        WHERE  utc.table_name = 'DIMENSIONS';
        
        -- only create the table if it does not exist already
        IF cnt = 0 THEN
            create_sequence('dimensions_seq_pkg', 'dimensions_seq_seq');
            
            -- create the dimensions table
            EXECUTE IMMEDIATE
                'CREATE TABLE dimensions OF dimension_ty (' || chr(10) ||
                '    dname PRIMARY KEY,' || chr(10) ||
                '    dimension_#_ty NOT NULL,' || chr(10) ||
                '    mobject_#_ty NOT NULL,' || chr(10) ||
                '    mobject_table NOT NULL,' || chr(10) ||
                '    enforce_consistency NOT NULL)' || chr(10) ||
                '    NESTED TABLE level_hierarchy STORE AS dimensions_level_hierarchy' || chr(10) ||
                '    NESTED TABLE level_positions STORE AS dimensions_level_positions' || chr(10);
            
            -- create indexes for the level-hierarchy and the level positions
            -- index for the level-hierarchies
            EXECUTE IMMEDIATE
                'CREATE UNIQUE INDEX dimensions_hierarchy_idx ON ' || 
                    'dimensions_level_hierarchy(NESTED_TABLE_ID, lvl, parent_level)';
            
            EXECUTE IMMEDIATE
                'CREATE UNIQUE INDEX dimensions_positions_idx ON ' || 
                    'dimensions_level_positions(NESTED_TABLE_ID, lvl)';
        END IF;
    END;
                              
    PROCEDURE create_type_headers(dimension_#_ty VARCHAR2,
                                  mobject_#_ty VARCHAR2) IS
                                  
        create_dimension_declaration VARCHAR2(3000);
        create_mobject_declaration VARCHAR2(2000);
    BEGIN
       create_dimension_declaration :=
            'CREATE OR REPLACE TYPE ' || dimension_#_ty || ' UNDER dimension_ty (' || chr(10) ||
            '    CONSTRUCTOR FUNCTION ' || dimension_#_ty || '(dname VARCHAR2, id VARCHAR2)' || chr(10) ||
            '        RETURN SELF AS RESULT,'  || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE insert_mobject(oname VARCHAR2,' || chr(10) ||
            '                                               top_level VARCHAR2,' || chr(10) || 
            '                                               parents mobject_trty,' || chr(10) || 
            '                                               level_hierarchy level_hierarchy_tty),' || chr(10) ||  
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE insert_mobject(oname VARCHAR2,' || chr(10) ||
            '                                               id VARCHAR2,' || chr(10) || 
            '                                               top_level VARCHAR2,' || chr(10) || 
            '                                               parents mobject_trty,' || chr(10) || 
            '                                               level_hierarchy level_hierarchy_tty),' || chr(10) || 
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE bulk_create_mobject(onames names_tty,' || chr(10) || 
            '                                                    top_level VARCHAR2,' || chr(10) || 
            '                                                    parents mobject_trty,' || chr(10) || 
            '                                                    level_hierarchy level_hierarchy_tty),' || chr(10) || 
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE bulk_set_attribute(attribute_name   VARCHAR2,' || chr(10) ||
            '                                                   attribute_values mobject_value_tty),' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE export_star(table_name VARCHAR2),' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE refresh_level_hierarchy,' || chr(10) ||
            '    OVERRIDING MEMBER FUNCTION calculate_level_hierarchy RETURN level_hierarchy_tty,' || chr(10) ||
            '    PRAGMA RESTRICT_REFERENCES(calculate_level_hierarchy, WNDS),'  || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE reload_cache,' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE drop_indexes,' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE rebuild_indexes,' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE drop_constraints,' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE rebuild_constraints,' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE persist,' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE delete_dimension,' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER FUNCTION get_attribute_description(attribute_name VARCHAR2) RETURN attribute_ty,' || chr(10) ||
            '    ' || chr(10) ||
            '    -- consistency checks' || chr(10) ||
            '    OVERRIDING MEMBER FUNCTION unique_attribute_induction(attribute_name VARCHAR2)' || chr(10) ||
            '                 RETURN BOOLEAN,' || chr(10) ||
            '    OVERRIDING MEMBER FUNCTION unique_level_induction(lvl VARCHAR2)' || chr(10) ||
            '                 RETURN BOOLEAN' || chr(10) ||
            ');';
       
       create_mobject_declaration := 
            'CREATE OR REPLACE TYPE ' || mobject_#_ty || ' UNDER mobject_ty (' || chr(10) ||
            '    CONSTRUCTOR FUNCTION ' || mobject_#_ty || '(oname VARCHAR2,' || chr(10) || 
            '                             id VARCHAR2,' || chr(10) || 
            '                             top_level VARCHAR2,' || chr(10) ||
            '                             parents mobject_trty,' || chr(10) ||
            '                             level_hierarchy level_hierarchy_tty)' || chr(10) ||
            '        RETURN SELF AS RESULT,'  || chr(10) ||
            '    OVERRIDING MEMBER FUNCTION top_level_position RETURN INTEGER,'  || chr(10) ||
            '    PRAGMA RESTRICT_REFERENCES(top_level_position, WNDS),'  || chr(10) ||
            '    '  || chr(10) ||
            '    OVERRIDING MEMBER FUNCTION get_descendants RETURN mobject_trty,' || chr(10) ||
            '    OVERRIDING MEMBER FUNCTION get_descendants_onames RETURN names_tty,' || chr(10) ||
            '    OVERRIDING MEMBER FUNCTION get_attribute_table(lvl VARCHAR2) RETURN VARCHAR2,' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE persist,' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE delete_attribute_metadata(attribute_name VARCHAR2),' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE delete_mobject,' || chr(10) ||
            '    STATIC FUNCTION calculate_inherited_levels(parent_onames names_tty, ' || chr(10) ||
            '                                               top_level VARCHAR2,' || chr(10) ||
            '                                               delta_level_hierarchy level_hierarchy_tty) RETURN level_hierarchy_tty' || chr(10) ||
            ');';
        
        EXECUTE IMMEDIATE create_dimension_declaration;
        EXECUTE IMMEDIATE create_mobject_declaration;
    END;
    
    /**
     *
     *
     */
    PROCEDURE create_mobject_table(mobject_#_ty       VARCHAR2,
                                   mobject_table      VARCHAR2,
                                   dim_id             VARCHAR2,
                                   primary_key_idx    VARCHAR2,
                                   unique_id_idx      VARCHAR2,
                                   ancestors_lvl_idx  VARCHAR2,
                                   ancestors_pk_idx   VARCHAR2,
                                   attributes_lvl_idx VARCHAR2,
                                   attributes_tab_idx VARCHAR2,
                                   hierarchy_idx      VARCHAR2,
                                   metadata_idx       VARCHAR2) IS
        
    BEGIN
        -- create the m-object table
        EXECUTE IMMEDIATE
            'CREATE TABLE ' || mobject_table || ' OF ' || mobject_#_ty || chr(10) ||
            '    (id NOT NULL,' || chr(10) ||
            '     CONSTRAINT ' || primary_key_idx || ' PRIMARY KEY (oname),' || chr(10) ||
            '     CONSTRAINT ' || unique_id_idx || ' UNIQUE (id),' || chr(10) ||
            '     -- set scope for the dimension. intentionally, this is not' || chr(10) ||
            '     -- a references constraint as it is not needed.' || chr(10) ||
            '     dim SCOPE IS dimensions)' || chr(10) ||
            '    NESTED TABLE parents STORE AS ' || dim_id || '_parents' || chr(10) ||
            '    NESTED TABLE ancestors STORE AS ' || dim_id || '_ancestors' || chr(10) ||
            '    NESTED TABLE level_hierarchy STORE AS ' || dim_id || '_levelhierarchy' || chr(10) ||
            '    NESTED TABLE attribute_tables STORE AS ' || dim_id || '_attributes' || chr(10) ||
            '    NESTED TABLE attribute_metadata STORE AS ' || dim_id || '_metadata';
                
        ---- create indexes for the nested tables ----
      
        -- the ancestors nested table's key is the level of the ancestor
        EXECUTE IMMEDIATE
            'CREATE UNIQUE INDEX ' || ancestors_pk_idx || ' ON ' || 
                dim_id || '_ancestors(NESTED_TABLE_ID, lvl)';
        
        -- make a non-unique index on the lvl so that we can query ancestors faster.
        EXECUTE IMMEDIATE
            'CREATE INDEX ' || ancestors_lvl_idx || ' ON ' || 
                dim_id || '_ancestors(lvl)';
        
        -- primary key of the nested table is the level of the attribute table
        EXECUTE IMMEDIATE
            'CREATE UNIQUE INDEX ' || attributes_lvl_idx || ' ON ' || 
                dim_id || '_attributes(NESTED_TABLE_ID, lvl)';
        
        -- a table name in the nested table is also unique (case-insensitive!)
        EXECUTE IMMEDIATE
            'CREATE UNIQUE INDEX ' || attributes_tab_idx || ' ON ' || 
                dim_id || '_attributes(NESTED_TABLE_ID, UPPER(table_name))';
        
        -- an index for metadata is placed of the attribute name column
        EXECUTE IMMEDIATE
            'CREATE INDEX ' || metadata_idx || ' ON ' || 
                dim_id || '_metadata(attribute_name)';
        
        -- index for the level-hierarchies
        EXECUTE IMMEDIATE
            'CREATE UNIQUE INDEX ' || hierarchy_idx || ' ON ' || 
                dim_id || '_levelhierarchy(NESTED_TABLE_ID, lvl, parent_level)';
    END;
    
    PROCEDURE create_dimension_#_ty(dimension_#_ty     VARCHAR2,
                                    dim_id             VARCHAR2,
                                    mobject_#_ty       VARCHAR2,
                                    mobject_table      VARCHAR2,
                                    mobject_id_seq     VARCHAR2,
                                    primary_key_idx    VARCHAR2,
                                    unique_id_idx      VARCHAR2,
                                    ancestors_lvl_idx  VARCHAR2,
                                    ancestors_pk_idx   VARCHAR2,
                                    attributes_lvl_idx VARCHAR2,
                                    attributes_tab_idx VARCHAR2,
                                    hierarchy_idx      VARCHAR2,
                                    metadata_idx       VARCHAR2) IS
        
        create_dimension_body CLOB;
    BEGIN
        create_dimension_body :=
            'CREATE OR REPLACE TYPE BODY ' || dimension_#_ty || ' AS' || chr(10) ||
            '    CONSTRUCTOR FUNCTION ' || dimension_#_ty || '(dname VARCHAR2, id VARCHAR2)' || chr(10) ||
            '        RETURN SELF AS RESULT IS' || chr(10) ||
            '        ' || chr(10) || 
            '    BEGIN' || chr(10) ||
            '        SELF.dname := dname;' || chr(10) ||
            '        SELF.id := id;' || chr(10) || 
            '        ' || chr(10) || 
            '        SELF.dimension_#_ty := ''' || dimension_#_ty || ''';' || chr(10) || 
            '        SELF.mobject_#_ty := ''' || mobject_#_ty || ''';' || chr(10) || 
            '        SELF.mobject_table := ''' || mobject_table || ''';' || chr(10) || 
            '        SELF.mobject_id_seq := ''' || mobject_id_seq || ''';' || chr(10) || 
            '        ' || chr(10) || 
            '        SELF.primary_key_idx := ''' || primary_key_idx || ''';' || chr(10) || 
            '        SELF.unique_id_idx := ''' || unique_id_idx || ''';' || chr(10) || 
            '        SELF.ancestors_lvl_idx := ''' || ancestors_lvl_idx || ''';' || chr(10) || 
            '        SELF.ancestors_pk_idx := ''' || ancestors_pk_idx || ''';' || chr(10) || 
            '        SELF.attributes_lvl_idx := ''' || attributes_lvl_idx || ''';' || chr(10) ||
            '        SELF.attributes_tab_idx := ''' || attributes_tab_idx || ''';' || chr(10) ||
            '        SELF.hierarchy_idx := ''' || hierarchy_idx || ''';' || chr(10) ||
            '        SELF.metadata_idx := ''' || metadata_idx || ''';' || chr(10) || 
            '        ' || chr(10) || 
            '        SELF.level_hierarchy := NULL;' || chr(10) ||
            '        SELF.level_positions := NULL;' || chr(10) ||
            '        ' || chr(10) || 
            '        -- by default, consistency is checked' || chr(10) || 
            '        SELF.enforce_consistency := 1;' || chr(10) || 
            '        ' || chr(10) || 
            '        -- by default, level-hierarchy and level position cache is enabled' || chr(10) || 
            '        SELF.enable_hierarchy_cache := 1;' || chr(10) || 
            '        SELF.enable_position_cache := 1;' || chr(10) || 
            '        ' || chr(10) || 
            '        RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) || 
            '    OVERRIDING MEMBER PROCEDURE persist IS' || chr(10) || 
            '        ' || chr(10) || 
            '    BEGIN' || chr(10) ||
            '        UPDATE dimensions d ' || chr(10) ||
            '        SET    d = SELF'  || chr(10) || 
            '        WHERE  d.dname = SELF.dname;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) || 
            '    OVERRIDING MEMBER PROCEDURE insert_mobject(oname VARCHAR2,' || chr(10) ||
            '                                               top_level VARCHAR2,' || chr(10) || 
            '                                               parents mobject_trty,' || chr(10) || 
            '                                               level_hierarchy level_hierarchy_tty) IS' || chr(10) || 
            '        id VARCHAR2(10);' || chr(10) || 
            '        ' || chr(10) || 
            '        new_obj ' || mobject_#_ty || ';' || chr(10) || 
            '    BEGIN' || chr(10) || 
            '        id := ''o'' || ' || mobject_id_seq || '.NEXTVAL;' || chr(10) || 
            '        ' || chr(10) ||  
            '        SELF.insert_mobject(oname, id, top_level, parents, level_hierarchy);' || chr(10) || 
            '    END;' || chr(10) || 
            '    ' || chr(10) || 
            '    OVERRIDING MEMBER PROCEDURE insert_mobject(oname VARCHAR2,' || chr(10) ||
            '                                               id VARCHAR2,' || chr(10) || 
            '                                               top_level VARCHAR2,' || chr(10) || 
            '                                               parents mobject_trty,' || chr(10) || 
            '                                               level_hierarchy level_hierarchy_tty) IS' || chr(10) || 
            '        new_obj ' || mobject_#_ty || ';' || chr(10) || 
            '    BEGIN' || chr(10) || 
            '        new_obj := ' || mobject_#_ty || '(oname, id, top_level, parents, level_hierarchy);' || chr(10) ||
            '        ' || chr(10) || 
            '        INSERT INTO ' || mobject_table || ' VALUES(new_obj);' || chr(10) || 
            '        ' || chr(10) || 
            '        IF SELF.enable_hierarchy_cache > 0 THEN' || chr(10) || 
            '            -- trigger changed the level-hierarchy only in the table, ' || chr(10) || 
            '            -- keep SELF in-sync with the database.' || chr(10) || 
            '            SELF.reload_cache;' || chr(10) || 
            '        END IF;' || chr(10) || 
            '    END;' || chr(10) || 
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE drop_indexes IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- mark indexes unusable' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''ALTER TABLE '' || mobject_table || '' DROP CONSTRAINT '' || SELF.primary_key_idx;' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''ALTER TABLE '' || mobject_table || '' DROP CONSTRAINT '' || SELF.unique_id_idx;' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''DROP INDEX '' || SELF.ancestors_lvl_idx;' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''DROP INDEX '' || SELF.ancestors_pk_idx;' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''DROP INDEX '' || SELF.attributes_tab_idx;' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''DROP INDEX '' || SELF.attributes_lvl_idx;' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''DROP INDEX '' || SELF.metadata_idx;' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''DROP INDEX '' || SELF.hierarchy_idx;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE rebuild_indexes IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- the ancestors nested table''s KEY IS THE LEVEL OF THE ancestor' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''ALTER TABLE '' || mobject_table || '' ADD CONSTRAINT '' || SELF.primary_key_idx || '' PRIMARY KEY (oname)'';' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''ALTER TABLE '' || mobject_table || '' ADD CONSTRAINT '' || SELF.unique_id_idx || '' UNIQUE (id)'';' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''CREATE UNIQUE INDEX '' || SELF.ancestors_pk_idx || '' ON '' || ' || chr(10) ||
            '               ''' || dim_id || '_ancestors(NESTED_TABLE_ID, lvl)'';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- make a non-unique index on the lvl so that we can query ancestors faster.' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''CREATE INDEX '' || SELF.ancestors_lvl_idx || '' ON '' || ' || chr(10) ||
            '                ''' || dim_id || '_ancestors(lvl)'';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- primary key of the nested table is the level of the attribute table' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''CREATE UNIQUE INDEX '' || SELF.attributes_lvl_idx || '' ON '' || ' || chr(10) ||
            '               ''' || dim_id || '_attributes(NESTED_TABLE_ID, lvl)'';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- a table name in the nested table is also unique (case-insensitive!)' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''CREATE UNIQUE INDEX '' || SELF.attributes_tab_idx || '' ON '' || ' || chr(10) ||
            '               ''' || dim_id || '_attributes(NESTED_TABLE_ID, UPPER(table_name))'';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- an index for metadata is placed of the attribute name column' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''CREATE INDEX '' || SELF.metadata_idx || '' ON '' || ' || chr(10) ||
            '               ''' || dim_id || '_metadata(attribute_name)'';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- index for the level-hierarchies' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''CREATE UNIQUE INDEX '' || SELF.hierarchy_idx || '' ON '' || ' || chr(10) ||
            '               ''' || dim_id || '_levelhierarchy(NESTED_TABLE_ID, lvl, parent_level)'';' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE drop_constraints IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- mark indexes unusable' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''ALTER TABLE '' || mobject_table || '' DROP CONSTRAINT '' || SELF.primary_key_idx;' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''ALTER TABLE '' || mobject_table || '' DROP CONSTRAINT '' || SELF.unique_id_idx;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE rebuild_constraints IS' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- the ancestors nested table''s KEY IS THE LEVEL OF THE ancestor' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''ALTER TABLE '' || mobject_table || '' ADD CONSTRAINT '' || SELF.primary_key_idx || '' PRIMARY KEY (oname)'';' || chr(10) ||
            '        EXECUTE IMMEDIATE' || chr(10) ||
            '            ''ALTER TABLE '' || mobject_table || '' ADD CONSTRAINT '' || SELF.unique_id_idx || '' UNIQUE (id)'';' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE bulk_create_mobject(onames names_tty,' || chr(10) || 
            '                                                    top_level VARCHAR2,' || chr(10) || 
            '                                                    parents mobject_trty,' || chr(10) || 
            '                                                    level_hierarchy level_hierarchy_tty) IS' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        ids names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        ancestors level_ancestor_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        attribute_tables attribute_table_tty;' || chr(10) ||
            '        attribute_metadata attribute_meta_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        dim_reference REF dimension_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        err error_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        enable_hierarchy_cache INTEGER;' || chr(10) ||
            '        enable_position_cache  INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        specializes INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- this collection holds the surrogate ids' || chr(10) ||
            '        ids := names_tty();' || chr(10) ||
            '        FOR i IN 1 .. onames.COUNT LOOP' || chr(10) ||
            '            ids.EXTEND;' || chr(10) ||
            '            ids(ids.LAST) := ''o'' || ' || mobject_id_seq || '.NEXTVAL;' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- calculate the ancestors once for all m-objects' || chr(10) ||
            '        ancestors := mobject_ty.calculate_ancestors(parents);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- initialize the collections for the attributes' || chr(10) ||
            '        attribute_tables := attribute_table_tty();' || chr(10) ||
            '        attribute_metadata := attribute_meta_tty();' || chr(10) ||
            '        ' || chr(10) ||
            '        -- get the reference of SELF' || chr(10) ||
            '        SELECT REF(d) INTO dim_reference' || chr(10) ||
            '        FROM   dimensions d' || chr(10) ||
            '        WHERE  d.dname = SELF.dname;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- enforce consistency if the flag tells you so' || chr(10) ||
            '        IF SELF.enforce_consistency > 0 THEN' || chr(10) ||
            '            mobject_ty.assert_consistency(top_level, level_hierarchy, parents, dim_reference);' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- save the cache flag status' || chr(10) ||
            '        enable_hierarchy_cache := SELF.enable_hierarchy_cache;' || chr(10) ||
            '        enable_position_cache  := SELF.enable_position_cache;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- do not refresh caches even if it is enabled' || chr(10) ||
            '        IF SELF.enable_hierarchy_cache > 0 THEN' || chr(10) ||
            '            SELF.set_enable_hierarchy_cache(FALSE);' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        IF SELF.enable_position_cache > 0 THEN' || chr(10) ||
            '            SELF.set_enable_position_cache(FALSE);' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        IF parents IS NULL OR parents.COUNT = 0 THEN' || chr(10) ||
            '            specializes := 0;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- perform a bulk insert. Note that we do not invoke' || chr(10) ||
            '        -- the constructor but directly insert the values. This is' || chr(10) ||
            '        -- much faster as all the work is done only once before the insert.' || chr(10) ||
            '        dbms_output.put_line(''FORALL starts: '' || to_char(SYSDATE, ''yyyy/mm/dd hh:MI:ss''));' || chr(10) ||
            '        FORALL i IN INDICES OF onames' || chr(10) ||
            '            INSERT INTO ' || mobject_table || chr(10) ||
            '            (oname, id, top_level, level_hierarchy, parents, ancestors,' || chr(10) ||
            '             attribute_tables, attribute_metadata, dim, specializes)' || chr(10) ||
            '            VALUES(onames(i), ' || chr(10) ||
            '                   ids(i),' || chr(10) ||
            '                   top_level,' || chr(10) ||
            '                   level_hierarchy,' || chr(10) ||
            '                   parents,' || chr(10) ||
            '                   ancestors,' || chr(10) ||
            '                   attribute_tables,' || chr(10) ||
            '                   attribute_metadata,' || chr(10) ||
            '                   dim_reference,' || chr(10) ||
            '                   specializes);' || chr(10) ||
            '        dbms_output.put_line(''FORALL ends: '' || to_char(SYSDATE, ''yyyy/mm/dd hh:MI:ss''));' || chr(10) ||
            '        ' || chr(10) ||
            '        -- restore cache status flags and update the cache, if caches are enabled' || chr(10) ||
            '        SELF.set_enable_hierarchy_cache(enable_hierarchy_cache);' || chr(10) ||
            '        SELF.set_enable_position_cache(enable_position_cache);' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER FUNCTION get_attribute_description(attribute_name VARCHAR2)' || chr(10) ||
            '            RETURN attribute_ty IS' || chr(10) ||
            '        attribute_level VARCHAR2(30);' || chr(10) ||
            '        table_name      VARCHAR2(30);' || chr(10) ||
            '        data_type       VARCHAR2(30);' || chr(10) ||
            '        data_length     VARCHAR2(30);' || chr(10) ||
            '        data_scale      VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- consult the Oracle data dictionary' || chr(10) ||
            '        SELECT x.lvl, utc.table_name, utc.data_type, utc.data_length, utc.data_scale INTO attribute_level, table_name, data_type, data_length, data_scale' || chr(10) ||
            '        FROM   user_tab_columns utc, ' || chr(10) ||
            '               (SELECT UPPER(t.lvl) AS lvl, UPPER(t.table_name) AS table_name' || chr(10) ||
            '                FROM   ' || mobject_table || ' o, TABLE(o.attribute_tables) t) x' || chr(10) ||
            '        WHERE  utc.column_name = UPPER(attribute_name) AND' || chr(10) ||
            '               utc.table_name IN x.table_name;' || chr(10) ||
            '               ' || chr(10) ||
            '        RETURN attribute_ty(attribute_name, attribute_level, table_name, data_type, data_length, data_scale);' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE bulk_set_attribute(attribute_name   VARCHAR2,' || chr(10) ||
            '                                                   attribute_values mobject_value_tty) IS' || chr(10) ||
            '        obj_ref REF mobject_ty;' || chr(10) ||
            '        obj mobject_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        err error_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        attribute_descr attribute_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        update_data VARCHAR2(1000);' || chr(10) ||
            '        ' || chr(10) ||
            '        TYPE number_tty IS TABLE OF NUMBER;' || chr(10) ||
            '        TYPE varchar2_tty IS TABLE OF VARCHAR2(4000); --> 4000 bytes since this is the limit in the database' || chr(10) ||
            '        ' || chr(10) ||
            '        number_values number_tty := number_tty();' || chr(10) ||
            '        varchar2_values varchar2_tty := varchar2_tty();' || chr(10) ||
            '        ' || chr(10) ||
            '        -- cursor variables' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- find out the table name and data type for this attribute.' || chr(10) ||
            '        attribute_descr := SELF.get_attribute_description(attribute_name);' || chr(10) || 
            '        ' || chr(10) ||
            '        -- check consistency, if enabled. inefficient, though.' || chr(10) ||
            '        IF SELF.enforce_consistency > 0 THEN' || chr(10) ||
            '            i := attribute_values.FIRST;' || chr(10) ||
            '            WHILE i IS NOT NULL LOOP' || chr(10) ||
            '                obj_ref := SELF.get_mobject_ref(attribute_values(i).mobject_name);' || chr(10) ||
            '                utl_ref.select_object(obj_ref, obj);' || chr(10) ||
            '                ' || chr(10) ||
            '                IF obj.has_attribute(attribute_name, 1, 0) <= 0 THEN' || chr(10) ||
            '                    SELECT VALUE(r) INTO err' || chr(10) ||
            '                    FROM   errors r' || chr(10) ||
            '                    WHERE  r.error_name = ''consistent_mobject_attribute_not_exists'';' || chr(10) ||
            '                    ' || chr(10) ||
            '                    err.raise_error();' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                i := attribute_values.NEXT(i);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- insert rows for those m-objects where no tuple exists already' || chr(10) ||
            '        FORALL i IN INDICES OF attribute_values' || chr(10) ||
            '            EXECUTE IMMEDIATE' || chr(10) ||
            '                ''INSERT INTO '' || attribute_descr.table_name || '' (obj, oname) ('' || chr(10) ||' || chr(10) ||
            '                ''    SELECT REF(o), o.oname'' || chr(10) ||' || chr(10) ||
            '                ''    FROM '' || mobject_table || '' o'' || chr(10) ||' || chr(10) ||
            '                ''    WHERE o.oname = :1 AND'' || chr(10) ||' || chr(10) ||
            '                ''          NOT EXISTS (SELECT t.oname'' || chr(10) ||' || chr(10) ||
            '                ''                      FROM   '' || attribute_descr.table_name || '' t'' || chr(10) ||' || chr(10) ||
            '                ''                      WHERE  t.oname = o.oname)'' || chr(10) ||' || chr(10) ||
            '                '')''' || chr(10) ||
            '            USING attribute_values(i).mobject_name;' || chr(10) ||
            '        ' || chr(10) ||
            '        update_data :=' || chr(10) ||
            '            ''UPDATE '' || attribute_descr.table_name || '' t'' || chr(10) ||' || chr(10) ||
            '            ''SET    t.'' || attribute_name || '' = :1'' || chr(10) ||' || chr(10) ||
            '            ''WHERE  t.oname = :2'';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- update the rows (insert the attribute value)' || chr(10) ||
            '        CASE attribute_descr.data_type' || chr(10) ||
            '            WHEN ''NUMBER'' THEN' || chr(10) ||
            '                -- have to convert it before inserting, since PLS-00801' || chr(10) ||
            '                i := attribute_values.FIRST;' || chr(10) ||
            '                WHILE i IS NOT NULL LOOP' || chr(10) ||
            '                    number_values.EXTEND;' || chr(10) ||
            '                    number_values(number_values.LAST) := ' || chr(10) ||
            '                        attribute_values(i).attribute_value.accessNumber();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    i := attribute_values.NEXT(i);' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '                FORALL i IN INDICES OF attribute_values' || chr(10) ||
            '                    EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                    USING number_values(i), attribute_values(i).mobject_name;' || chr(10) ||
            '            WHEN ''VARCHAR2'' THEN' || chr(10) ||
            '                -- have to convert it before inserting, since PLS-00801' || chr(10) ||
            '                i := attribute_values.FIRST;' || chr(10) ||
            '                WHILE i IS NOT NULL LOOP' || chr(10) ||
            '                    varchar2_values.EXTEND;' || chr(10) ||
            '                    varchar2_values(varchar2_values.LAST) := ' || chr(10) ||
            '                        attribute_values(i).attribute_value.accessVarchar2();' || chr(10) ||
            '                    ' || chr(10) ||
            '                    i := attribute_values.NEXT(i);' || chr(10) ||
            '                END LOOP;' || chr(10) ||
            '                ' || chr(10) ||
            '                FORALL i IN INDICES OF attribute_values' || chr(10) ||
            '                    EXECUTE IMMEDIATE update_data' || chr(10) ||
            '                    USING varchar2_values(i), attribute_values(i).mobject_name;' || chr(10) ||
            '        END CASE;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) || 
            '    OVERRIDING MEMBER PROCEDURE reload_cache IS' || chr(10) || 
            '        ' || chr(10) || 
            '    BEGIN' || chr(10) ||
            '        SELECT d.level_hierarchy, d.level_positions INTO SELF.level_hierarchy, SELF.level_positions' || chr(10) || 
            '        FROM   dimensions d' || chr(10) || 
            '        WHERE  d.dname = SELF.dname;' || chr(10) ||
            '    END;' || chr(10) || 
            '        ' || chr(10) || 
            '    OVERRIDING MEMBER FUNCTION calculate_level_hierarchy RETURN level_hierarchy_tty IS' || chr(10) || 
            '        ' || chr(10) || 
            '        CURSOR hierarchy_cursor IS' || chr(10) || 
            '            SELECT DISTINCT h.lvl, h.parent_level' || chr(10) || 
            '            FROM   ' || mobject_table || ' o, TABLE(o.level_hierarchy) h;' || chr(10) || 
            '        ' || chr(10) || 
            '        new_hierarchy level_hierarchy_tty;' || chr(10) || 
            '        ' || chr(10) || 
            '        lvl VARCHAR2(30);' || chr(10) || 
            '        parent_level VARCHAR2(30);' || chr(10) || 
            '    BEGIN' || chr(10) ||
            '        OPEN hierarchy_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        new_hierarchy := level_hierarchy_tty();' || chr(10) || 
            '        ' || chr(10) || 
            '        LOOP' || chr(10) ||
            '            FETCH hierarchy_cursor INTO lvl, parent_level;' || chr(10) ||
            '            EXIT WHEN hierarchy_cursor%NOTFOUND;' || chr(10) ||
            '            ' || chr(10) ||
            '            new_hierarchy.EXTEND;' || chr(10) ||
            '            new_hierarchy(new_hierarchy.LAST) :=' || chr(10) ||
            '                level_hierarchy_ty(lvl, parent_level);' || chr(10) ||
            '            ' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        CLOSE hierarchy_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- remove redundant pairs' || chr(10) ||
            '        level_hierarchies.normalize(new_hierarchy);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- persist the changes' || chr(10) ||
            '        RETURN new_hierarchy;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10) || 
            '    OVERRIDING MEMBER PROCEDURE refresh_level_hierarchy IS' || chr(10) || 
            '        ' || chr(10) || 
            '    BEGIN' || chr(10) ||
            '        SELF.level_hierarchy := SELF.calculate_level_hierarchy();' || chr(10) ||
            '        ' || chr(10) ||
            '        -- persist the changes' || chr(10) ||
            '        SELF.persist;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10) ||  
            '    OVERRIDING MEMBER PROCEDURE delete_dimension IS' || chr(10) || 
            '        obj_ref REF mobject_ty;' || chr(10) || 
            '        obj mobject_ty;' || chr(10) || 
            '        ' || chr(10) || 
            '        cnt INTEGER;' || chr(10) || 
            '        ' || chr(10) || 
            '        -- retrieve the root m-object' || chr(10) ||
            '        CURSOR mobject_cursor IS ' || chr(10) || 
            '            SELECT REF(o) FROM ' || mobject_table || ' o WHERE o.parents IS NULL;' || chr(10) || 
            '    BEGIN' || chr(10) ||
            '        -- call the delete method of the dimension''s root m-object' || chr(10) || 
            '        OPEN mobject_cursor;' || chr(10) ||
            '        ' || chr(10) ||  
            '        -- should be ONLY ONE root m-object, but defensive programming here!' || chr(10) || 
            '        LOOP' || chr(10) || 
            '            FETCH mobject_cursor INTO obj_ref;' || chr(10) ||
            '            EXIT WHEN mobject_cursor%NOTFOUND;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- check if the m-object still exists' || chr(10) ||
            '            SELECT COUNT(*) INTO cnt' || chr(10) ||
            '            FROM   ' || mobject_table || ' o' || chr(10) ||
            '            WHERE  REF(o) = obj_ref;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF cnt > 0 THEN' || chr(10) ||
            '                utl_ref.select_object(obj_ref, obj);' || chr(10) ||
            '                ' || chr(10) ||
            '                -- delete the root m-object' || chr(10) ||
            '                IF obj IS NOT NULL THEN' || chr(10) ||
            '                    obj.delete_mobject;' || chr(10) ||
            '                END IF;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) || 
            '        CLOSE mobject_cursor;' || chr(10) || 
            '        ' || chr(10) || 
            '        -- delete the object from the table' || chr(10) || 
            '        DELETE FROM dimensions WHERE dname = SELF.dname;' || chr(10) || 
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            '    -- consistency checks' || chr(10) ||
            '    OVERRIDING MEMBER FUNCTION unique_attribute_induction(attribute_name VARCHAR2)' || chr(10) ||
            '                 RETURN BOOLEAN IS' || chr(10) ||
            '        ' || chr(10) ||
            '        -- get all attribute tables for this dimension' || chr(10) || 
            '        CURSOR table_cursor IS' || chr(10) || 
            '            SELECT t.table_name' || chr(10) || 
            '            FROM   ' || mobject_table || ' o, TABLE(o.attribute_tables) t;' || chr(10) || 
            '        ' || chr(10) || 
            '        table_name1 VARCHAR2(30);' || chr(10) || 
            '        cnt INTEGER;' || chr(10) || 
            '    BEGIN' || chr(10) || 
            '        OPEN table_cursor;' || chr(10) || 
            '        ' || chr(10) || 
            '        LOOP' || chr(10) || 
            '            FETCH table_cursor INTO table_name1;' || chr(10) || 
            '            EXIT WHEN table_cursor%NOTFOUND;' || chr(10) ||  
            '            ' || chr(10) ||   
            '            -- check if the table has a column with the name of the' || chr(10) ||   
            '            -- argument attribute' || chr(10) ||   
            '            SELECT COUNT(*) INTO cnt' || chr(10) || 
            '            FROM   user_tab_columns utc' || chr(10) || 
            '            WHERE  utc.table_name = UPPER(table_name1) AND' || chr(10) ||
            '                   utc.column_name = UPPER(attribute_name);' || chr(10) ||  
            '            ' || chr(10) ||   
            '            EXIT WHEN cnt > 0;' || chr(10) || 
            '        END LOOP;' || chr(10) || 
            '        ' || chr(10) || 
            '        RETURN cnt = 0;' || chr(10) || 
            '    END;' || chr(10) ||
            '    ' || chr(10) || 
            '    OVERRIDING MEMBER FUNCTION unique_level_induction(lvl VARCHAR2)' || chr(10) ||
            '                 RETURN BOOLEAN IS' || chr(10) ||
            '        level_name VARCHAR2(30) := lvl;' || chr(10) ||
            '        ' || chr(10) ||
            '        cnt INTEGER;' || chr(10) || 
            '    BEGIN' || chr(10) ||  
            '        SELECT COUNT(*) INTO cnt' || chr(10) ||
            '        FROM   (SELECT DISTINCT h.lvl AS lvl' || chr(10) || 
            '                FROM   ' || mobject_table || ' o, TABLE(o.level_hierarchy) h) x' || chr(10) ||
            '        WHERE  x.lvl = level_name;' || chr(10) || 
            '        ' || chr(10) || 
            '        RETURN cnt = 0;' || chr(10) || 
            '    END;' || chr(10) ||
            '    ' || chr(10) || 
            '    OVERRIDING MEMBER PROCEDURE export_star(table_name VARCHAR2) IS' || chr(10) ||
            '        create_statement VARCHAR2(32000);' || chr(10) ||
            '        ' || chr(10) || 
            '        insert_statement               VARCHAR2(32000);' || chr(10) ||
            '        insert_statement_levels        VARCHAR2(32000);' || chr(10) ||
            '        insert_statement_select_levels VARCHAR2(32000);' || chr(10) ||
            '        ' || chr(10) || 
            '        update_statement VARCHAR2(32000);' || chr(10) || 
            '        ' || chr(10) || 
            '        level_names names_tty;' || chr(10) || 
            '        table_names names_tty;' || chr(10) || 
            '        attribute_names names_tty;' || chr(10) || 
            '        attribute_names_and_types attribute_tty;' || chr(10) || 
            '        ' || chr(10) || 
            '        i INTEGER;' || chr(10) || 
            '        j INTEGER;' || chr(10) ||
            '        ' || chr(10) || 
            '    BEGIN' || chr(10) ||
            '        -- get all levels in this dimension' || chr(10) ||
            '        SELECT h.lvl BULK COLLECT INTO level_names' || chr(10) ||
            '        FROM   TABLE(SELF.level_positions) h' || chr(10) ||
            '        ORDER BY h.position;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- get all attributes of the dimension''s m-objects' || chr(10) ||
            '        SELECT attribute_ty(utc.column_name, x.lvl, utc.table_name, utc.data_type, utc.data_length, utc.data_scale) BULK COLLECT INTO attribute_names_and_types' || chr(10) ||
            '        FROM   user_tab_columns utc,' || chr(10) ||
            '               (SELECT UPPER(t.table_name) AS table_name, UPPER(t.lvl) AS lvl' || chr(10) ||
            '                FROM   ' || mobject_table || ' o, TABLE(o.attribute_tables) t) x' || chr(10) ||
            '        WHERE  utc.table_name IN x.table_name AND utc.column_name <> ''ONAME'' AND utc.column_name <> ''OBJ'';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- get the names of the attribute table names' || chr(10) ||
            '        SELECT DISTINCT UPPER(t.table_name) BULK COLLECT INTO table_names' || chr(10) ||
            '        FROM   TABLE(attribute_names_and_types) t;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- get the attribute names' || chr(10) ||
            '        SELECT DISTINCT UPPER(t.attribute_name) BULK COLLECT INTO attribute_names' || chr(10) ||
            '        FROM   TABLE(attribute_names_and_types) t;' || chr(10) ||
            '        ' || chr(10) ||
            '        /*****************************************/' || chr(10) ||
            '        -- build the CREATE STATEMENT for the table' || chr(10) ||
            '        ' || chr(10) ||
            '        create_statement := ' || chr(10) ||
            '            ''CREATE TABLE '' || table_name || ''('' || chr(10) ||' || chr(10) ||
            '            ''    id VARCHAR2(30) PRIMARY KEY,'' || chr(10) || ' || chr(10) ||
            '            ''    lvl VARCHAR2(30)'';' || chr(10) ||
            '        ' || chr(10) ||
            '        -- add a column for each level' || chr(10) ||
            '        i := level_names.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            create_statement := create_statement || ' || chr(10) ||
            '                '','' || chr(10) || ' || chr(10) ||
            '                ''    '' || level_names(i) || '' VARCHAR2(30)''; ' || chr(10) ||
            '            ' || chr(10) ||
            '            i := level_names.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- add a column for each attribute' || chr(10) ||
            '        i := attribute_names_and_types.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            create_statement := create_statement || ' || chr(10) ||
            '                '','' || chr(10) || ' || chr(10) ||
            '                ''    '' || attribute_names_and_types(i).attribute_name || '' '' || data_types.to_string(attribute_names_and_types(i).data_type, attribute_names_and_types(i).data_length, attribute_names_and_types(i).data_scale); ' || chr(10) ||
            '            ' || chr(10) ||
            '            i := attribute_names_and_types.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        create_statement := create_statement || chr(10) || '')'';' || chr(10) ||
            '        ' || chr(10) ||
            '        --dbms_output.put_line(create_statement);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- execute the statement and create the table' || chr(10) ||
            '        EXECUTE IMMEDIATE create_statement;' || chr(10) ||
            '        /*****************************************/' || chr(10) ||
            '        ' || chr(10) || 
            '        /******************************************/' || chr(10) ||
            '        -- build the INSERT STATEMENT for the table' || chr(10) || 
            '        i := level_names.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            insert_statement_levels := insert_statement_levels || '', '' || level_names(i);' || chr(10) || 
            '            ' || chr(10) ||
            '            -- TODO: replace hard-coded values with bind variables in the following query' || chr(10) ||  
            '            insert_statement_select_levels := insert_statement_select_levels || ' || chr(10) || 
            '                '', '' || ''NVL((SELECT a.ancestor.oname FROM TABLE(o.ancestors) a WHERE  a.lvl = '''''' || level_names(i) || ''''''), CASE o.top_level WHEN '''''' || level_names(i) || '''''' THEN o.oname END)'';' || chr(10) || 
            '            ' || chr(10) ||  
            '            i := level_names.NEXT(i);' || chr(10) || 
            '        END LOOP;' || chr(10) || 
            '        ' || chr(10) || 
            '        -- insert a tuple for every m-object' || chr(10) || 
            '        insert_statement := insert_statement || chr(10) || ' || chr(10) ||
            '            ''INSERT INTO '' || table_name || '' (id, lvl'' || insert_statement_levels || '') ('' || chr(10) || ' || chr(10) ||
            '            ''    SELECT o.oname, o.top_level'' || insert_statement_select_levels || chr(10) ||' || chr(10) ||
            '            ''    FROM   ' || mobject_table || ' o'' || chr(10) ||' || chr(10) || 
            '            '')'';' || chr(10) || 
            '        ' || chr(10) || 
            '        --dbms_output.put_line(insert_statement);' || chr(10) ||
            '        EXECUTE IMMEDIATE insert_statement;' || chr(10) ||
            '        ' || chr(10) || 
            '        -- build the UPDATE STATEMENTS for the table' || chr(10) ||
            '        -- iterate through the table names and read ' || chr(10) || 
            '        -- from the attribute table to create an update ' || chr(10) || 
            '        i := table_names.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) || 
            '            update_statement := ' || chr(10) ||
            '                ''UPDATE '' || table_name || '' SET ''; ' || chr(10) ||
            '            ' || chr(10) ||
            '            SELECT a.attribute_name BULK COLLECT INTO attribute_names' || chr(10) ||
            '            FROM  TABLE(attribute_names_and_types) a' || chr(10) ||
            '            WHERE a.table_name = table_names(i);' || chr(10) ||
            '            ' || chr(10) ||
            '            j := attribute_names.FIRST;' || chr(10) ||
            '            WHILE j IS NOT NULL LOOP' || chr(10) ||
            '                IF j > 1 THEN' || chr(10) ||
            '                    update_statement := update_statement || '', '';' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                -- TODO: Update statement should perhaps have a WHERE clause.' || chr(10) ||
            '                --       Might lead to problems otherwise, but not sure.' || chr(10) ||
            '                --       export_star for m-cubes definitely needs WHERE clause.' || chr(10) ||
            '                update_statement := update_statement || ' || chr(10) ||
            '                    attribute_names(j) || '' = (SELECT '' || attribute_names(j) || '' FROM '' || table_names(i) || '' WHERE '' || table_names(i) || ''.oname = '' || table_name || ''.id)'';' || chr(10) ||
            '                ' || chr(10) ||
            '                ' || chr(10) ||
            '                j := attribute_names.NEXT(j);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '            ' || chr(10) ||
            '            --dbms_output.put_line(update_statement);' || chr(10) ||
            '            EXECUTE IMMEDIATE update_statement;' || chr(10) ||
            '            ' || chr(10) ||
            '            i := table_names.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) || 
            '        ' || chr(10) || 
            '        /******************************************/' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) || 
            'END;';
                       
        EXECUTE IMMEDIATE create_dimension_body;
    END;
    
    PROCEDURE create_mobject_#_ty(mobject_#_ty VARCHAR2,
                                  mobject_table VARCHAR2,
                                  dname VARCHAR2) IS
        
        create_mobject_body VARCHAR2(15000);
    BEGIN
        create_mobject_body :=
            'CREATE OR REPLACE TYPE BODY ' || mobject_#_ty || ' AS' || chr(10) ||
            ---- the constructor function cannot be inherited
            '    CONSTRUCTOR FUNCTION ' || mobject_#_ty || '(oname VARCHAR2,' || chr(10) ||
            '                                 id VARCHAR2,' || chr(10) ||
            '                                 top_level VARCHAR2,' || chr(10) ||
            '                                 parents mobject_trty,' || chr(10) ||
            '                                 level_hierarchy level_hierarchy_tty)' || chr(10) ||
            '        RETURN SELF AS RESULT IS' || chr(10) ||
            '       ' || chr(10) || 
            '       -- the name of the dimension is hard-coded into the declaration' || chr(10) ||
            '       dimension_name VARCHAR2(30) := ''' || dname || ''';' || chr(10) || 
            '       ' || chr(10) || 
            '       dim dimension_ty;' || chr(10) || 
            '       ' || chr(10) || 
            '       enforce_consistency INTEGER;' || chr(10) || 
            '       ' || chr(10) || 
            '       err error_ty;' || chr(10) || 
            '    BEGIN' || chr(10) ||
            '       SELF.oname := oname;' || chr(10) ||
            '       SELF.id := id;' || chr(10) ||
            '       ' || chr(10) ||
            '       SELF.top_level := top_level;' || chr(10) ||
            '       ' || chr(10) ||
            '       IF parents IS NOT NULL AND parents.count > 0 THEN ' || chr(10) ||
            '           SELF.parents := parents;' || chr(10) ||
            '       ELSE' || chr(10) ||
            '           SELF.parents := NULL;' || chr(10) ||
            '       END IF;' || chr(10) ||
            '       ' || chr(10) ||
            '       SELF.level_hierarchy := level_hierarchy;' || chr(10) ||
            '       SELF.attribute_tables := attribute_table_tty();' || chr(10) ||
            '       SELF.attribute_metadata := attribute_meta_tty();' || chr(10) ||
            '       ' || chr(10) || 
            '       SELF.ancestors := SELF.calculate_ancestors();' || chr(10) || 
            '       ' || chr(10) ||
            '       -- get the dimension''s reference' || chr(10) ||
            '       SELECT REF(d) INTO SELF.dim ' || chr(10) ||
            '       FROM   dimensions d ' || chr(10) ||
            '       WHERE  d.dname = dimension_name;' || chr(10) || 
            '       ' || chr(10) || 
            '       -- check if the dimension enforces consistency' || chr(10) ||
            '       SELECT d.enforce_consistency INTO enforce_consistency' || chr(10) ||  
            '       FROM   dimensions d' || chr(10) ||  
            '       WHERE  d.dname = dimension_name;' || chr(10) ||  
            '       ' || chr(10) ||  
            '       IF enforce_consistency > 0 THEN' || chr(10) || 
            '           -- this procedure raises an error if the m-object is inconsistent' || chr(10) ||
            '           SELF.assert_consistency();' || chr(10) ||  
            '       END IF;' || chr(10) || 
            '       ' || chr(10) || 
            '       -- check if the m-object introduces a new level (or attribute)' || chr(10) || 
            '       IF SELF.does_specialize() THEN' || chr(10) || 
            '           SELF.specializes := 1;' || chr(10) || 
            '       ELSE' || chr(10) || 
            '           SELF.specializes := 0;' || chr(10) || 
            '       END IF;' || chr(10) || 
            '       ' || chr(10) || 
            '       RETURN;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            
            -- implement this here so we can take advantage of indexes
            '    OVERRIDING MEMBER FUNCTION top_level_position RETURN INTEGER IS'  || chr(10) ||
            '        -- hard-code the dimension name into the body' || chr(10) ||
            '        dimension_name VARCHAR2(30) := ''' || dname || ''';' || chr(10) ||
            '        ' || chr(10) ||
            '        level_position INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||  
            '        SELECT p.position INTO level_position' || chr(10) ||
            '        FROM   TABLE(SELECT d.level_positions' || chr(10) ||
            '                     FROM   dimensions d' || chr(10) ||
            '                     WHERE  d.dname = dimension_name) p' || chr(10) ||
            '        WHERE  p.lvl = SELF.top_level;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN level_position;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||        
            
            -- this function is overridden to take advantage of the index
            -- structures when the table is accessed directly. might be an
            -- advantage if one m-object defines a multitude of attribute 
            -- tables.
            '    OVERRIDING MEMBER FUNCTION get_attribute_table(lvl VARCHAR2)' || chr(10) ||
            '                           RETURN VARCHAR2 IS' || chr(10) ||
            '        table_name VARCHAR2(30);' || chr(10) ||
            '        searched_level VARCHAR2(30) := lvl;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        BEGIN' || chr(10) ||
            '            SELECT l.table_name INTO table_name' || chr(10) ||
            '            FROM   TABLE(SELECT o.attribute_tables' || chr(10) ||
            '                         FROM   ' || mobject_table || ' o' || chr(10) ||
            '                         WHERE  o.oname = SELF.oname) l' || chr(10) ||
            '            WHERE  l.lvl = searched_level;' || chr(10) ||
            '        EXCEPTION' || chr(10) ||
            '            WHEN NO_DATA_FOUND THEN' || chr(10) ||
            '                table_name := NULL;' || chr(10) ||
            '        END;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN table_name;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            
            -- the persist procedure has the name of the m-object table
            -- hard-coded into its body.
            '    OVERRIDING MEMBER PROCEDURE persist IS' || chr(10) ||
            '    ' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        UPDATE ' || mobject_table || ' obj ' || chr(10) ||
            '        SET    obj = TREAT(SELF AS ' || mobject_#_ty || ')'  || chr(10) || 
            '        WHERE  obj.oname = SELF.oname;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            '    OVERRIDING MEMBER PROCEDURE delete_attribute_metadata(attribute_name VARCHAR2) IS' || chr(10) ||
            '        searched_name VARCHAR2(30) := attribute_name;' || chr(10) ||
            '        ' || chr(10) ||
            '        descendant_name VARCHAR2(30);' || chr(10) ||
            '        ' || chr(10) ||
            '        CURSOR descendant_cursor IS' || chr(10) ||
            '            SELECT o.oname' || chr(10) ||
            '            FROM   ' || mobject_table || ' o' || chr(10) ||
            '            WHERE  EXISTS(SELECT *' || chr(10) ||
            '                          FROM   TABLE(o.ancestors) a' || chr(10) ||
            '                          WHERE  a.lvl = SELF.top_level AND a.ancestor.oname = SELF.oname);' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- delete the m-object metadata from self' || chr(10) ||
            '        DELETE FROM TABLE(SELECT o.attribute_metadata FROM ' || mobject_table || ' o WHERE o.oname = SELF.oname)' || chr(10) ||
            '        WHERE  attribute_name = searched_name;' || chr(10) ||
            '        ' || chr(10) ||
            '        OPEN descendant_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- delete the metadata from the descendants' || chr(10) ||
            '        LOOP' || chr(10) ||
            '            FETCH descendant_cursor INTO descendant_name;' || chr(10) ||
            '            EXIT WHEN descendant_cursor%NOTFOUND;' || chr(10) ||
            '            ' || chr(10) ||
            '            DELETE FROM TABLE(SELECT o.attribute_metadata FROM ' || mobject_table || ' o WHERE o.oname = descendant_name)' || chr(10) ||
            '            WHERE  attribute_name = searched_name;' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        CLOSE descendant_cursor;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            
            -- the name of the m-object table is again hard-coded into the body
            '    OVERRIDING MEMBER FUNCTION get_descendants RETURN mobject_trty IS' || chr(10) ||
            '        descendants mobject_trty;' || chr(10) ||
            '        ' || chr(10) ||
            '        CURSOR descendant_cursor IS' || chr(10) ||
            '            SELECT REF(o)' || chr(10) ||
            '            FROM   ' || mobject_table || ' o' || chr(10) ||
            '            WHERE  EXISTS(SELECT *' || chr(10) ||
            '                          FROM   TABLE(o.ancestors) a' || chr(10) ||
            '                          WHERE  a.lvl = SELF.top_level AND a.ancestor.oname = SELF.oname)' || chr(10) ||
            '            ORDER BY VALUE(o) ASC;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        OPEN descendant_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        FETCH descendant_cursor BULK COLLECT INTO descendants;' || chr(10) ||
            '        ' || chr(10) ||
            '        CLOSE descendant_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN descendants;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            
            -- the name of the m-object table is again hard-coded into the body
            '    OVERRIDING MEMBER FUNCTION get_descendants_onames RETURN names_tty IS' || chr(10) ||
            '        descendants names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        CURSOR descendant_cursor IS' || chr(10) ||
            '            SELECT o.oname' || chr(10) ||
            '            FROM   ' || mobject_table || ' o' || chr(10) ||
            '            WHERE  EXISTS(SELECT *' || chr(10) ||
            '                          FROM   TABLE(o.ancestors) a' || chr(10) ||
            '                          WHERE  a.lvl = SELF.top_level AND a.ancestor.oname = SELF.oname)' || chr(10) ||
            '            ORDER BY VALUE(o) ASC;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        OPEN descendant_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        FETCH descendant_cursor BULK COLLECT INTO descendants;' || chr(10) ||
            '        ' || chr(10) ||
            '        CLOSE descendant_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN descendants;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            
            -- the name of the m-object table is again hard-coded into the body
            '    OVERRIDING MEMBER PROCEDURE delete_mobject IS' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        cnt INTEGER;' || chr(10) ||
            '        ' || chr(10) ||
            '        descendant_obj_ref REF mobject_ty;' || chr(10) ||
            '        descendant_obj mobject_ty;' || chr(10) ||
            '        ' || chr(10) ||
            '        CURSOR descendant_cursor IS' || chr(10) ||
            '            SELECT REF(o)' || chr(10) ||
            '            FROM   ' || mobject_table || ' o' || chr(10) ||
            '            WHERE  EXISTS(SELECT *' || chr(10) ||
            '                          FROM   TABLE(o.parents) a' || chr(10) ||
            '                          WHERE  a.column_value.oname = SELF.oname);' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- delete all attribute tables created by this m-object.' || chr(10) ||
            '        IF SELF.attribute_tables IS NOT NULL THEN' || chr(10) ||
            '            i := SELF.attribute_tables.FIRST;' || chr(10) ||
            '            WHILE i IS NOT NULL LOOP' || chr(10) ||
            '                IF SELF.attribute_tables(i) IS NOT NULL THEN' || chr(10) ||
            '                    EXECUTE IMMEDIATE' || chr(10) ||
            '                        ''DROP TABLE '' || SELF.attribute_tables(i).table_name;' || chr(10) ||
            '                END IF;' || chr(10) ||
            '                ' || chr(10) ||
            '                i := SELF.attribute_tables.NEXT(i);' || chr(10) ||
            '            END LOOP;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- delete all descendant m-objects' || chr(10) ||
            '        OPEN descendant_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        LOOP' || chr(10) ||
            '            FETCH descendant_cursor INTO descendant_obj_ref;' || chr(10) ||
            '            EXIT WHEN descendant_cursor%NOTFOUND;' || chr(10) ||
            '            ' || chr(10) ||
            '            -- check if the m-object still exists' || chr(10) ||
            '            SELECT COUNT(*) INTO cnt' || chr(10) ||
            '            FROM   ' || mobject_table || ' o' || chr(10) ||
            '            WHERE  REF(o) = descendant_obj_ref;' || chr(10) ||
            '            ' || chr(10) ||
            '            IF cnt > 0 THEN' || chr(10) ||
            '                utl_ref.select_object(descendant_obj_ref, descendant_obj);' || chr(10) ||
            '                ' || chr(10) ||
            '                IF descendant_obj IS NOT NULL THEN' || chr(10) ||
            '                    descendant_obj.delete_mobject;' || chr(10) ||
            '                END IF;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        CLOSE descendant_cursor;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- delete the m-object from the m-object table.' || chr(10) ||
            '        DELETE FROM ' || mobject_table || ' WHERE oname = SELF.oname;' || chr(10) ||
            '    END;' || chr(10) ||
            '    ' || chr(10) ||
            '    STATIC FUNCTION calculate_inherited_levels(parent_onames names_tty, ' || chr(10) ||
            '                                               top_level VARCHAR2,' || chr(10) ||
            '                                               delta_level_hierarchy level_hierarchy_tty) RETURN level_hierarchy_tty IS' || chr(10) ||
            '        parent_level_hierarchy level_hierarchy_tty;' || chr(10) ||
            '        level_hierarchy level_hierarchy_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        parallel_to_toplvl names_tty;' || chr(10) ||
            '        ' || chr(10) ||
            '        top_level1 VARCHAR2(30) := top_level;' || chr(10) ||
            '        ' || chr(10) ||
            '        i INTEGER;' || chr(10) ||
            '    BEGIN' || chr(10) ||
            '        -- get the level hierarchy of the parents' || chr(10) ||
            '        SELECT level_hierarchy_ty(h.lvl, h.parent_level) BULK COLLECT INTO parent_level_hierarchy' || chr(10) ||
            '        FROM   ' || mobject_table || ' o, TABLE(o.level_hierarchy) h' || chr(10) ||
            '        WHERE  o.oname IN (SELECT * FROM TABLE(parent_onames)) AND' || chr(10) ||
            '               h.lvl <> o.top_level;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- get the levels that are parallel to the top level; these have to be removed.' || chr(10) ||
            '        SELECT h.lvl BULK COLLECT INTO parallel_to_toplvl' || chr(10) ||
            '        FROM   ' || mobject_table || ' o, TABLE(o.level_hierarchy) h' || chr(10) ||
            '        WHERE  o.oname IN (SELECT * FROM TABLE(parent_onames)) AND' || chr(10) ||
            '               h.lvl <> o.top_level AND' || chr(10) ||
            '               h.lvl <> top_level1 AND' || chr(10) ||
            '               level_hierarchies.is_sublevel_of(h.lvl, top_level1, o.level_hierarchy, 1) = 0;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- remove the levels parallel to the top level.' || chr(10) ||
            '        i := parallel_to_toplvl.FIRST;' || chr(10) ||
            '        WHILE i IS NOT NULL LOOP' || chr(10) ||
            '            parent_level_hierarchy := ' || chr(10) ||
            '                level_hierarchies.clear_path(parallel_to_toplvl(i), parent_level_hierarchy);' || chr(10) ||
            '            ' || chr(10) ||
            '            i := parallel_to_toplvl.NEXT(i);' || chr(10) ||
            '        END LOOP;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- merge the parent''s level hierarchy with the new (descendant) object''s level hierarchy' || chr(10) ||
            '        level_hierarchy :=' || chr(10) ||
            '            level_hierarchies.merge_level_hierarchies(parent_level_hierarchy,' || chr(10) ||
            '                                                      delta_level_hierarchy);' || chr(10) ||
            '        ' || chr(10) ||
            '        -- sort the level hierarchy' || chr(10) ||
            '        level_hierarchy := ' || chr(10) ||
            '            level_hierarchies.sort_level_hierarchy(level_hierarchy);' || chr(10) ||
            '        ' || chr(10) ||
            '        RETURN level_hierarchy;' || chr(10) ||
            '    END;' || chr(10) ||
            '        ' || chr(10) ||
            'END;';
        
        -- execute the dynamic statement
        EXECUTE IMMEDIATE create_mobject_body;
    END;
    
    PROCEDURE create_hierarchy_triggers(mobject_table VARCHAR2,
                                        dname VARCHAR2,
                                        insert_trigger VARCHAR2,
                                        delete_trigger VARCHAR2) IS
    
        create_insert_trigger VARCHAR2(10000);
        create_delete_trigger VARCHAR2(10000);
    BEGIN
        create_insert_trigger := 
            'CREATE OR REPLACE TRIGGER ' || insert_trigger || ' '|| chr(10) ||
            'AFTER INSERT' || chr(10) ||
            'ON ' || mobject_table || chr(10) ||
            'FOR EACH ROW' || chr(10) ||
            'DECLARE' || chr(10) ||
            '    dimension_name VARCHAR2(30) := ''' || dname || ''';' || chr(10) ||
            '    ' || chr(10) || 
            '    dim_level_hierarchy level_hierarchy_tty;' || chr(10) ||
            '    dim_level_positions level_position_tty;' || chr(10) ||
            '    ' || chr(10) ||    
            '    enable_hierarchy_cache INTEGER;' || chr(10) ||    
            '    enable_position_cache  INTEGER;' || chr(10) ||    
            'BEGIN' || chr(10) ||    
            '    IF :new.level_hierarchy IS NOT NULL THEN' || chr(10) ||
            '        SELECT d.level_hierarchy, d.enable_hierarchy_cache, d.enable_position_cache INTO dim_level_hierarchy, enable_hierarchy_cache, enable_position_cache' || chr(10) ||     
            '        FROM   dimensions d' || chr(10) ||
            '        WHERE  d.dname = dimension_name;' || chr(10) || 
            '        ' || chr(10) || 
            '        IF enable_hierarchy_cache > 0 THEN' || chr(10) ||
            '            -- calculate the new global level-hierarchy of the dimension' || chr(10) ||
            '            dim_level_hierarchy := ' || chr(10) ||
            '                level_hierarchies.merge_level_hierarchies(dim_level_hierarchy,' || chr(10) ||
            '                                                          :new.level_hierarchy);' || chr(10) ||
            '            ' || chr(10) ||
            '            IF enable_position_cache > 0 THEN' || chr(10) ||
            '                -- update the level-position cache' || chr(10) ||
            '                dim_level_positions := ' || chr(10) ||
            '                    level_hierarchies.get_level_positions(dim_level_hierarchy);' || chr(10) ||
            '                ' || chr(10) ||
            '                -- update both the level-hierarchy and position cache' || chr(10) ||
            '                UPDATE dimensions' || chr(10) ||
            '                SET    level_hierarchy = dim_level_hierarchy,' || chr(10) ||
            '                       level_positions = dim_level_positions' || chr(10) ||
            '                WHERE  dname = dimension_name;' || chr(10) ||
            '            ELSE' || chr(10) ||
            '                -- only update the level-hierarchy cache' || chr(10) ||
            '                UPDATE dimensions' || chr(10) ||
            '                SET    level_hierarchy = dim_level_hierarchy' || chr(10) ||
            '                WHERE  dname = dimension_name;' || chr(10) ||
            '            END IF;' || chr(10) ||
            '        END IF;' || chr(10) ||
            '    END IF;' || chr(10) ||
            'END;';
        
        create_delete_trigger := 
            'CREATE OR REPLACE TRIGGER ' || delete_trigger || ' '|| chr(10) ||
            'AFTER DELETE' || chr(10) ||
            'ON ' || mobject_table || chr(10) ||
            'DECLARE' || chr(10) ||
            '    dimension_name VARCHAR2(30) := ''' || dname || ''';' || chr(10) ||
            '    dim dimension_ty;' || chr(10) ||
            'BEGIN' || chr(10) ||
            '    -- get the dimension object' || chr(10) ||
            '    SELECT VALUE(d) INTO dim FROM dimensions d WHERE d.dname = dimension_name;' || chr(10) ||
            '    ' || chr(10) ||
            '    IF dim IS NOT NULL THEN' || chr(10) ||
            '        -- refresh the level-hierarchy' || chr(10) ||
            '        dim.refresh_level_hierarchy;' || chr(10) ||
            '        ' || chr(10) ||
            '        -- refresh the position cache' || chr(10) ||
            '        dim.refresh_level_positions;' || chr(10) ||
            '    END IF;' || chr(10) ||
            'END;';
        
        EXECUTE IMMEDIATE
            create_insert_trigger;
            
        EXECUTE IMMEDIATE
            create_delete_trigger;
    END;
    
    
    ---------------------- PUBLIC METHODS ----------------------
    
    /**
     * Use this function to create a new dimension. The dimension object type
     * and the m-object object type specializations are created dynamically and
     * the creation is transparent to the user.
     *
     * @param dname the unique name of the new dimension (30 bytes).
     *
     * @return a reference to the newly created dimension.
     */
    FUNCTION create_dimension(dname VARCHAR2) 
            RETURN REF dimension_ty IS
        id VARCHAR2(10);
        
        dimension_#_ty VARCHAR2(30);
        mobject_#_ty VARCHAR2(30);
        mobject_table VARCHAR2(30);
        mobject_id_seq_pkg VARCHAR2(30);
        mobject_id_seq_seq VARCHAR2(30);
        
        primary_key_idx    VARCHAR2(30);
        unique_id_idx      VARCHAR2(30);
        ancestors_lvl_idx  VARCHAR2(30);
        ancestors_pk_idx   VARCHAR2(30);
        attributes_lvl_idx VARCHAR2(30);
        attributes_tab_idx VARCHAR2(30);
        hierarchy_idx      VARCHAR2(30);
        metadata_idx       VARCHAR2(30);
        
        insert_trigger VARCHAR2(30);
        delete_trigger VARCHAR2(30);
        
        new_dim_ref REF dimension_ty;
        new_dim dimension_ty;
        
        sql_cursor INTEGER;
        rows_processed INTEGER;
    BEGIN
        -- create the dimensions table (if not exists)
        create_dimensions_table();
        
        EXECUTE IMMEDIATE
            'SELECT ''d'' || dimensions_seq_pkg.NEXT_VAL() FROM dual'
            INTO id; 
        
        -- define the names of the specialized, dimension-specific object types
        dimension_#_ty := 'dimension_' || id || '_ty';
        mobject_#_ty := 'mobject_' || id || '_ty';
        mobject_table := id;
        
        mobject_id_seq_pkg := id || '_seq_pkg';
        mobject_id_seq_seq := id || '_seq_seq';
        
        primary_key_idx    := id || '_pk_idx';
        unique_id_idx      := id || '_unique_id_idx';
        ancestors_lvl_idx  := id || '_ancestors_lvl_idx';
        ancestors_pk_idx   := id || '_ancestors_pk_idx';
        attributes_lvl_idx := id || '_attributes_lvl_idx';
        attributes_tab_idx := id || '_attributes_tab_idx';
        hierarchy_idx      := id || '_hierarchy_idx';
        metadata_idx       := id || '_metadata_idx';
        
        insert_trigger := id || '_insert_hierarchy';
        delete_trigger := id || '_delete_hierarchy';
        
        -- dynamically specialize the object types, create tables and triggers
        create_type_headers(dimension_#_ty, mobject_#_ty);
        create_mobject_table(mobject_#_ty, mobject_table, id, primary_key_idx,
                             unique_id_idx, ancestors_lvl_idx, ancestors_pk_idx,
                             attributes_lvl_idx, attributes_tab_idx, hierarchy_idx,
                             metadata_idx);
        create_sequence(mobject_id_seq_pkg, mobject_id_seq_seq);
        create_dimension_#_ty(dimension_#_ty, id, mobject_#_ty, mobject_table, mobject_id_seq_pkg, 
                              primary_key_idx, unique_id_idx, ancestors_lvl_idx, ancestors_pk_idx,
                              attributes_lvl_idx, attributes_tab_idx, hierarchy_idx, metadata_idx);
        create_mobject_#_ty(mobject_#_ty, mobject_table, dname);
        create_hierarchy_triggers(mobject_table, dname, insert_trigger, delete_trigger);
                
        ---- use dynamic sql to create the dimension
        sql_cursor := dbms_sql.open_cursor;
        
        dbms_sql.parse(sql_cursor,
                       'DECLARE' || chr(10) ||
                       '    new_dim ' || dimension_#_ty || ';' || chr(10) ||
                       'BEGIN' || chr(10) ||
                       '    -- create the new dimension' || chr(10) ||
                       '    new_dim := ' || dimension_#_ty || '(:dname, :id);' || chr(10) ||
                       '    ' || chr(10) ||
                       '    -- insert the dimension into the dimensions table' || chr(10) ||
                       '    INSERT INTO dimensions VALUES(new_dim);' || chr(10) ||
                       '    ' || chr(10) ||
                       '    --:new_dim := new_dim;' || chr(10) ||
                       'END;',
                       dbms_sql.native);
        
        --dbms_sql.bind_variable(sql_cursor, 'new_dim', new_dim);
        dbms_sql.bind_variable(sql_cursor, 'dname', dname);
        dbms_sql.bind_variable(sql_cursor, 'id', id);
        
        rows_processed := dbms_sql.execute(sql_cursor);
        
        -- retrieve the new dimension object
        --IF rows_processed > 0 THEN
        --    dbms_sql.variable_value(sql_cursor, 'new_dim', new_dim);
        --END IF;
        
        -- close the cursor
        dbms_sql.close_cursor(sql_cursor);
        
        -- get the reference of the new dimension
        -- TODO: Perhaps make a function 
        --       dimension_ty.get_reference RETURN REF dimension_ty,
        --       so that dynamic SQL is eliminated here.
        EXECUTE IMMEDIATE 'SELECT REF(d) FROM dimensions d WHERE d.dname = :1'
            INTO new_dim_ref
            USING dname;
        
        RETURN new_dim_ref;
    END;
    
    PROCEDURE delete_dimension(dname VARCHAR2) IS
        sql_cursor INTEGER;
        rows_processed INTEGER;
                
        attribute_tables names_tty;       
        
        dimension_#_ty VARCHAR2(30);
        mobject_#_ty VARCHAR2(30);
        mobject_table VARCHAR2(30);
        mobject_id_seq VARCHAR2(30);
        
        i INTEGER;
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT dimension_#_ty, mobject_#_ty, mobject_table, mobject_id_seq' || chr(10) ||
            'FROM   dimensions' || chr(10) ||
            'WHERE  dname = :1' || chr(10)
        INTO dimension_#_ty, mobject_#_ty, mobject_table, mobject_id_seq
        USING dname;
        
        EXECUTE IMMEDIATE
            'DELETE FROM dimensions WHERE dname = :1'
        USING dname;
        
        -- delete the attribute tables
        EXECUTE IMMEDIATE
            'SELECT DISTINCT t.table_name' || chr(10) ||
            'FROM   ' || mobject_table || ' o, TABLE(o.attribute_tables) t' || chr(10)
        BULK COLLECT INTO attribute_tables;
        
        i := attribute_tables.FIRST;
        WHILE i IS NOT NULL LOOP
            EXECUTE IMMEDIATE
                'DROP TABLE ' || attribute_tables(i);
            
            i := attribute_tables.NEXT(i);
        END LOOP;
                
        -- the dynamically created object types are deleted by this method
        IF mobject_table IS NOT NULL THEN
            EXECUTE IMMEDIATE
                'DROP TABLE ' || mobject_table;
        END IF;
          
        IF mobject_#_ty IS NOT NULL THEN
            EXECUTE IMMEDIATE
                'DROP TYPE ' || mobject_#_ty || ' VALIDATE';
        END IF;
        
        IF dimension_#_ty IS NOT NULL THEN
            EXECUTE IMMEDIATE
                'DROP TYPE ' || dimension_#_ty || ' VALIDATE';
        END IF;
        
        IF mobject_id_seq IS NOT NULL THEN
            EXECUTE IMMEDIATE
                'BEGIN ' || mobject_id_seq || '.delete_sequence; END;';
            
            EXECUTE IMMEDIATE
                'DROP PACKAGE ' || mobject_id_seq || '';
        END IF;
    END;
END;
/


CREATE OR REPLACE TYPE BODY dimension_ty AS    
    MEMBER FUNCTION get_level_position(level_name VARCHAR2) RETURN INTEGER IS        
        pos INTEGER;
    BEGIN
        SELECT p.position INTO pos
        FROM   TABLE(SELF.level_positions) p
        WHERE  p.lvl = level_name;
        
        RETURN pos;
    END;
    
    MEMBER PROCEDURE insert_mobject(oname VARCHAR2,
                                    top_level VARCHAR2,
                                    parents mobject_trty,
                                    level_hierarchy level_hierarchy_tty) IS
    BEGIN
        NULL;
    END;
    
    MEMBER PROCEDURE insert_mobject(oname VARCHAR2, 
                                    id VARCHAR2,
                                    top_level VARCHAR2,
                                    parents mobject_trty,
                                    level_hierarchy level_hierarchy_tty) IS
    BEGIN
        NULL;
    END;
    
    /**
     * 
     * @see insert_mobject
     */
    MEMBER FUNCTION create_mobject(SELF IN OUT NOCOPY dimension_ty,
                                   oname VARCHAR2,
                                   id VARCHAR2,
                                   top_level VARCHAR2,
                                   parents mobject_trty,
                                   level_hierarchy level_hierarchy_tty)
        RETURN REF mobject_ty IS
        
        new_obj REF mobject_ty;
    BEGIN
        -- insert the mobject
        SELF.insert_mobject(oname, 
                            id,
                            top_level, 
                            parents, 
                            level_hierarchy);
        
        -- get the newly created m-object
        new_obj := SELF.get_mobject_ref(oname);
        
        RETURN new_obj;
    END;
    
    /**
     * 
     * @see insert_mobject
     */
    MEMBER FUNCTION create_mobject(SELF IN OUT NOCOPY dimension_ty,
                                   oname VARCHAR2,
                                   top_level VARCHAR2,
                                   parents mobject_trty,
                                   level_hierarchy level_hierarchy_tty)
        RETURN REF mobject_ty IS
        
        id VARCHAR2(10);
        
        new_obj REF mobject_ty;
    BEGIN
        -- insert the mobject
        SELF.insert_mobject(oname,
                            top_level, 
                            parents, 
                            level_hierarchy);
        
        -- get the newly created m-object
        new_obj := SELF.get_mobject_ref(oname);
        
        RETURN new_obj;
    END;
    
    
    MEMBER PROCEDURE bulk_create_mobject(onames          names_tty, 
                                         top_level       VARCHAR2,
                                         parents         mobject_trty,
                                         level_hierarchy level_hierarchy_tty) IS
    BEGIN
        NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE bulk_set_attribute(attribute_name   VARCHAR2,
                                        attribute_values mobject_value_tty) IS
    
    BEGIN
        NULL;
    END;
    
    -- NOTE: HANGS WHEN TRYING TO OVERRIDE THIS METHOD! DO NOT KNOW WHY!
    MEMBER FUNCTION get_mobject_ref(oname VARCHAR2) RETURN REF mobject_ty IS
        obj_ref REF mobject_ty;
    BEGIN
        EXECUTE IMMEDIATE
           'SELECT REF(o) FROM ' || SELF.mobject_table || ' o WHERE o.oname = :1'
           INTO obj_ref
           USING oname;
        
        RETURN obj_ref;
    END;    
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE export_star(table_name VARCHAR2) IS
        
    BEGIN
        NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE persist IS
    
    BEGIN
        NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE delete_dimension IS
        
    BEGIN
        NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE refresh_level_hierarchy IS
        
    BEGIN
        NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER FUNCTION calculate_level_hierarchy RETURN level_hierarchy_tty IS
        
    BEGIN
        RETURN NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE reload_cache IS
        
    BEGIN
        NULL;
    END;
    
    MEMBER PROCEDURE refresh_level_positions IS
        
    BEGIN
        
        -- completely reset the cache
        SELF.level_positions := 
            level_hierarchies.get_level_positions(SELF.level_hierarchy);
        
        SELF.persist;
    END;
    
    -- consistency checks
    MEMBER PROCEDURE set_enforce_consistency(enforce_consistency BOOLEAN) IS
    
    BEGIN
        IF enforce_consistency THEN
            SELF.enforce_consistency := 1;
        ELSE
            SELF.enforce_consistency := 0;
        END IF;
        
        SELF.persist;
    END;
    
    MEMBER PROCEDURE set_enforce_consistency(enforce_consistency INTEGER) IS
    
    BEGIN
        SELF.enforce_consistency := enforce_consistency;
        
        SELF.persist;
    END;
    
    
    MEMBER PROCEDURE set_enable_hierarchy_cache(enable_hierarchy_cache INTEGER) IS
    
    BEGIN
        SELF.enable_hierarchy_cache := enable_hierarchy_cache;
        
        IF enable_hierarchy_cache > 0 THEN
            -- refresh the level-hierarchy
            SELF.refresh_level_hierarchy;
        END IF;
        
        SELF.persist;
    END;
    
    MEMBER PROCEDURE set_enable_hierarchy_cache(enable_hierarchy_cache BOOLEAN) IS
    
    BEGIN
        IF enable_hierarchy_cache THEN
            SELF.enable_hierarchy_cache := 1;
            
            -- refresh the level-hierarchy
            SELF.refresh_level_hierarchy;
        ELSE
            SELF.enable_hierarchy_cache := 0;
        END IF;
        
        SELF.persist;
    END;
    
    
    MEMBER PROCEDURE set_enable_position_cache(enable_position_cache INTEGER) IS
    
    BEGIN
        SELF.enable_position_cache := enable_position_cache;
        
        IF enable_position_cache > 0 THEN
            -- refresh the level-hierarchy
            SELF.refresh_level_positions;
        END IF;
        
        SELF.persist;
    END;
    
    MEMBER PROCEDURE set_enable_position_cache(enable_position_cache BOOLEAN) IS
    
    BEGIN
        IF enable_position_cache THEN
            SELF.enable_position_cache := 1;
            
            -- refresh the level-hierarchy
            SELF.refresh_level_positions;
        ELSE
            SELF.enable_position_cache := 0;
        END IF;
        
        SELF.persist;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE drop_indexes IS
        
    BEGIN
        NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE rebuild_indexes IS
        
    BEGIN
        NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE drop_constraints IS
        
    BEGIN
        NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE rebuild_constraints IS
        
    BEGIN
        NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER FUNCTION unique_attribute_induction(attribute_name VARCHAR2)
        RETURN BOOLEAN IS
    
    BEGIN
        RETURN NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER FUNCTION unique_level_induction(lvl VARCHAR2)
        RETURN BOOLEAN IS
        
    BEGIN
        RETURN NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER FUNCTION get_attribute_description(attribute_name VARCHAR2) 
        RETURN attribute_ty IS
    
    BEGIN
        RETURN NULL;
    END;
END;
/
