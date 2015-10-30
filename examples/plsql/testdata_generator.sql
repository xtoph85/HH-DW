
DROP PACKAGE testdata_generator;

CREATE OR REPLACE PACKAGE testdata_generator AS    
    /**
     * This function generates PL/SQL code for the creation of a dimension along
     * with the corresponding m-objects.
     * 
     * @param dname the name of the dimension that is to be created
     * @param nr_of_levels the number of levels of the root m-object 
     * @param nr_of_children the number of concretizations of each non-leaf m-object
     * @param children_grow_factor the factor with which to multiply the number 
     *                             of children of an m-object, taking as basis
     *                             the m-object's parent's number of children.
     * @param additional_levels number of additional levels introduced by each m-object
     * @param max_depth_of_intro steps from root until which m-objects introduce new levels
     * @param heterogeneitiy_ratio percentage of children of one m-object that introduce new levels
     * @param bulk_statements use bulk statements or not?
     **/
    FUNCTION generate_mobject_testdata(dname                VARCHAR2,
                                       nr_of_levels         INTEGER,
                                       nr_of_children       INTEGER,
                                       children_grow_factor NUMBER,
                                       additional_levels    INTEGER,
                                       max_depth_of_intro   INTEGER,
                                       heterogeneity_ratio  NUMBER,
                                       bulk_statements      BOOLEAN)
        RETURN CLOB;
    
    -- @todo generation method for m-cube data
END;
/

CREATE OR REPLACE PACKAGE BODY testdata_generator AS    
    FUNCTION generate_mobject_testdata(dname                VARCHAR2,
                                       nr_of_levels         INTEGER,
                                       nr_of_children       INTEGER,
                                       children_grow_factor NUMBER,
                                       additional_levels    INTEGER,
                                       max_depth_of_intro   INTEGER,
                                       heterogeneity_ratio  NUMBER,
                                       bulk_statements      BOOLEAN)
            RETURN CLOB IS
        
        plsql_code CLOB;
        
        create_root_mobject VARCHAR2(1000);
        
        mobject_name_seq INTEGER := 0; -- use this variable as sequence counter 
                                       -- for generating the m-object names.
        
        level_name_seq INTEGER := 0; -- use this variable as sequence counter
                                     -- for generating names of the dimension's
                                     -- levels.
                                     
        root_mobj_name           VARCHAR2(10);
        root_mobj_id             VARCHAR2(10);
        root_mobj_toplevel       VARCHAR2(10);
        root_mobj_levelhierarchy VARCHAR2(500);
        
        
        i INTEGER;
        
        -- The curr_val function takes a positive integer as input and returns
        -- an alphanumeric value.
        FUNCTION curr_val(seq_counter INTEGER) RETURN VARCHAR2 IS
            val VARCHAR2(10);
        BEGIN
            SELECT SUBSTR(base36.val, MOD(TRUNC(seq_counter/2821109907456),36)+1, 1) ||  -- 36^8
                   SUBSTR(base36.val, MOD(TRUNC(seq_counter/78364164096),36)+1, 1) ||  -- 36^7
                   SUBSTR(base36.val, MOD(TRUNC(seq_counter/2176782336),36)+1, 1) ||  -- 36^6
                   SUBSTR(base36.val, MOD(TRUNC(seq_counter/60466176),36)+1, 1) ||  -- 36^5
                   SUBSTR(base36.val, MOD(TRUNC(seq_counter/1679616),36)+1, 1) ||  -- 36^4
                   SUBSTR(base36.val, MOD(TRUNC(seq_counter/46656),36)+1, 1) ||  -- 36^3
                   SUBSTR(base36.val, MOD(TRUNC(seq_counter/1296),36)+1, 1) ||  -- 36^2
                   SUBSTR(base36.val, MOD(TRUNC(seq_counter/36),36)+1, 1) ||  -- 36^1
                   SUBSTR(base36.val, MOD(seq_counter,36)+1, 1)
            INTO   val
            FROM   (SELECT '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ' AS val FROM dual) base36;
            
            RETURN val;
        END;
        
        -- The next_val function takes a positive integer as input, increases
        -- its value by 1, and returns an alphanumeric value corresponding to
        -- the raised input value. The input integer is passed by reference, the
        -- increase of the input number is visible for the caller. This function
        -- may be used to realize a local alphanumeric sequence.
        FUNCTION next_val(seq_counter IN OUT NOCOPY INTEGER) RETURN VARCHAR2 IS
            val VARCHAR2(10);
        BEGIN
            seq_counter := seq_counter + 1;
            
            val := curr_val(seq_counter);
            
            RETURN val;
        END;
    BEGIN
        root_mobj_name := 'o' || next_val(mobject_name_seq);
        root_mobj_id   := root_mobj_name;
        
        root_mobj_levelhierarchy := 
            'level_hierarchy_tty(';
            
        root_mobj_toplevel := 'l' || next_val(level_name_seq);
        
        i := 0;
        WHILE i < nr_of_levels LOOP
            IF i > 0 THEN
                root_mobj_levelhierarchy := root_mobj_levelhierarchy|| ', ' || chr(10) || '                            ';
            END IF;
            
            IF i = 0 THEN
                root_mobj_levelhierarchy := root_mobj_levelhierarchy ||
                    'level_hierarchy_ty(''l' || curr_val(level_name_seq) || ''', NULL)';
            ELSE 
                root_mobj_levelhierarchy := root_mobj_levelhierarchy ||
                    'level_hierarchy_ty(''l' || next_val(level_name_seq) || ''', ''l' || curr_val(level_name_seq - 1) || ''')';
            END IF;
            
            i := i + 1;
        END LOOP;
        
        create_root_mobject := 
            '    dim.create_mobject(' || chr(10) ||
            '        ''' || root_mobj_name || ''',' || chr(10) ||
            '        ''' || root_mobj_id || ''',' || chr(10) ||
            '        ''' || root_mobj_toplevel || ''',' || chr(10) ||
            '        NULL,' || chr(10) ||
            '        ' || root_mobj_levelhierarchy || ')' || chr(10) ||
            '    );' || chr(10);
        
        plsql_code :=
            'DECLARE' || chr(10) ||
            '    dim_ref REF dimension_ty;' || chr(10) ||
            '    dim dimension_ty;' || chr(10) ||
            'BEGIN' || chr(10) ||
            '    dim_ref := dimension.create_dimension(''' || dname || ''');' || chr(10) ||
            '    utl_ref.select_object(dim_ref, dim);' || chr(10) ||
            '    ' || chr(10) ||
            '    -- Create the root m-object' || chr(10) ||
            
            create_root_mobject ||
            
            '    ' || chr(10) ||
            'END;' || chr(10) ||
            '/' || chr(10);
        
        RETURN plsql_code;
    END;
END;
/

