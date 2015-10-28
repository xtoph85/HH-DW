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

DROP TYPE mobject_ty FORCE;
DROP TYPE mobject_trty FORCE;
DROP TYPE mobject_tty FORCE;
DROP TYPE level_ancestor_tty FORCE;
DROP TYPE level_ancestor_ty FORCE;
DROP TYPE level_hierarchy_tty FORCE;
DROP TYPE level_hierarchy_ty FORCE;
DROP TYPE level_position_ty FORCE;
DROP TYPE level_position_tty FORCE;
DROP TYPE attribute_table_ty FORCE;
DROP TYPE attribute_table_tty FORCE;
DROP TYPE attribute_meta_ty FORCE;
DROP TYPE attribute_meta_tty FORCE;
DROP TYPE mobject_value_ty FORCE;
DROP TYPE mobject_value_tty FORCE;
DROP TYPE attribute_ty FORCE;
DROP TYPE attribute_tty FORCE;
DROP TYPE attribute_unit_tty FORCE;
DROP TYPE attribute_unit_ty FORCE;
DROP TYPE dimension_ty FORCE;
DROP TYPE names_tty FORCE;
DROP TYPE long_string_tty FORCE;

DROP PACKAGE collections;
DROP PACKAGE level_hierarchies;
DROP PACKAGE attribute_collections;
DROP PACKAGE data_types;

DROP PACKAGE consistent_mobj_concretization;

--------------------------------------------------------------------------------
--                               DECLARATION                                  --
--------------------------------------------------------------------------------

CREATE OR REPLACE TYPE names_tty IS TABLE OF VARCHAR2(30);
/

CREATE OR REPLACE TYPE long_string_tty IS TABLE OF VARCHAR2(1000);
/

CREATE OR REPLACE TYPE mobject_ty;
/

CREATE OR REPLACE TYPE dimension_ty;
/

-- this is used to get information about an attribute: the table where the
-- values are stored, the data_type etc.
CREATE OR REPLACE TYPE attribute_ty AS OBJECT(
    attribute_name VARCHAR2(30),
    attribute_level VARCHAR2(30),
    table_name VARCHAR2(30),
    data_type VARCHAR2(30),
    data_length NUMBER,
    data_scale NUMBER
);
/

CREATE OR REPLACE TYPE attribute_tty AS TABLE OF attribute_ty;
/

CREATE OR REPLACE TYPE level_hierarchy_ty AS OBJECT (lvl VARCHAR2(30),
                                                     parent_level VARCHAR2(30));
/

CREATE OR REPLACE TYPE level_hierarchy_tty AS TABLE OF level_hierarchy_ty;
/

CREATE OR REPLACE TYPE level_position_ty AS OBJECT (lvl VARCHAR2(30), 
                                                    position INTEGER);
/

CREATE OR REPLACE TYPE level_position_tty AS TABLE OF level_position_ty;
/

CREATE OR REPLACE TYPE mobject_trty AS TABLE OF REF mobject_ty;
/

CREATE OR REPLACE TYPE level_ancestor_ty AS OBJECT (lvl VARCHAR2(30), 
                                                    ancestor REF mobject_ty);
/

CREATE OR REPLACE TYPE level_ancestor_tty AS TABLE OF level_ancestor_ty;
/

CREATE OR REPLACE TYPE attribute_table_ty AS OBJECT (lvl VARCHAR2(30), 
                                                     table_name VARCHAR2(30));
/

CREATE OR REPLACE TYPE attribute_table_tty AS TABLE OF attribute_table_ty;
/

CREATE OR REPLACE TYPE attribute_meta_ty AS OBJECT (attribute_name VARCHAR2(30),
                                                    attribute_level VARCHAR2(30), 
                                                    metalevel VARCHAR2(30),
                                                    default_value NUMBER,
                                                    attribute_value ANYDATA);
/

CREATE OR REPLACE TYPE attribute_meta_tty AS TABLE OF attribute_meta_ty;
/


CREATE OR REPLACE TYPE attribute_unit_ty AS OBJECT (
    measure_name VARCHAR2(30),
    measure_unit ANYDATA
);
/

CREATE OR REPLACE TYPE attribute_unit_tty AS TABLE OF attribute_unit_ty;
/


CREATE OR REPLACE TYPE mobject_value_ty AS OBJECT (
    mobject_name VARCHAR2(30),
    attribute_value ANYDATA
);
/

CREATE OR REPLACE TYPE mobject_value_tty AS TABLE OF mobject_value_ty;
/

CREATE OR REPLACE TYPE mobject_ty AS OBJECT (
    oname              VARCHAR2(30),
    id                 VARCHAR2(10), -- unique surrogate key
    
    top_level          VARCHAR2(30),
    level_hierarchy    level_hierarchy_tty,
    
    parents            mobject_trty,
    ancestors          level_ancestor_tty,
    
    attribute_tables   attribute_table_tty,
    attribute_metadata attribute_meta_tty,
    
    -- flag that stores if m-relationship adds schema information
    specializes        INTEGER, -- 1 if it adds attributes or levels, 0 otherwise
    
    -- every object has a reference to its dimension
    dim                REF dimension_ty,
    
    -- attribute-related DML methods
    MEMBER PROCEDURE add_attribute(attribute_name     VARCHAR2,
                                   attribute_level    VARCHAR2,
                                   attribute_datatype VARCHAR2),
    MEMBER PROCEDURE set_attribute(attribute_name  VARCHAR2, 
                                   metalevel       VARCHAR2,
                                   default_value   BOOLEAN,
                                   attribute_value ANYDATA),
    MEMBER PROCEDURE set_attribute(attribute_name  VARCHAR2,
                                   attribute_value ANYDATA),
    MEMBER PROCEDURE delete_attribute(attribute_name VARCHAR2),
    
        -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE delete_attribute_metadata(attribute_name VARCHAR2),
    
    -- attribute-related query methods
    MEMBER FUNCTION has_attribute(attribute_name  IN  VARCHAR2,
                                  top_level_only  IN  BOOLEAN,
                                  introduced_only IN  BOOLEAN,
                                  description     OUT attribute_ty) RETURN BOOLEAN,
    MEMBER FUNCTION has_attribute(attribute_name  VARCHAR2,
                                  top_level_only  INTEGER,
                                  introduced_only INTEGER) RETURN INTEGER,
    
    MEMBER FUNCTION get_attribute(attribute_name VARCHAR2) RETURN ANYDATA,
    
    MEMBER FUNCTION list_attributes(top_level_only  BOOLEAN,
                                    introduced_only BOOLEAN) RETURN attribute_tty,
    MEMBER FUNCTION list_attributes(top_level_only  INTEGER,
                                    introduced_only INTEGER) RETURN attribute_tty,
    
        -- TO BE OVERRIDDEN!
    MEMBER FUNCTION get_attribute_table(lvl VARCHAR2) 
             RETURN VARCHAR2,
    
    -- other DML methods
        -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE persist,
        -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE delete_mobject,
    
    -- other query methods
        -- TO BE OVERRIDDEN!
    MEMBER FUNCTION get_descendants RETURN mobject_trty,
        -- TO BE OVERRIDDEN!
    MEMBER FUNCTION get_descendants_onames RETURN names_tty,
    
    ORDER MEMBER FUNCTION compare_to(other mobject_ty) RETURN INTEGER,
    PRAGMA RESTRICT_REFERENCES(compare_to, WNDS),
    
    MEMBER FUNCTION compare_to#map(other mobject_ty) RETURN INTEGER,
    PRAGMA RESTRICT_REFERENCES(compare_to#map, WNDS),
    
    -- "MAP" function
    MEMBER FUNCTION top_level_position RETURN INTEGER,
    PRAGMA RESTRICT_REFERENCES(top_level_position, WNDS),
    
    MEMBER FUNCTION is_descendant_of(other_oname VARCHAR2) RETURN INTEGER,
    PRAGMA RESTRICT_REFERENCES(is_descendant_of, WNDS),
    
    MEMBER FUNCTION is_descendant_of(other REF mobject_ty) RETURN INTEGER,
    PRAGMA RESTRICT_REFERENCES(is_descendant_of, WNDS),
    
    MEMBER FUNCTION has_level(lvl VARCHAR2) RETURN BOOLEAN,
    MEMBER FUNCTION introduced_level(lvl VARCHAR2) RETURN BOOLEAN,
    
    -- consistency check
    MEMBER PROCEDURE assert_consistency,
    STATIC PROCEDURE assert_consistency(top_level       VARCHAR2,
                                        level_hierarchy level_hierarchy_tty,
                                        parents         mobject_trty,
                                        dim_ref         REF dimension_ty),
    
    -- does the m-object specialize the schema?
    MEMBER FUNCTION does_specialize RETURN BOOLEAN,
    
    -- helper functions / procedures
    MEMBER FUNCTION calculate_ancestors RETURN level_ancestor_tty,
    STATIC FUNCTION calculate_ancestors(parents mobject_trty) 
        RETURN level_ancestor_tty,
    STATIC FUNCTION calculate_inherited_levels(parent_onames names_tty,
                                               top_level VARCHAR2,
                                               delta_level_hierarchy level_hierarchy_tty) RETURN level_hierarchy_tty,
    MEMBER PROCEDURE init_attribute_table(table_name VARCHAR2)
) NOT FINAL NOT INSTANTIABLE;
/

CREATE OR REPLACE TYPE mobject_tty AS TABLE OF mobject_ty;
/

/*CREATE OR REPLACE TYPE alias_ty AS OBJECT(local_name VARCHAR2,
                                          alias_name VARCHAR2);
/

CREATE OR REPLACE TYPE alias_tty AS TABLE OF alias_ty;
/*/

CREATE OR REPLACE TYPE dimension_ty AS OBJECT (
    -- identifiers
    dname          VARCHAR2(30),
    id             VARCHAR2(10), -- unique surrogate key
    
    -- cache for efficiency
    level_hierarchy level_hierarchy_tty,
    level_positions  level_position_tty,
    
    -- names of dynamically created entities
    dimension_#_ty VARCHAR2(30), -- name of the specialization of dimension_ty
    mobject_#_ty   VARCHAR2(30), -- name of the specialization of mobject_ty
    mobject_table  VARCHAR2(30), -- name of the m-object table
    mobject_id_seq VARCHAR2(30), -- name of the package that returns surrogate
                                 -- keys for the m-objects
    primary_key_idx    VARCHAR2(30),
    unique_id_idx      VARCHAR2(30),
    ancestors_lvl_idx  VARCHAR2(30),
    ancestors_pk_idx   VARCHAR2(30),
    attributes_lvl_idx VARCHAR2(30),
    attributes_tab_idx VARCHAR2(30),
    hierarchy_idx      VARCHAR2(30),
    metadata_idx       VARCHAR2(30),
        
    -- administration flags
    enforce_consistency INTEGER, -- should consistency be enforced?
    enable_hierarchy_cache INTEGER, -- is the dimension's global level-hierarchy cached?
    enable_position_cache INTEGER,  -- are the positions of the levels in the hierarchy cached?
    
    -- DDL/DML methods
    MEMBER FUNCTION create_mobject(SELF            IN OUT NOCOPY dimension_ty,
                                   oname           VARCHAR2, 
                                   id              VARCHAR2,
                                   top_level       VARCHAR2,
                                   parents         mobject_trty,
                                   level_hierarchy level_hierarchy_tty)
        RETURN REF mobject_ty,
    MEMBER FUNCTION create_mobject(SELF            IN OUT NOCOPY dimension_ty,
                                   oname           VARCHAR2, 
                                   top_level       VARCHAR2,
                                   parents         mobject_trty,
                                   level_hierarchy level_hierarchy_tty)
        RETURN REF mobject_ty,
    MEMBER PROCEDURE insert_mobject(oname           VARCHAR2,
                                    top_level       VARCHAR2,
                                    parents         mobject_trty,
                                    level_hierarchy level_hierarchy_tty),
    MEMBER PROCEDURE insert_mobject(oname           VARCHAR2,
                                    id              VARCHAR2,
                                    top_level       VARCHAR2,
                                    parents         mobject_trty,
                                    level_hierarchy level_hierarchy_tty),
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE bulk_create_mobject(onames          names_tty, 
                                         top_level       VARCHAR2,
                                         parents         mobject_trty,
                                         level_hierarchy level_hierarchy_tty),
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE bulk_set_attribute(attribute_name   VARCHAR2,
                                        attribute_values mobject_value_tty),
      
      -- TO BE OVERRIDDEN!
    MEMBER FUNCTION get_attribute_description(attribute_name VARCHAR2)
        RETURN attribute_ty,             
    
    -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE export_star(table_name VARCHAR2),
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE persist,
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE delete_dimension,
    
    -- query methods
    MEMBER FUNCTION get_mobject_ref(oname VARCHAR2) RETURN REF mobject_ty,
    
    -- export methods
    
    -- cache methods
      -- TO BE OVERRIDDEN!
    MEMBER FUNCTION get_level_position(level_name VARCHAR2) RETURN INTEGER,
    PRAGMA RESTRICT_REFERENCES(get_level_position, WNDS),
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE refresh_level_hierarchy,
      -- TO BE OVERRIDDEN!
    MEMBER FUNCTION calculate_level_hierarchy RETURN level_hierarchy_tty,
    PRAGMA RESTRICT_REFERENCES(calculate_level_hierarchy, WNDS),
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE reload_cache,
    
    MEMBER PROCEDURE refresh_level_positions,
    
    -- consistency checks
    MEMBER PROCEDURE set_enforce_consistency(enforce_consistency BOOLEAN),
    MEMBER PROCEDURE set_enforce_consistency(enforce_consistency INTEGER),
    
    MEMBER FUNCTION unique_attribute_induction(attribute_name VARCHAR2)
        RETURN BOOLEAN,
    MEMBER FUNCTION unique_level_induction(lvl VARCHAR2)
        RETURN BOOLEAN,
    
    -- inexes / cache
    MEMBER PROCEDURE set_enable_hierarchy_cache(enable_hierarchy_cache BOOLEAN),
    MEMBER PROCEDURE set_enable_hierarchy_cache(enable_hierarchy_cache INTEGER),
    MEMBER PROCEDURE set_enable_position_cache(enable_position_cache BOOLEAN),
    MEMBER PROCEDURE set_enable_position_cache(enable_position_cache INTEGER),
    
    MEMBER PROCEDURE drop_indexes,
    MEMBER PROCEDURE rebuild_indexes,
    MEMBER PROCEDURE drop_constraints,
    MEMBER PROCEDURE rebuild_constraints
) NOT FINAL NOT INSTANTIABLE;
/

CREATE OR REPLACE PACKAGE collections AS
    FUNCTION contains(haystack names_tty, 
                      needle   VARCHAR2) RETURN INTEGER;
    FUNCTION contains_ignore_case(haystack names_tty,
                                  needle VARCHAR2) RETURN INTEGER;
    
    PRAGMA RESTRICT_REFERENCES(contains, WNDS);
    PRAGMA RESTRICT_REFERENCES(contains_ignore_case, WNDS);
END;
/

CREATE OR REPLACE PACKAGE level_hierarchies AS
    FUNCTION is_sublevel_of(sub_level VARCHAR2, 
                            parent_level VARCHAR2,
                            level_hierarchy level_hierarchy_tty,
                            transitive BOOLEAN) RETURN BOOLEAN;
    PRAGMA RESTRICT_REFERENCES(is_sublevel_of, WNDS);
                            
    -- same as first is_sublevel_of, but transitive is default true.
    FUNCTION is_sublevel_of(sub_level VARCHAR2, 
                            parent_level VARCHAR2,
                            level_hierarchy level_hierarchy_tty) RETURN BOOLEAN;
    PRAGMA RESTRICT_REFERENCES(is_sublevel_of, WNDS);
   
    FUNCTION is_sublevel_of(sub_level VARCHAR2, 
                            parent_level VARCHAR2,
                            level_hierarchy level_hierarchy_tty,
                            transitive INTEGER) RETURN INTEGER;
    PRAGMA RESTRICT_REFERENCES(is_sublevel_of, WNDS);
    
    FUNCTION get_parent_levels(lvl VARCHAR2, 
                               level_hierarchy level_hierarchy_tty) RETURN names_tty;
                               
    FUNCTION contains_level(lvl VARCHAR2,
                            level_hierarchy level_hierarchy_tty) RETURN BOOLEAN;
    
    FUNCTION sort_level_hierarchy(level_hierarchy level_hierarchy_tty) RETURN level_hierarchy_tty;
    PRAGMA RESTRICT_REFERENCES(sort_level_hierarchy, WNDS);
    
    FUNCTION merge_level_hierarchies(level_hierarchy1 level_hierarchy_tty,
                                     level_hierarchy2 level_hierarchy_tty) 
             RETURN level_hierarchy_tty;
             
    FUNCTION get_level_positions(level_hierarchy level_hierarchy_tty) 
             RETURN level_position_tty;
    
    PROCEDURE normalize(level_hierarchy IN OUT level_hierarchy_tty);
    PRAGMA RESTRICT_REFERENCES(normalize, WNDS);
    
    FUNCTION has_sublevels(lvl VARCHAR2,
                           level_hierarchy level_hierarchy_tty) RETURN BOOLEAN;
                           
    FUNCTION clear_path(lvl VARCHAR2,
                        level_hierarchy level_hierarchy_tty) RETURN level_hierarchy_tty;
END;
/

CREATE OR REPLACE PACKAGE data_types AS
    FUNCTION to_string(data_type   VARCHAR2,
                       data_length INTEGER,
                       data_scale  VARCHAR2) RETURN VARCHAR2;
END;
/

CREATE OR REPLACE PACKAGE attribute_collections AS
    PROCEDURE append_attribute_list(head IN OUT attribute_tty,
                                    tail attribute_tty);
    FUNCTION get_attributes_by_table(table_name VARCHAR2,
                                     lvl VARCHAR2) RETURN attribute_tty;
    PROCEDURE get_attributes_by_table(table_name VARCHAR2,
                                      lvl VARCHAR2,
                                      attribute_list IN OUT attribute_tty);
    FUNCTION get_attribute_by_table(attribute_name VARCHAR2, 
                                    table_name VARCHAR2,
                                    lvl VARCHAR2) RETURN attribute_ty;
END;
/

CREATE OR REPLACE PACKAGE consistent_mobj_concretization AS
    FUNCTION top_level_consistency(top_level VARCHAR2,
                                   parents mobject_trty) RETURN BOOLEAN;
    FUNCTION level_containment(top_level VARCHAR2,
                               level_hierarchy level_hierarchy_tty,
                               parents mobject_trty) RETURN BOOLEAN;
    FUNCTION level_order_compatibility(top_level VARCHAR2,
                                       level_hierarchy level_hierarchy_tty,
                                       parents mobject_trty) RETURN BOOLEAN;
    FUNCTION level_order_locality(level_hierarchy level_hierarchy_tty,
                                  parents mobject_trty) RETURN BOOLEAN;
END;
/

--------------------------------------------------------------------------------
--                           AUXILIARY PACKAGES                               --
--------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE BODY collections IS
    FUNCTION contains(haystack names_tty, needle VARCHAR2) RETURN INTEGER IS
        cnt INTEGER;
    BEGIN
        SELECT COUNT(*) INTO cnt
        FROM   TABLE(haystack) h
        WHERE  VALUE(h) = needle;
        
        RETURN cnt;
    END;
    
    FUNCTION contains_ignore_case(haystack names_tty, needle VARCHAR2) RETURN INTEGER IS
        cnt INTEGER;
    BEGIN
        SELECT COUNT(*) INTO cnt
        FROM   TABLE(haystack) h
        WHERE  UPPER(VALUE(h)) = UPPER(needle);
        
        RETURN cnt;
    END;
END;
/

/**
 * The levelhierarchies package contains functions to manipulate and examine
 * the level-hierarchy of a particular m-object. It is used to check if one
 * level is the sub-level of another level, if the level-hierarchy contains a
 * particular level or to calculate all parent-levels of a particular level.
 */
CREATE OR REPLACE PACKAGE BODY level_hierarchies IS
    FUNCTION is_sublevel_of(sub_level VARCHAR2, 
                            parent_level VARCHAR2,
                            level_hierarchy level_hierarchy_tty,
                            transitive BOOLEAN)
        RETURN BOOLEAN IS
        
        parent_lvl VARCHAR2(30) := parent_level;
        
        cnt INTEGER;
    BEGIN
        IF transitive THEN
            -- transitive sub-levels are considered
            SELECT COUNT(*) INTO cnt       
            FROM   TABLE(level_hierarchy) h    -- take the given hierarchy.
                
            WHERE  h.lvl = sub_level           -- the (transitive) sub-level.
                
            START WITH                         -- the (transitive) parent-level.
                       ((h.parent_level IS NULL AND parent_lvl IS NULL) OR
                        h.parent_level = parent_lvl)
            CONNECT BY PRIOR h.lvl = h.parent_level;
        ELSE
            -- only direct sub-levels are considered
            SELECT COUNT(*) INTO cnt
            FROM   TABLE(level_hierarchy) h     -- take the given hierarchy.
            WHERE  h.lvl = sub_level AND        -- the (direct) sub-level.
                                                -- the (direct) parent-level.
                   ((h.parent_level IS NULL AND parent_lvl IS NULL) OR
                    h.parent_level = parent_lvl); 
        END IF;
        
        RETURN cnt > 0;
    END;
    
    FUNCTION is_sublevel_of(sub_level VARCHAR2, 
                            parent_level VARCHAR2,
                            level_hierarchy level_hierarchy_tty)
        RETURN BOOLEAN IS
    
    BEGIN
        RETURN is_sublevel_of(sub_level,
                              parent_level,
                              level_hierarchy,
                              TRUE);
    END;
    
    FUNCTION is_sublevel_of(sub_level VARCHAR2, 
                            parent_level VARCHAR2,
                            level_hierarchy level_hierarchy_tty,
                            transitive INTEGER)
        RETURN INTEGER IS
        
        parent_lvl VARCHAR2(30) := parent_level;
        
        cnt INTEGER;
    BEGIN
        IF transitive > 0 THEN
            -- transitive sub-levels are considered
            SELECT COUNT(*) INTO cnt       
            FROM   TABLE(level_hierarchy) h    -- take the given hierarchy.
                
            WHERE  h.lvl = sub_level           -- the (transitive) sub-level.
                
            START WITH                         -- the (transitive) parent-level.
                       ((h.parent_level IS NULL AND parent_lvl IS NULL) OR
                        h.parent_level = parent_lvl)
            CONNECT BY PRIOR h.lvl = h.parent_level;
        ELSE
            -- only direct sub-levels are considered
            SELECT COUNT(*) INTO cnt
            FROM   TABLE(level_hierarchy) h     -- take the given hierarchy.
            WHERE  h.lvl = sub_level AND        -- the (direct) sub-level.
                                                -- the (direct) parent-level.
                   ((h.parent_level IS NULL AND parent_lvl IS NULL) OR
                    h.parent_level = parent_lvl); 
        END IF;
        
        RETURN cnt;
    END;
    
    FUNCTION get_parent_levels(lvl VARCHAR2,
                               level_hierarchy level_hierarchy_tty)
        RETURN names_tty IS
        
        parent_levels names_tty;
        searched_level VARCHAR2(30) := lvl;
    BEGIN
        SELECT h.parent_level BULK COLLECT INTO parent_levels
        FROM   TABLE(level_hierarchy) h
        WHERE  h.lvl = searched_level;
        
        RETURN parent_levels;
    END;
    
    FUNCTION contains_level(lvl VARCHAR2, level_hierarchy level_hierarchy_tty)
        RETURN BOOLEAN IS
        
        cnt INTEGER;
        searched_level VARCHAR2(30) := lvl;
    BEGIN
        SELECT COUNT(*) INTO cnt
        FROM   TABLE(level_hierarchy) h
        WHERE  h.lvl = searched_level;
        
        RETURN cnt > 0;
    END;
    
    FUNCTION sort_level_hierarchy(level_hierarchy level_hierarchy_tty) RETURN level_hierarchy_tty IS
        TYPE integer_array IS TABLE OF INTEGER;
        
        sorted_hierarchy level_hierarchy_tty;
        
        lvls names_tty;
        parent_levels names_tty;
        pos integer_array;
        
        i INTEGER;
    BEGIN
        -- order the pairs
        SELECT DISTINCT h.lvl, h.parent_level, LEVEL BULK COLLECT INTO lvls, parent_levels, pos
        FROM   TABLE(level_hierarchy) h                
        START WITH h.parent_level IS NULL OR
                   h.parent_level IN (SELECT x.parent_level
                                      FROM   TABLE(level_hierarchy) x 
                                      WHERE  NOT EXISTS (SELECT y.lvl FROM TABLE(level_hierarchy) y WHERE y.lvl = x.parent_level))
        CONNECT BY PRIOR h.lvl = h.parent_level
        ORDER BY LEVEL;
        
        -- create a level hierarchy
        sorted_hierarchy := level_hierarchy_tty();
        i := lvls.FIRST;
        WHILE i IS NOT NULL LOOP
            sorted_hierarchy.EXTEND;
            sorted_hierarchy(sorted_hierarchy.LAST) :=
                level_hierarchy_ty(lvls(i), parent_levels(i));
            
            --dbms_output.put_line(lvls(i) || ' ' || parent_levels(i) || ' ' || pos(i));
            
            i := lvls.NEXT(i);
        END LOOP;
        
        RETURN sorted_hierarchy;
    END;
    
    /**
     * Two level-hierarchies can be merged by this function. A new level-
     * hierarchy is returned that contains all level <-> parent-level
     * relationships that can be deducted from the simple union of the two
     * hierarchies. Double pairs are not included. If a pair is stated
     * explicitly in one hierarchy but can also be deducted transitively, then
     * it is not included.
     * @param level_hierarchy1 the first level-hierarchy.
     * @param level_hierarchy2 the second level-hierarchy.
     * @return the merged level-hierarchy.
     */
    FUNCTION merge_level_hierarchies(level_hierarchy1 level_hierarchy_tty,
                                     level_hierarchy2 level_hierarchy_tty)
             RETURN level_hierarchy_tty IS
        
        merged_hierarchy level_hierarchy_tty;
    BEGIN
        IF level_hierarchy1 IS NULL THEN
            merged_hierarchy := level_hierarchy2;
        ELSIF level_hierarchy2 IS NULL THEN
            merged_hierarchy := level_hierarchy1;
        ELSE        
            merged_hierarchy := level_hierarchy_tty();
            
            -- retrieve all levels that are unique to one of the two level-
            -- hierarchies.
            DECLARE
                lvl VARCHAR2(30);
                parent_level VARCHAR2(30);
                
                -- retrieve all level <-> parent-level pairs that are exclusive to 
                -- one of the two argument level-hierarchies. These have to be
                -- included in the result level-hierarchy.
                CURSOR hierarchy_cursor IS
                    (SELECT * 
                     FROM TABLE(level_hierarchy1) h1 
                     WHERE level_hierarchies.is_sublevel_of(h1.lvl, 
                                                            h1.parent_level, 
                                                            level_hierarchy2, 
                                                            1)                <= 0) 
                       UNION 
                    (SELECT * 
                     FROM TABLE(level_hierarchy2) h2
                     WHERE level_hierarchies.is_sublevel_of(h2.lvl, 
                                                            h2.parent_level, 
                                                            level_hierarchy1, 
                                                            1)                <= 0);
            BEGIN
                OPEN hierarchy_cursor;
                
                -- construct a new level-hierarchy
                LOOP
                    FETCH hierarchy_cursor INTO lvl, parent_level;
                    EXIT WHEN hierarchy_cursor%NOTFOUND;
                    
                    merged_hierarchy.EXTEND;
                    merged_hierarchy(merged_hierarchy.LAST) := 
                        level_hierarchy_ty(lvl, parent_level);
                END LOOP;
                
                CLOSE hierarchy_cursor;
            END;
            
            -- now add the pairs from the first level-hierarchy if they can not
            -- already be deduced from the merged level-hierarchy.
            DECLARE
                lvl VARCHAR2(30);
                parent_level VARCHAR2(30);
                
                -- retrieve all level <-> parent-level pairs that are not
                -- explicitly contained or can be deduced from the previously
                -- constructed level-hierarchy.
                CURSOR hierarchy_cursor IS
                    SELECT h.lvl, h.parent_level
                    FROM   TABLE(level_hierarchy1) h
                    WHERE  level_hierarchies.is_sublevel_of(h.lvl, 
                                                            h.parent_level, 
                                                            merged_hierarchy, 
                                                            1)                 <= 0;
            BEGIN
                OPEN hierarchy_cursor;
                
                -- add to the new (merged) level-hierarchy
                LOOP
                    FETCH hierarchy_cursor INTO lvl, parent_level;
                    EXIT WHEN hierarchy_cursor%NOTFOUND;
                    
                    merged_hierarchy.EXTEND;
                    merged_hierarchy(merged_hierarchy.LAST) := 
                        level_hierarchy_ty(lvl, parent_level);
                END LOOP;
                
                CLOSE hierarchy_cursor;
            END;
            
            -- now add the pairs from the second level-hierarchy if they can not
            -- already be deduced from the merged level-hierarchy.
            DECLARE
                lvl VARCHAR2(30);
                parent_level VARCHAR2(30);
                
                -- retrieve all level <-> parent-level pairs that are not
                -- explicitly contained or can be deduced from the previously
                -- constructed level-hierarchy.
                CURSOR hierarchy_cursor IS
                    SELECT h.lvl, h.parent_level
                    FROM   TABLE(level_hierarchy2) h
                    WHERE  level_hierarchies.is_sublevel_of(h.lvl, 
                                                            h.parent_level, 
                                                            merged_hierarchy, 
                                                            1)                 <= 0;
            BEGIN
                OPEN hierarchy_cursor;
                
                -- add to the new (merged) level-hierarchy
                LOOP
                    FETCH hierarchy_cursor INTO lvl, parent_level;
                    EXIT WHEN hierarchy_cursor%NOTFOUND;
                    
                    merged_hierarchy.EXTEND;
                    merged_hierarchy(merged_hierarchy.LAST) := 
                        level_hierarchy_ty(lvl, parent_level);
                END LOOP;
                
                CLOSE hierarchy_cursor;
            END;
        END IF;
        
        -- return the merged hierarchy, but sort it beforehand
        RETURN merged_hierarchy;
    END;
    
    FUNCTION get_level_positions(level_hierarchy level_hierarchy_tty)
                RETURN level_position_tty IS
    
        CURSOR hierarchy_cursor IS
            SELECT DISTINCT h.lvl, MAX(LEVEL)
            FROM   TABLE(level_hierarchy) h                
            START WITH h.parent_level IS NULL
            CONNECT BY PRIOR h.lvl = h.parent_level
            GROUP BY h.lvl;
        
        lvl VARCHAR2(30);
        pos INTEGER;
        
        level_positions level_position_tty;
    BEGIN
        OPEN hierarchy_cursor;
        
        -- completely reset the cache
        level_positions := level_position_tty();
        
        LOOP
            FETCH hierarchy_cursor INTO lvl, pos;
            EXIT WHEN hierarchy_cursor%NOTFOUND;
            
            -- add a new pair
            level_positions.EXTEND;
            level_positions(level_positions.LAST) := 
                level_position_ty(lvl, pos);
        END LOOP;
        
        CLOSE hierarchy_cursor;
        
        RETURN level_positions;
    END;
    
    PROCEDURE normalize(level_hierarchy IN OUT level_hierarchy_tty) IS
        i INTEGER;
        
        FUNCTION is_redundant(ir_pair_index INTEGER) RETURN BOOLEAN IS
            ir_pair level_hierarchy_ty;
            
            ir_redundant BOOLEAN;
        BEGIN
            IF level_hierarchy IS NOT NULL THEN
                ir_pair := level_hierarchy(ir_pair_index);
                
                -- temporarily remove this pair
                level_hierarchy(ir_pair_index).lvl := NULL;
                level_hierarchy(ir_pair_index).parent_level := NULL;
                
                ir_redundant := level_hierarchies.is_sublevel_of(ir_pair.lvl,
                                                                 ir_pair.parent_level,
                                                                 level_hierarchy);
                
                -- restore the pair
                level_hierarchy(ir_pair_index).lvl := ir_pair.lvl;
                level_hierarchy(ir_pair_index).parent_level := ir_pair.parent_level;
            END IF;
            
            RETURN ir_redundant;
        END;
    BEGIN
        i := level_hierarchy.FIRST;
        WHILE i IS NOT NULL LOOP
            IF is_redundant(i) THEN
                level_hierarchy.DELETE(i);
            END IF;
            
            i := level_hierarchy.NEXT(i);
        END LOOP;
    END;
    
    FUNCTION has_sublevels(lvl VARCHAR2,
                           level_hierarchy level_hierarchy_tty) 
             RETURN BOOLEAN IS
        cnt INTEGER;
        
        searched_level VARCHAR2(30) := lvl;
    BEGIN
        SELECT COUNT(*) INTO cnt
        FROM   TABLE(level_hierarchy) h
        WHERE  h.parent_level = searched_level;
        
        RETURN cnt > 0;
    END;
    
    -- this function deletes all levels from the level_hierarchy iff they are
    -- ONLY a sublevel of lvl.
    FUNCTION clear_path(lvl VARCHAR2,
                        level_hierarchy level_hierarchy_tty) 
             RETURN level_hierarchy_tty IS
        
        new_hierarchy level_hierarchy_tty;
        
        clear_level VARCHAR2(30) := lvl;
        
        -- this cursor retrieves all pairs from the level-hierarchy where the
        -- level or the parent-level component is not the level whose path is
        -- to be cleared AND where the level is not a sub-level of the to be
        -- cleared level. However, a level is included if it is a (transitive)
        -- sub-level of a level that is included.
        CURSOR hierarchy_cursor IS
            SELECT h.lvl, h.parent_level
            FROM   TABLE(level_hierarchy) h
            WHERE  NOT h.lvl = clear_level AND
                   NOT h.parent_level = clear_level AND
                   (level_hierarchies.is_sublevel_of(h.lvl, 
                                                     clear_level, 
                                                     level_hierarchy, 1) <= 0 OR
                    EXISTS (SELECT h1.parent_level
                            FROM   TABLE(level_hierarchy) h1
                            WHERE  NOT h1.parent_level = clear_level AND
                                   level_hierarchies.is_sublevel_of(h.lvl, 
                                                                    h1.parent_level, 
                                                                    level_hierarchy, 1) > 0));
        
        some_level VARCHAR2(30);
        parent_level VARCHAR2(30);
    BEGIN
        new_hierarchy := level_hierarchy_tty();
        
        OPEN hierarchy_cursor;
        
        LOOP
            FETCH hierarchy_cursor INTO some_level, parent_level;
            EXIT WHEN hierarchy_cursor%NOTFOUND;
            
            new_hierarchy.EXTEND;
            new_hierarchy(new_hierarchy.LAST) := 
                level_hierarchy_ty(some_level, parent_level);
        END LOOP;
        
        CLOSE hierarchy_cursor;
        
        RETURN new_hierarchy;
    END;
    
END;
/

CREATE OR REPLACE PACKAGE BODY data_types AS
    FUNCTION to_string(data_type   VARCHAR2,
                       data_length INTEGER,
                       data_scale  VARCHAR2) RETURN VARCHAR2 IS
    
        string_value VARCHAR2(100);
    BEGIN
        string_value := data_type;
        
        IF data_length IS NOT NULL THEN
            string_value := string_value || '(' || data_length;
            
            IF data_scale IS NOT NULL THEN
                string_value := string_value || ', ' || data_scale;
            END IF;
            
            string_value := string_value || ')';
        END IF;
        
        RETURN string_value;
    END;
END;
/

CREATE OR REPLACE PACKAGE BODY attribute_collections AS
    PROCEDURE append_attribute_list(head IN OUT attribute_tty,
                                    tail attribute_tty) IS
        i INTEGER;
    BEGIN
        IF head IS NULL THEN
            head := tail;
        ELSE
            -- append the tail to the head collection
            IF tail IS NOT NULL THEN
                i := tail.FIRST;
                WHILE i IS NOT NULL LOOP
                    head.EXTEND;
                    head(head.LAST) := tail(i);
                    
                    i := tail.NEXT(i);
                END LOOP;
            END IF;
        END IF;
    END;
    
    FUNCTION get_attributes_by_table(table_name VARCHAR2,
                                     lvl VARCHAR2) RETURN attribute_tty IS
        attribute_list attribute_tty;
    BEGIN
        attribute_list := attribute_tty();
        
        -- invoke alternative signature that fills the argument list.
        get_attributes_by_table(table_name, lvl, attribute_list);
        
        RETURN attribute_list;
    END;
    
    PROCEDURE get_attributes_by_table(table_name VARCHAR2,
                                      lvl VARCHAR2,
                                      attribute_list IN OUT attribute_tty) IS
        -- has to be done for the query
        table_name1 VARCHAR2(30) := table_name;
        
        CURSOR utc_cursor IS 
            SELECT utc.column_name, utc.data_type, utc.data_length, utc.data_scale 
            FROM   user_tab_columns utc 
            WHERE  utc.table_name = UPPER(table_name1) AND 
                   utc.column_name <> 'ONAME' AND
                   utc.column_name <> 'OBJ';
          
        attribute_name VARCHAR2(40);
        data_type VARCHAR2(40); 
        data_length NUMBER; 
        data_scale NUMBER;
    BEGIN                  
        OPEN utc_cursor; 
        
        LOOP 
            FETCH utc_cursor INTO attribute_name, 
                                  data_type, 
                                  data_length, 
                                  data_scale; 
            
            EXIT WHEN utc_cursor%NOTFOUND; 
             
            attribute_list.extend; 
            attribute_list(attribute_list.LAST) := 
                attribute_ty(UPPER(attribute_name), 
                             lvl, 
                             UPPER(table_name), 
                             data_type,
                             data_length,
                             data_scale);
        END LOOP;
       
        CLOSE utc_cursor;
    END;
    
    FUNCTION get_attribute_by_table(attribute_name VARCHAR2, 
                                    table_name VARCHAR2,
                                    lvl VARCHAR2) 
                        RETURN attribute_ty IS 
        
        table_name1 VARCHAR2(30) := table_name;
        
        CURSOR utc_cursor IS 
            SELECT utc.data_type, utc.data_length, utc.data_scale 
            FROM   user_tab_columns utc 
            WHERE  utc.table_name = UPPER(table_name1) AND
                   utc.column_name = UPPER(attribute_name);
        
        attribute_descr attribute_ty;
         
        data_type VARCHAR2(40); 
        data_length INTEGER; 
        data_scale INTEGER;
    BEGIN                  
        OPEN utc_cursor; 
            
        FETCH utc_cursor INTO data_type, 
                              data_length,
                              data_scale; 
        
        IF utc_cursor%FOUND THEN
            attribute_descr := attribute_ty(UPPER(attribute_name), 
                                            lvl, 
                                            UPPER(table_name), 
                                            data_type, 
                                            data_length, 
                                            data_scale);
        END IF; 
        
        CLOSE utc_cursor;
                 
        RETURN attribute_descr;
    END;
END;
/

--------------------------------------------------------------------------------
--                               MOBJECT_TY                                   --
--------------------------------------------------------------------------------

CREATE OR REPLACE TYPE BODY mobject_ty AS
    /**
     * This member function can be used to order the m-objects according to
     * their top-level. The level position of the m-object's top-level is
     * returned. The level position is an integer number reflecting the
     * top-level's position within the dimension's global level-hierarchy.
     *
     * Example:
     *
     *       top          --> position: 1
     *     /      \
     * country  region    --> position: 2
     *     \      /
     *       city         --> position: 3
     *
     *
     * The level position of each level is cached with the dimension and
     * updated whenever a new m-object introducing a new level is introduced.
     * It is thus faster to use this function than the compare_to function that
     * always computes the results.
     *
     * NOTE: This is not the default method that is used to order m-objects.
     *       Reasonable and valid results are only returned if the level-
     *       hierarchy and position caches are enabled. Otherwise, use the
     *       ORDER MEMBER FUNCTION to order m-objects within SQL.
     */
      -- TO BE OVERWRITTEN!
    MEMBER FUNCTION top_level_position RETURN INTEGER IS
        dim dimension_ty;
    BEGIN
        -- retrieve the dimension
        utl_ref.select_object(SELF.dim, dim);
        
        -- determine the position of SELF's top-level in the global
        -- level-hierarchy.
        RETURN dim.get_level_position(SELF.top_level);
    END;
    
    /**
     * This version of the compare_to function makes use of the 
     * top_level_position function and takes advantage of the cached
     * level positions in the dimension. It is an order method based on the
     * object type's map method.
     * 
     * NOTE: This function's output may differ from the compare_to function's
     *       output in certain cases. However, this is only the case for two
     *       m-objects whose order is not really defined in the compare_to 
     *       function.
     * 
     */
    MEMBER FUNCTION compare_to#map(other mobject_ty) RETURN INTEGER IS
    BEGIN
        RETURN SELF.top_level_position - 
               other.top_level_position;
    END;
    
    /**
     * This function compares two m-objects according to the relative order of
     * their top-levels in the dimension's global level-hierarchy.
     *
     * NOTE: Two m-objects of different dimensions can be compared by this
     *       function. However, the result may not be reasonable!
     */
    ORDER MEMBER FUNCTION compare_to(other mobject_ty) RETURN INTEGER IS
        dim dimension_ty;
        
        level_hierarchy level_hierarchy_tty;
        
        return_value INTEGER;
    BEGIN
        -- NOTE: Two m-objects of different dimensions can be compared by this
        -- function. However, the result may not be reasonable!
        
        IF SELF.oname = other.oname THEN
            return_value := 0;
        ELSE
            -- get the dimension to find out the level_hierarchy
            utl_ref.select_object(SELF.dim, dim);
            
            -- check if we can use the cached level-hierarchy or if we have
            -- to calculate the hierarchy here.
            IF dim.enable_hierarchy_cache > 0 THEN
                level_hierarchy := dim.level_hierarchy;
            ELSE
                level_hierarchy := dim.calculate_level_hierarchy();
            END IF;
            
            -- if the other m-object's top-level is a sublevel of SELF's top-
            -- level in the dimension's global level-hierarchy then return -1.
            IF level_hierarchies.is_sublevel_of(other.top_level, 
                                                SELF.top_level,
                                                level_hierarchy) THEN
                return_value := -1;
            
            -- if the other m-object's top-level is an ancestor level of SELF's 
            -- toplvl then return 1.
            ELSIF level_hierarchies.is_sublevel_of(SELF.top_level,
                                                   other.top_level,
                                                   level_hierarchy) THEN
                return_value := 1;
            
            -- otherwise, 0 is returned (actually, order doesn't really matter
            -- in this case). NOTE: differs from the #map version in this case.
            ELSE
                return_value := 0;
            END IF;
        END IF;
        
        RETURN return_value;
    END;
    
    /*
     *
     *
     */
    MEMBER FUNCTION is_descendant_of(other_oname VARCHAR2) RETURN INTEGER IS        
        cnt INTEGER;
    BEGIN
        SELECT COUNT(*) INTO cnt
        FROM   TABLE(SELF.ancestors) p
        WHERE  p.ancestor.oname = other_oname;
        
        RETURN cnt;
    END;
    
    MEMBER FUNCTION is_descendant_of(other REF mobject_ty) RETURN INTEGER IS        
        cnt INTEGER;
    BEGIN
        SELECT COUNT(*) INTO cnt
        FROM   TABLE(SELF.ancestors) p
        WHERE  p.ancestor = other;
        
        RETURN cnt;
    END;
    
    MEMBER FUNCTION calculate_ancestors RETURN level_ancestor_tty IS
    BEGIN
        RETURN mobject_ty.calculate_ancestors(SELF.parents);
    END;
    
    /**
     *
     *
     */
    STATIC FUNCTION calculate_ancestors(parents mobject_trty) 
               RETURN level_ancestor_tty IS        
        parent_obj mobject_ty;
        
        i INTEGER;
        lvl VARCHAR2(30);
        ancestor REF mobject_ty;
        
        ancestors level_ancestor_tty;
    BEGIN
        -- only proceed if the parents are not null
        IF (parents IS NOT NULL) THEN
            -- create the ancestors collection
            ancestors := level_ancestor_tty();
            
            i := parents.FIRST;
            WHILE i IS NOT NULL LOOP
                -- get the parent object
                utl_ref.select_object(parents(i), parent_obj);
                
                -- add the ancestors of the parent
                DECLARE
                    -- only include ancestors that have not already been added
                    -- to the ancestors collection.
                    CURSOR ancestor_cursor IS 
                        SELECT r.lvl, r.ancestor
                        FROM   TABLE(parent_obj.ancestors) r
                        WHERE  NOT EXISTS(SELECT s.lvl
                                          FROM   TABLE(ancestors) s
                                          WHERE  s.lvl = r.lvl);
                BEGIN
                    OPEN ancestor_cursor;
                    
                    LOOP
                        FETCH ancestor_cursor INTO lvl, ancestor;
                        EXIT WHEN ancestor_cursor%NOTFOUND;
                        
                        ancestors.EXTEND;
                        ancestors(ancestors.LAST) := 
                            level_ancestor_ty(lvl, ancestor);
                    END LOOP;
                    
                    CLOSE ancestor_cursor;
                END;
                
                -- add the parent to the ancestors
                ancestors.EXTEND;
                ancestors(ancestors.LAST) := 
                    level_ancestor_ty(parent_obj.top_level, parents(i));
                
                i := parents.NEXT(i);
            END LOOP;
        END IF;
        
        RETURN ancestors;
    END;
    
    STATIC FUNCTION calculate_inherited_levels(parent_onames names_tty,
                                               top_level VARCHAR2,
                                               delta_level_hierarchy level_hierarchy_tty) RETURN level_hierarchy_tty IS
    
    BEGIN
        RETURN NULL;
    END;
    
    MEMBER PROCEDURE assert_consistency IS
    
    BEGIN
        mobject_ty.assert_consistency(SELF.top_level,
                                      SELF.level_hierarchy,
                                      SELF.parents,
                                      SELF.dim);
    END;
    
    STATIC PROCEDURE assert_consistency(top_level       VARCHAR2,
                                        level_hierarchy level_hierarchy_tty,
                                        parents         mobject_trty,
                                        dim_ref         REF dimension_ty) IS
        dim dimension_ty;
        parent_obj mobject_ty;
        
        parent_levels names_tty;
        new_parent_levels names_tty;
        new_levels names_tty;
        
        err error_ty;
        
        i INTEGER;
        j INTEGER;
    BEGIN
        IF NOT consistent_mobj_concretization.top_level_consistency(top_level, parents) THEN
            SELECT VALUE(e) INTO err
            FROM   errors e
            WHERE  e.error_name = 'consistent_mobject_concretization_top_level_consistency';
               
            err.raise_error;
        END IF;
           
        -- check if all levels of the parents that are under the new m-object's top-level
        -- are contained in the new m-object
        IF NOT consistent_mobj_concretization.level_containment(top_level, level_hierarchy, parents) THEN
            SELECT VALUE(e) INTO err
            FROM   errors e
            WHERE  e.error_name = 'consistent_mobject_concretization_level_containment';
               
            err.raise_error;
        END IF;
           
        -- check if the relative order of the levels has not changed
        IF NOT consistent_mobj_concretization.level_order_compatibility(top_level, level_hierarchy, parents) THEN
            SELECT VALUE(e) INTO err
            FROM   errors e
            WHERE  e.error_name = 'consistent_mobject_concretization_level_order_compatibility';
            
            err.raise_error;
        END IF;
        
        -- check if all newly introduced levels have parents only within SELF.level_hierarchy
        IF NOT consistent_mobj_concretization.level_order_locality(level_hierarchy, parents) THEN
            SELECT VALUE(e) INTO err
            FROM   errors e
            WHERE  e.error_name = 'consistent_mobject_concretization_level_order_locality';
            
            err.raise_error;
        END IF;
        
        -- check if the newly introduced levels have not been inducted elsewhere (unique induction rule for levels)
        utl_ref.select_object(dim_ref, dim);
        
        ---- 1.) get the newly introduced levels
        IF parents IS NOT NULL THEN
            -- get all levels from all parents
            i := parents.FIRST;
            WHILE i IS NOT NULL LOOP
                utl_ref.select_object(parents(i), parent_obj);
                
                SELECT DISTINCT lvl BULK COLLECT INTO new_parent_levels
                FROM   (SELECT DISTINCT x.lvl AS lvl
                        FROM   TABLE(parent_obj.level_hierarchy) x)
                       UNION
                       (SELECT n.column_value AS lvl
                        FROM   TABLE(parent_levels) n);
                
                parent_levels := new_parent_levels;
                
                i := parents.NEXT(i);
            END LOOP;
                
            -- get the actual new levels
            SELECT DISTINCT lvl BULK COLLECT INTO new_levels
            FROM   TABLE(level_hierarchy) h
            WHERE  h.lvl NOT IN (SELECT n.column_value AS lvl FROM TABLE(parent_levels) n);
        END IF;
                
        ---- 2.) check for each newly introduced level if the unique induction 
        ----     rule is satisfied.
        IF new_levels IS NOT NULL THEN
            j := new_levels.FIRST;
            WHILE j IS NOT NULL LOOP
                IF NOT dim.unique_level_induction(new_levels(j)) THEN
                    SELECT VALUE(e) INTO err
                    FROM   errors e
                    WHERE  e.error_name = 'consistent_dimension_unique_level_induction';
                
                    err.raise_error;
                END IF;
                        
                j := new_levels.NEXT(j);
            END LOOP;
        END IF;
        
        -- TODO: add further consistency checks.
    END;
    
    /**
     * MUST BE OVERRIDDEN!
     */
    MEMBER FUNCTION get_attribute_table(lvl VARCHAR2) RETURN VARCHAR2 IS
    
    BEGIN
        RETURN NULL;
    END;
    
    /**
     *
     *
     */
    MEMBER PROCEDURE add_attribute(attribute_name     VARCHAR2,
                                   attribute_level    VARCHAR2,
                                   attribute_datatype VARCHAR2) IS
        table_name VARCHAR2(90);
        table_name1 VARCHAR2(30);
        
        attribute_typecode VARCHAR2(100);
        nested_table_name  VARCHAR2(90);
        nested_table_store VARCHAR2(1000);
        
        row_count INTEGER;
        
        i NUMBER := 1;
        found BOOLEAN := FALSE;
        
        dim dimension_ty;
        
        err error_ty;
    BEGIN
        -- get the dimension object
        utl_ref.select_object(SELF.dim, dim);
        
        -- check for consistency if the dimension tells us so
        IF dim.enforce_consistency > 0 THEN
            -- check if the indicated level is in the level hierarchy.
            IF NOT SELF.has_level(attribute_level) THEN
                SELECT VALUE(e) INTO err
                FROM   errors e
                WHERE  e.error_name = 'consistent_mobject_attribute_level_not_exists';
                
                err.raise_error;
            END IF;
            
            -- check if the attribute complies with the unique induction rule
            IF NOT dim.unique_attribute_induction(attribute_name) THEN
                -- get the corresponding error
                SELECT VALUE(e) INTO err
                FROM   errors e
                WHERE  e.error_name = 'consistent_dimension_unique_attribute_induction';
                
                -- throw error message
                err.raise_error;
            END IF;
                        
            -- TODO: perhaps add further consistency checks
            
        END IF;
                
        -- check if there already is a table for the level of the newly
        -- added attribute that can be used to store the attribute.
        table_name := SELF.get_attribute_table(attribute_level);
        
        -- if the table does not exist, create the table
        IF table_name IS NULL THEN
            table_name := dim.id || '_' || 
                          SELF.id || '_' || 
                          attribute_level;
           
            -- if the table name is too long, get a unique short name
            IF LENGTH(table_name) > 30 THEN                
                -- get a unique table name that is at most 30 bytes long.
                table_name := 
                    identifiers.get_unique_short_name(30,
                                                      UPPER(table_name),
                                                      'user_tab_columns',
                                                      'table_name');
            END IF;
            
            -- create the table               
            EXECUTE IMMEDIATE
                'CREATE TABLE ' || table_name || '(' || chr(10) ||
                '    oname VARCHAR2(40) PRIMARY KEY REFERENCES ' || 
                 dim.mobject_table || '(oname) ON DELETE CASCADE,' || chr(10) ||
                '    obj REF ' || dim.mobject_#_ty || ' REFERENCES ' || 
                 dim.mobject_table || ' ON DELETE CASCADE ' || chr(10) ||
                ')';
               
            SELF.attribute_tables.extend;
            SELF.attribute_tables(SELF.attribute_tables.LAST) :=
                                  attribute_table_ty(attribute_level, 
                                                     UPPER(table_name));
        END IF;
        
        -- this is done for the query which would not work otherwise
        table_name1 := table_name;
             
        SELECT COUNT(*) INTO row_count 
        FROM   user_tab_columns utc
        WHERE  utc.table_name = UPPER(table_name1) AND 
               utc.column_name = UPPER(attribute_name);
        
        IF (row_count = 0) THEN
            -- check if data type is a user-defined type
            SELECT COUNT(t.typecode) INTO row_count
            FROM   user_types t 
            WHERE  t.type_name = UPPER(attribute_datatype);
            
            -- check if data type is a user-defined type
            IF row_count > 0 THEN
                SELECT t.typecode INTO attribute_typecode
                FROM   user_types t 
                WHERE  t.type_name = UPPER(attribute_datatype);
                
                IF attribute_typecode = 'COLLECTION' THEN
                    nested_table_name := dim.id || '_' || 
                                         SELF.id || '_' || 
                                         attribute_level || '_' ||
                                         attribute_name;
               
                    -- if the table name is too long, get a unique short name
                    IF LENGTH(nested_table_name) > 30 THEN                
                        -- get a unique table name that is at most 30 bytes long.
                        nested_table_name := 
                            identifiers.get_unique_short_name(30,
                                                              UPPER(nested_table_name),
                                                              'all_tables',
                                                              'table_name');
                    END IF;
                    
                    nested_table_store := ' NESTED TABLE ' || attribute_name || ' STORE AS ' || nested_table_name;
                    
                    dbms_output.put_line(nested_table_store);
                END IF;
            END IF;
            
            EXECUTE IMMEDIATE
                'ALTER TABLE ' || table_name || 
                ' ADD (' || attribute_name || ' ' || attribute_datatype || ')' ||
                nested_table_store;            
        END IF;
        
        -- an m-object which adds an attribute specializes the schema
        SELF.specializes := 1;
        
        -- save the changes
        SELF.persist;
    END;
    
    MEMBER PROCEDURE set_attribute(attribute_name VARCHAR2,
                                   metalevel VARCHAR2,
                                   default_value BOOLEAN,
                                   attribute_value ANYDATA) IS
        i INTEGER;
        j INTEGER;
        found BOOLEAN;
        
        dim dimension_ty;
        
        ancestor mobject_ty;
        
        default_val NUMBER;
        
        attr_description attribute_ty;
        
        attr_lvl VARCHAR2(40);
        
        err error_ty;
    BEGIN
        IF (metalevel IS NULL AND NOT default_value) THEN
            set_attribute(attribute_name, attribute_value);
        ELSE
            -- get the dimension object
            utl_ref.select_object(SELF.dim, dim);
            
            -- check for consistency if the dimension tells us so
            IF dim.enforce_consistency > 0 THEN              
                -- TODO: add consistency checks
                NULL;
            END IF;
            
            IF has_attribute(attribute_name, FALSE, FALSE, attr_description) THEN
                attr_lvl := attr_description.attribute_level;
                
                IF default_value THEN
                    default_val := 1;
                ELSE
                    default_val := 0;
                END IF;
                
                -- find out if there already is a tuple
                i := SELF.attribute_metadata.FIRST;
                found := FALSE;
                WHILE i IS NOT NULL AND NOT found LOOP
                    IF(SELF.attribute_metadata(i).attribute_level = attr_lvl AND
                       SELF.attribute_metadata(i).attribute_name = attribute_name AND
                       ((SELF.attribute_metadata(i).metalevel IS NULL AND
                         metalevel IS NULL) OR
                        SELF.attribute_metadata(i).metalevel = metalevel) AND                       
                       SELF.attribute_metadata(i).default_value = default_val) THEN
                        found := TRUE;
                    END IF;
                    
                    IF NOT found THEN
                        i := SELF.attribute_metadata.NEXT(i);
                    END IF;
                END LOOP;
                
                IF NOT found THEN
                    SELF.attribute_metadata.extend;
                    SELF.attribute_metadata(SELF.attribute_metadata.last) :=
                        attribute_meta_ty(attribute_name, attr_lvl, metalevel, default_val, attribute_value);
                ELSE
                    SELF.attribute_metadata(i).attribute_value := attribute_value;
                END IF;
                
                SELF.persist;
            END IF;
        END IF;
    END;
    
    MEMBER PROCEDURE set_attribute(attribute_name VARCHAR2,
                                   attribute_value ANYDATA) IS
        sql_cursor INTEGER;
        rows_processed INTEGER;
        
        attribute_descr attribute_ty;
        table_name VARCHAR2(30);
        
        data_type VARCHAR2(30);
        
        status INTEGER;
        long_string_tty_value long_string_tty;
        
        dim dimension_ty;
        
        i INTEGER;
        found BOOLEAN := FALSE;
    BEGIN
        -- if there is nothing to set, don't.
        IF attribute_value IS NOT NULL THEN
            found := has_attribute(attribute_name, 
                                   TRUE, FALSE, 
                                   attribute_descr);
            
            -- get the dimension object
            utl_ref.select_object(SELF.dim, dim);
            
            -- check for consistency if the dimension tells us so
            IF dim.enforce_consistency > 0 THEN              
                -- TODO: add consistency checks
                NULL;
            END IF;
            
            IF found THEN
                table_name := attribute_descr.table_name;
                data_type := attribute_descr.data_type;
                
                -- add a tuple to the attribute table
                init_attribute_table(table_name);
                
                sql_cursor := dbms_sql.open_cursor;
                
                dbms_sql.parse(sql_cursor,
                               'UPDATE ' || table_name || chr(10) ||
                               'SET ' || attribute_name || '= :attribute_value' || chr(10) ||
                               'WHERE oname = :self_oname',
                               dbms_sql.native);
                
                dbms_sql.bind_variable(sql_cursor, 
                                       'self_oname', 
                                       SELF.oname);
                
                -- TODO: Throw error message when passed argument is of wrong type.
                
                dbms_output.put_line(data_type);
                CASE data_type        
                    WHEN 'VARCHAR2' THEN
                        dbms_sql.bind_variable(sql_cursor, 
                                               'attribute_value', 
                                               attribute_value.accessVarchar2);
                    WHEN 'NUMBER' THEN
                        dbms_sql.bind_variable(sql_cursor, 
                                               'attribute_value', 
                                               attribute_value.accessNumber);
                    WHEN 'LONG_STRING_TTY' THEN
                        status := attribute_value.getCollection(long_string_tty_value);
                        
                        dbms_sql.bind_variable(sql_cursor, 
                                               'attribute_value', 
                                               long_string_tty_value);
                END CASE;
                
                rows_processed := dbms_sql.execute(sql_cursor);
                
                dbms_sql.close_cursor(sql_cursor);
                
                SELF.persist;
            END IF;
        END IF;
    END;
        
    MEMBER PROCEDURE init_attribute_table(table_name VARCHAR2) IS
        row_count INTEGER;
        
        -- need the dimension to get the name of the dynamic object type
        dim dimension_ty;
    BEGIN
        -- get the dimension
        utl_ref.select_object(SELF.dim, dim);
        
        -- check if there already is an entry for SELF in the specified table
        EXECUTE IMMEDIATE 
            'SELECT COUNT(*)' || chr(10) ||
            'FROM   ' || table_name || ' a' || chr(10) ||
            'WHERE  a.oname = :1'
            INTO  row_count
            USING SELF.oname;
        
        -- if there is no entry yet, insert a new entry
        IF row_count = 0 THEN            
            EXECUTE IMMEDIATE
                'INSERT INTO ' || table_name || '(oname, obj) SELECT o.oname, REF(o) FROM ' || dim.mobject_table || ' o WHERE o.oname = :1'
                USING SELF.oname;
        END IF;
    END;
    
    MEMBER FUNCTION get_attribute(attribute_name VARCHAR2) RETURN ANYDATA IS
        dim dimension_ty;
        
        sql_cursor INTEGER;
        rows_processed INTEGER;
        
        attribute_descr attribute_ty;
        table_name1 VARCHAR2(80);
        
        default_values_query VARCHAR2(1000);
        
        data_type VARCHAR2(40);
        
        --
        value_varchar2        VARCHAR2(1000);
        value_number          NUMBER;
        value_long_string_tty long_string_tty;
        --
        
        found BOOLEAN := FALSE;
        
        return_value ANYDATA;
    BEGIN
        -- determine if the attribute exists at the object's top level
        found := has_attribute(attribute_name, TRUE, FALSE, attribute_descr);
        
        IF found THEN
            table_name1 := attribute_descr.table_name;
            
            -- determine the data_type of the attribute
            data_type := attribute_descr.data_type;
            /*
            SELECT utc.data_type INTO data_type 
            FROM user_tab_columns utc
            WHERE utc.table_name = UPPER(table_name1) AND utc.column_name = UPPER(attribute_name);
            */
                
            sql_cursor := dbms_sql.open_cursor;
            
            dbms_sql.parse(sql_cursor,
                           'SELECT a.' || attribute_name || chr(10) ||
                           'FROM   ' || table_name1 || ' a' || chr(10) ||
                           'WHERE  a.oname=''' || SELF.oname || '''',
                           dbms_sql.native);
            
            IF (data_type = 'VARCHAR2') THEN         
                dbms_sql.define_column(sql_cursor, 1, value_varchar2, 1000);
            ELSIF (data_type = 'NUMBER') THEN         
                dbms_sql.define_column(sql_cursor, 1, value_number);
            ELSIF (data_type = 'LONG_STRING_TTY') THEN
                dbms_sql.define_column(sql_cursor, 1, value_long_string_tty);
            END IF;
            
            rows_processed := dbms_sql.execute(sql_cursor);
            
            rows_processed := dbms_sql.fetch_rows(sql_cursor);
            
            IF rows_processed > 0 THEN
                IF (data_type = 'VARCHAR2') THEN
                    dbms_sql.column_value(sql_cursor, 1, value_varchar2);
                    return_value := anydata.convertVarchar2(value_varchar2);
                ELSIF (data_type = 'NUMBER') THEN         
                    dbms_sql.column_value(sql_cursor, 1, value_number);
                    return_value := anydata.convertNumber(value_number);
                ELSIF (data_type = 'LONG_STRING_TTY') THEN
                    dbms_sql.column_value(sql_cursor, 1, value_long_string_tty);
                    return_value := anydata.convertCollection(value_long_string_tty);
                END IF;
            ELSE
                -- check for default values
                utl_ref.select_object(SELF.dim, dim);
                
                default_values_query :=
                    'SELECT x.val' || chr(10) ||
                    'FROM   TABLE(SELECT d.level_positions FROM dimensions d WHERE d.dname = :dname) p,' || chr(10) ||
                    '       (SELECT o1.top_level AS lvl, ANYDATA.access' || data_type ||'(m.attribute_value) AS val' || chr(10) ||
                    '        FROM   ' || dim.mobject_table || ' o1, TABLE(o1.attribute_metadata) m' || chr(10) ||
                    '        WHERE  m.attribute_name = :attribute_name AND' || chr(10) ||
                    '               m.metalevel IS NULL AND' || chr(10) ||
                    '               REF(o1) IN (SELECT ancestor FROM TABLE(:ancestors))) x' || chr(10) ||
                    'WHERE  p.lvl = x.lvl AND' || chr(10) ||
                    '       p.position = ' || chr(10) ||
                    '           (SELECT MAX(p.position) AS max_position' || chr(10) ||
                    '            FROM   TABLE(SELECT d.level_positions FROM dimensions d WHERE d.dname = :dname) p,' || chr(10) ||
                    '                   (SELECT o1.top_level AS lvl' || chr(10) ||
                    '                    FROM   ' || dim.mobject_table || ' o1, TABLE(o1.attribute_metadata) m' || chr(10) ||
                    '                    WHERE  m.attribute_name = :attribute_name AND' || chr(10) ||
                    '                           m.metalevel IS NULL AND' || chr(10) ||
                    '                           REF(o1) IN (SELECT ancestor FROM TABLE(:ancestors))) z' || chr(10) ||
                    '            WHERE  p.lvl = z.lvl)';
                                 
                dbms_sql.parse(sql_cursor,
                               default_values_query,
                               dbms_sql.native);
                
                dbms_sql.bind_variable(sql_cursor, 'attribute_name', attribute_name);
                dbms_sql.bind_variable(sql_cursor, 'dname', dim.dname);
                dbms_sql.bind_variable(sql_cursor, 'ancestors', SELF.ancestors);
                
                IF (data_type = 'VARCHAR2') THEN         
                    dbms_sql.define_column(sql_cursor, 1, value_varchar2, 100);
                ELSIF (data_type = 'NUMBER') THEN         
                    dbms_sql.define_column(sql_cursor, 1, value_number);
                END IF;
                            
                rows_processed := dbms_sql.execute(sql_cursor);
                rows_processed := dbms_sql.fetch_rows(sql_cursor);
                
                IF rows_processed > 0 THEN
                    IF (data_type = 'VARCHAR2') THEN
                        dbms_sql.column_value(sql_cursor, 1, value_varchar2);
                        return_value := anydata.convertVarchar2(value_varchar2);
                    ELSIF (data_type = 'NUMBER') THEN         
                        dbms_sql.column_value(sql_cursor, 1, value_number);
                        return_value := anydata.convertNumber(value_number);
                    ELSIF (data_type = 'LONG_STRING_TTY') THEN         
                        dbms_sql.column_value(sql_cursor, 1, value_long_string_tty);
                        return_value := anydata.convertCollection(value_long_string_tty);
                    END IF;
                END IF;
            END IF;
            
            dbms_sql.close_cursor(sql_cursor);
        END IF;
        
        RETURN return_value;
    END;
    
    MEMBER FUNCTION has_level(lvl VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        RETURN level_hierarchies.contains_level(lvl, SELF.level_hierarchy);
    END;
    
    /**
     * This function is for the use in SQL statements. It uses no BOOLEAN
     * data type in the signature.
     */
    MEMBER FUNCTION has_attribute(attribute_name VARCHAR2,
                                  top_level_only INTEGER,
                                  introduced_only INTEGER) RETURN INTEGER IS
        descr attribute_ty;
    BEGIN
        IF SELF.has_attribute(attribute_name, top_level_only > 0, introduced_only > 0, descr) THEN
            RETURN 1;
        ELSE
            RETURN 0;
        END IF;
    END;
                                  
    /**
     * Determines if this m-object has a particular (transitive) attribute. If top_level_only
     * is true, only attributes which have been introduced for the m-object's toplvl
     * are considered. 
     */
    MEMBER FUNCTION has_attribute(attribute_name VARCHAR2,
                                  top_level_only BOOLEAN,
                                  introduced_only BOOLEAN,
                                  description OUT attribute_ty) RETURN BOOLEAN IS
        i INTEGER;
        j INTEGER;
        found BOOLEAN := FALSE;
        
        row_count INTEGER;
        
        table_name1 VARCHAR2(30);
        
        parent_obj mobject_ty;
    BEGIN
        -- search this m-object's attribute_tables for attributes
        i := SELF.attribute_tables.FIRST;
        WHILE i IS NOT NULL AND NOT found LOOP
            -- 
            IF NOT top_level_only OR 
               SELF.attribute_tables(i).lvl = SELF.top_level THEN
                table_name1 := SELF.attribute_tables(i).table_name;
                
                SELECT COUNT(*) INTO row_count 
                FROM user_tab_columns utc
                WHERE utc.table_name = UPPER(table_name1) AND 
                      utc.column_name = UPPER(attribute_name);
                
                IF (row_count > 0) THEN
                    found := TRUE;
                    
                    -- get the attribute description
                    description := 
                        attribute_collections.get_attribute_by_table(attribute_name,
                                                                    table_name1,
                                                                    SELF.attribute_tables(i).lvl);
                END IF;
            END IF;
            
            i := SELF.attribute_tables.NEXT(i);
        END LOOP;
        
        -- search for attributes introduced by ancestor m-objects.
        IF NOT introduced_only AND SELF.ancestors IS NOT NULL THEN
            j := SELF.ancestors.FIRST;
            WHILE j IS NOT NULL AND NOT found LOOP
                -- get the ancestor object
                utl_ref.select_object(SELF.ancestors(j).ancestor, parent_obj);
                    
                i := parent_obj.attribute_tables.FIRST;
                WHILE i IS NOT NULL AND NOT found LOOP 
                    IF parent_obj.attribute_tables(i).lvl = SELF.top_level OR 
                       (NOT top_level_only AND SELF.has_level(parent_obj.attribute_tables(i).lvl)) THEN
                        table_name1 := parent_obj.attribute_tables(i).table_name;
                        
                        SELECT COUNT(*) INTO row_count
                        FROM user_tab_columns utc
                        WHERE utc.table_name = UPPER(table_name1) AND 
                              utc.column_name = UPPER(attribute_name);
                        
                        IF (row_count > 0) THEN
                            found := TRUE;
                            
                            -- get the attribute description
                            description := 
                                attribute_collections.get_attribute_by_table(attribute_name,
                                                                            table_name1,
                                                                            parent_obj.attribute_tables(i).lvl);
                        END IF;
                    END IF;    
                    i :=  parent_obj.attribute_tables.NEXT(i);
                END LOOP; 
                j := SELF.ancestors.NEXT(j);
            END LOOP;
        END IF;
        
        IF (NOT found) THEN
            description := NULL;
        END IF;
        
        RETURN found;
    END;
    
    MEMBER FUNCTION list_attributes(top_level_only INTEGER,
                                    introduced_only INTEGER) 
           RETURN attribute_tty IS
    BEGIN
        RETURN list_attributes(top_level_only > 0, introduced_only > 0);
    END;
    
    -- if introduced_only is set, only attributes are included that have been
    -- introduced by this m-object (SELF).
    MEMBER FUNCTION list_attributes(top_level_only BOOLEAN,
                                    introduced_only BOOLEAN) 
           RETURN attribute_tty IS
        i INTEGER;
        j INTEGER;
        
        attribute_list attribute_tty := attribute_tty();
        
        parent_obj mobject_ty;
    BEGIN
        -- search this m-object's attribute_tables for attributes
        i := SELF.attribute_tables.FIRST;
        WHILE i IS NOT NULL LOOP
            IF SELF.attribute_tables(i).lvl = SELF.top_level OR NOT top_level_only THEN
                -- call the procedure to get the measures introduced by
                -- the object referenced by SELF.
                attribute_collections.get_attributes_by_table(SELF.attribute_tables(i).table_name,
                                                             SELF.attribute_tables(i).lvl,
                                                             attribute_list);
            END IF;
            
            i := SELF.attribute_tables.NEXT(i);
        END LOOP;
        
        -- search for attributes introduced by ancestor m-objects.
        IF NOT introduced_only AND SELF.ancestors IS NOT NULL THEN
            j := SELF.ancestors.FIRST;
            WHILE j IS NOT NULL LOOP
                -- get the ancestor object
                utl_ref.select_object(SELF.ancestors(j).ancestor, parent_obj);
                    
                i := parent_obj.attribute_tables.FIRST;
                WHILE i IS NOT NULL LOOP
                    IF parent_obj.attribute_tables(i).lvl = SELF.top_level OR 
                       (NOT top_level_only AND 
                        SELF.has_level(parent_obj.attribute_tables(i).lvl)) THEN
                        -- call the procedure to get the measures introduced
                        -- by the currently visited parent m-object.
                        attribute_collections.get_attributes_by_table(parent_obj.attribute_tables(i).table_name,
                                                                     parent_obj.attribute_tables(i).lvl,
                                                                     attribute_list);
                    END IF;  
                    
                    i := parent_obj.attribute_tables.NEXT(i);
                END LOOP;
                
                j := SELF.ancestors.NEXT(j);
            END LOOP;
        END IF;
        
        RETURN attribute_list;
    END;
    
    -- TO BE OVERRIDDEN!
    MEMBER FUNCTION get_descendants RETURN mobject_trty IS
    BEGIN
        RETURN NULL;
    END;
    
    -- TO BE OVERRIDDEN!
    MEMBER FUNCTION get_descendants_onames RETURN names_tty IS
    BEGIN
        RETURN NULL;
    END;
    
    -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE delete_mobject IS
    BEGIN
        NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE persist IS
    BEGIN
        NULL;
    END;
    
      -- TO BE OVERRIDDEN!
    MEMBER PROCEDURE delete_attribute_metadata(attribute_name VARCHAR2) IS
    BEGIN
        NULL;
    END;
    
    MEMBER PROCEDURE delete_attribute(attribute_name VARCHAR2) IS
        i INTEGER;
        
        found BOOLEAN;
        
        row_count INTEGER;
        
        table_name1 VARCHAR2(30);
    BEGIN
        -- loop through the attribute tables to check which table contains
        -- the given attribute.
        IF SELF.attribute_tables IS NOT NULL THEN
            found := FALSE;
            i := SELF.attribute_tables.FIRST;
            WHILE NOT found AND i IS NOT NULL LOOP
                table_name1 := SELF.attribute_tables(i).table_name;
                
                SELECT COUNT(*) INTO row_count 
                FROM user_tab_columns utc
                WHERE utc.table_name = UPPER(table_name1) AND 
                      utc.column_name = UPPER(attribute_name);
                
                found := row_count > 0;
                
                i := SELF.attribute_tables.NEXT(i);
            END LOOP;
        END IF;
                
        IF found THEN
            -- delete the attribute metadata
            SELF.delete_attribute_metadata(attribute_name);
            
            -- check if the column in the table still exists
            SELECT COUNT(*) INTO row_count 
            FROM   user_tab_columns utc
            WHERE  utc.table_name = UPPER(table_name1) AND 
                   utc.column_name = UPPER(attribute_name);
            
            -- drop the column if the column exists
            IF (row_count > 0) THEN
                EXECUTE IMMEDIATE 
                    'ALTER TABLE ' || table_name1 || 
                    ' DROP COLUMN ' || attribute_name;
            END IF;
            
            -- check if there are any columns left except the oname and the obj
            SELECT COUNT(*) INTO row_count
            FROM   user_tab_columns utc
            WHERE  utc.table_name = UPPER(table_name1) AND
                   utc.column_name <> 'ONAME' AND
                   utc.column_name <> 'OBJ';
            
            -- if there are no attributes left in this attribute table, the
            -- table can be dropped
            IF row_count = 0 THEN
                EXECUTE IMMEDIATE
                    'DROP TABLE ' || table_name1;
                
                found := FALSE;
                i := SELF.attribute_tables.FIRST;
                WHILE NOT found AND i IS NOT NULL LOOP
                    found := SELF.attribute_tables(i).table_name = table_name1;
                    
                    IF found THEN
                        SELF.attribute_tables.DELETE(i);
                    END IF;
                                        
                    i := SELF.attribute_tables.NEXT(i);
                END LOOP;
            END IF;
        END IF;
        
        SELF.persist;
    END;
    
    MEMBER FUNCTION introduced_level(lvl VARCHAR2) RETURN BOOLEAN IS
        introduced BOOLEAN := FALSE;
        found BOOLEAN := FALSE;
        
        anc_obj mobject_ty;
        
        i INTEGER;
    BEGIN
        -- do not return true if the m-object does not have the level in question
        IF SELF.has_level(lvl) THEN
            -- look if an ancestor m-object also has the same level
            i := SELF.ancestors.FIRST;
            WHILE NOT found AND i IS NOT NULL LOOP
                utl_ref.select_object(SELF.ancestors(i).ancestor, anc_obj);
                
                found := anc_obj.has_level(lvl);
                
                i := SELF.ancestors.NEXT(i);
            END LOOP;
            
            -- if the level was not found, this m-object introduces the level
            introduced := NOT found;
        END IF;
        
        RETURN introduced;
    END;
    
    MEMBER FUNCTION does_specialize RETURN BOOLEAN IS
        specializes BOOLEAN := FALSE;
        
        i INTEGER;
    BEGIN
        specializes := (SELF.parents IS NULL OR SELF.parents.COUNT = 0);
        
        IF NOT specializes THEN
            i := SELF.level_hierarchy.FIRST;
            WHILE i IS NOT NULL AND NOT specializes LOOP
                specializes := SELF.introduced_level(SELF.level_hierarchy(i).lvl);
                
                i := SELF.level_hierarchy.NEXT(i);
            END LOOP;
            
            IF NOT specializes THEN
                specializes := (SELF.attribute_tables IS NOT NULL AND
                                SELF.attribute_tables.COUNT > 0);
            END IF;
        END IF;
        
        RETURN specializes;
    END;
END;
/


--------------------------------------------------------------------------------
--                           CONSISTENCY CHECKS                               --
--------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY consistent_mobj_concretization IS
    FUNCTION top_level_consistency(top_level     VARCHAR2,
                                   parents mobject_trty) RETURN BOOLEAN IS
        i INTEGER;
        parent_obj mobject_ty;
        
        check_passed BOOLEAN;
    BEGIN
        check_passed := TRUE;
        
        -- only check if there are parents. If there are no parents, check is
        -- passed.
        IF parents IS NOT NULL THEN
            -- the top-level must pass the check for all parents
            i := parents.FIRST;
            WHILE check_passed AND i IS NOT NULL LOOP
                IF parents(i) IS NOT NULL THEN
                    -- get the parent m-object
                    utl_ref.select_object(parents(i), parent_obj);
                    
                    -- the check for this parent is passed if the new m-object's
                    -- top-level is a DIRECT sub-level of the parent's top-level in
                    -- the parent's level-hierarchy.
                    check_passed :=
                        level_hierarchies.is_sublevel_of(top_level,
                                                         parent_obj.top_level,
                                                         parent_obj.level_hierarchy,
                                                         FALSE);
                END IF;
                
                i := parents.NEXT(i);
            END LOOP;
        END IF;
        
        RETURN check_passed;
    END;
    
    FUNCTION level_containment(top_level VARCHAR2,
                               level_hierarchy level_hierarchy_tty,
                               parents mobject_trty)
            RETURN BOOLEAN IS
        
        i INTEGER;
        j INTEGER;
        parent_obj mobject_ty;
        
        check_passed BOOLEAN;
        
        level_list names_tty;
    BEGIN
        check_passed := TRUE;
        
        IF parents IS NOT NULL THEN
            i := parents.FIRST;
            WHILE check_passed AND i IS NOT NULL LOOP
                IF parents(i) IS NOT NULL THEN
                    -- get the parent object
                    utl_ref.select_object(parents(i), parent_obj);
                    
                    IF parent_obj.level_hierarchy IS NOT NULL THEN
                        -- get the levels of the level_hierarchy that are a
                        -- sub-level of the new m-object's top-level in the
                        -- parent's level-hierarchy.
                        SELECT h.lvl BULK COLLECT INTO level_list
                        FROM   TABLE(parent_obj.level_hierarchy) h
                        WHERE  level_hierarchies.is_sublevel_of(h.lvl,
                                                                top_level,
                                                                parent_obj.level_hierarchy,
                                                                1) > 0;
                        
                        -- check if all the levels are contained in the new
                        -- m-object's level-hierarchy.
                        j := level_list.FIRST;
                        WHILE check_passed AND j IS NOT NULL LOOP
                            -- is the current level contained in the new
                            -- m-object's level-hierarchy?
                            check_passed :=
                                level_hierarchies.contains_level(level_list(j),
                                                                 level_hierarchy);
                            
                            j := level_list.NEXT(j);
                        END LOOP;
                    END IF;
                END IF;
                
                i := parents.NEXT(i);
            END LOOP;
        END IF;
        
        RETURN check_passed;
    END;
    
    FUNCTION level_order_compatibility(top_level VARCHAR2,
                                       level_hierarchy level_hierarchy_tty,
                                       parents mobject_trty) RETURN BOOLEAN IS
        
        parent_obj mobject_ty;
        
        i INTEGER;
        j INTEGER;
        check_passed BOOLEAN;
    BEGIN
        check_passed := TRUE;
        
        IF parents IS NOT NULL THEN
            -- loop through the parents
            i := parents.FIRST;
            WHILE check_passed AND i IS NOT NULL LOOP
                -- get the parent object
                utl_ref.select_object(parents(i), parent_obj);
                
                IF parent_obj.level_hierarchy IS NOT NULL THEN
                    -- check for each pair in the parent's level-hierarchy 
                    j := parent_obj.level_hierarchy.FIRST;
                    WHILE check_passed AND j IS NOT NULL LOOP
                        -- consider only levels under the new m-object's top-level AND
                        -- consider only pairs where the top-level is in the concretizing
                        -- m-object's level hierarchy.
                        -- pairs for the top-level are considered anyway.
                        IF (level_hierarchies.is_sublevel_of(parent_obj.level_hierarchy(j).lvl,
                                                             top_level,
                                                             parent_obj.level_hierarchy) AND
                            level_hierarchies.contains_level(parent_obj.level_hierarchy(j).parent_level,
                                                             level_hierarchy)) OR
                            parent_obj.level_hierarchy(j).lvl = top_level
                           THEN
                            -- each pair in the parent's level-hierarchy is explicitly or
                            -- implicitly given in the new m-object's level-hierarchy.
                            check_passed :=
                                level_hierarchies.is_sublevel_of(parent_obj.level_hierarchy(j).lvl,
                                                                 parent_obj.level_hierarchy(j).parent_level,
                                                                 level_hierarchy);
                        END IF;
                        
                        j := parent_obj.level_hierarchy.NEXT(j);
                    END LOOP;
                END IF;
                
                i := parents.NEXT(i);
            END LOOP;
        END IF;
        
        RETURN check_passed;
    END;
    
    FUNCTION level_order_locality(level_hierarchy level_hierarchy_tty,
                                  parents mobject_trty) RETURN BOOLEAN IS
        
        parent_obj mobject_ty;
        i INTEGER;
        j INTEGER;
        
        check_passed BOOLEAN;
        found BOOLEAN;
    BEGIN
        check_passed := TRUE;
        
        IF level_hierarchy IS NOT NULL AND
           parents IS NOT NULL THEN
            -- loop through each level to check if it is a newly 
            -- introduced level.
            i := level_hierarchy.FIRST;
            WHILE check_passed AND i IS NOT NULL LOOP
                -- loop through the parents to check whether the current level
                -- actually is a newly introduced level.
                j := parents.FIRST;
                found := FALSE;
                WHILE NOT found AND j IS NOT NULL LOOP
                    utl_ref.select_object(parents(j), parent_obj);
                    
                    -- check if the parent has the current level
                    found := parent_obj.has_level(level_hierarchy(i).lvl);
                    
                    j := parents.NEXT(j);
                END LOOP;
                
                -- if the current level is a newly introduced level, check if
                -- the parent-level of the current pair is also contained on
                -- the level side in the new m-object's level-hierarchy.
                IF NOT found THEN
                    check_passed :=
                        level_hierarchies.contains_level(level_hierarchy(i).parent_level,
                                                         level_hierarchy);
                END IF;
                
                i := level_hierarchy.NEXT(i);
            END LOOP;
        END IF;
        
        RETURN check_passed;
    END;
END;
