/** 
 * The Hetero-Homogeneous Data Warehouse Project (HH-DW)
 * Copyright (C) 2011 Department of Business Informatics -- Data & Knowledge Engineering
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

DROP TYPE error_ty FORCE;

DROP TABLE errors;


--------------------------------------------------------------------------------
--                              TYPE AND TABLE                                --
--------------------------------------------------------------------------------

-- used for error handling (inconsistencies etc.)
CREATE OR REPLACE TYPE error_ty AS OBJECT(
    error_number NUMBER,
    error_name VARCHAR2(400),
    message VARCHAR2(2048),
    MEMBER PROCEDURE raise_error
);
/

CREATE OR REPLACE TYPE BODY error_ty AS 
    MEMBER PROCEDURE raise_error IS
        
    BEGIN
        raise_application_error(SELF.error_number, SELF.message);
    END;
END;
/

-- this table contains all error messages
CREATE TABLE errors OF error_ty (
    error_number PRIMARY KEY, 
    error_name UNIQUE NOT NULL
);

-- consistent m-object error codes
INSERT INTO errors VALUES(-20000, 'consistent_mobject_attribute_not_exists', 'The attribute you tried to set a value for does not exist for this m-object.');
INSERT INTO errors VALUES(-20001, 'consistent_mobject_attribute_level_not_exists', 'The level you tried to add an attribute for does not exist.');
INSERT INTO errors VALUES(-20002, 'consistent_mobject_level_hierarchy_loop', 'The level hierarchy of the m-object you tried to add contains a loop.');

-- consistent concretization of m-objects error codes
INSERT INTO errors VALUES(-20100, 'consistent_mobject_concretization_top_level_consistency', 'The top-level of the concretizing m-object o'' is not a second-top-level of its parent m-object o.');
INSERT INTO errors VALUES(-20101, 'consistent_mobject_concretization_level_containment', 'A level of m-object o is not contained in the level-hierarchy of descendant m-object o''.');
INSERT INTO errors VALUES(-20102, 'consistent_mobject_concretization_level_order_compatibility', 'Level order is incompatible. The relative order of levels is different in m-object o and its descendant m-object o''.');
INSERT INTO errors VALUES(-20103, 'consistent_mobject_concretization_level_order_locality', 'Concretizing m-object o'' of m-object o introduces a level that has a parent-level that is not in the level-hierarchy of m-object o''.');
INSERT INTO errors VALUES(-20104, 'consistent_mobject_concretization_shared_value_overwrite', 'Tried to change an attribute (meta-)value that was not default.');

-- consistent dimension error codes
INSERT INTO errors VALUES(-20200, 'consistent_dimension_unique_attribute_induction', 'Attribute has already been inducted.');
INSERT INTO errors VALUES(-20201, 'consistent_dimension_unique_level_induction', 'Level has already been inducted elsewhere.');

-- consistent m-rel error codes
INSERT INTO errors VALUES(-20300, 'consistent_mrelationship_measure_not_exists', 'The measure you tried to set a value for does not exist for this m-relationship.');
INSERT INTO errors VALUES(-20301, 'consistent_mrelationship_measure_connection_level_not_exists', 'The connection level you tried to add a measure for does not exist.');

-- consistent concretization of m-relationships error codes
INSERT INTO errors VALUES(-20400, 'consistent_mrelationship_concretization_assured_granularity', 'M-relationship moves an already defined measure to a less detailed granularity.');
INSERT INTO errors VALUES(-20401, 'consistent_mrelationship_concretization_measure_type_stability', '');
INSERT INTO errors VALUES(-20402, 'consistent_mrelationship_concretization_aggregation_function_stability', '');
INSERT INTO errors VALUES(-20403, 'consistent_mrelationship_concretization_shared_value_overwrite', 'Tried to change a measure (meta-)value that was not default.');

-- consistent m-cube error codes
INSERT INTO errors VALUES(-20500, 'consistent_mcube_no_root_mrelationship', 'The m-cube lacks a single m-relationship that corresponds to the m-cube''s root coordinate.');
INSERT INTO errors VALUES(-20501, 'consistent_mcube_duplicate_coordinate', 'Tried to insert an m-relationship into a cell where another m-relationship for that coordinate already exists.');
INSERT INTO errors VALUES(-20502, 'consistent_mcube_unique_measure_induction', 'Measure has already been inducted.');
INSERT INTO errors VALUES(-20503, 'consistent_mcube_unique_value_assertion', 'Overlapping subcubes assert values for same measure.');
INSERT INTO errors VALUES(-20505, 'consistent_mcube_mrelationship_not_under_root', 'Tried to insert an m-relationship at a coordinate that is not a proper sub-coordinate of the m-cube''s root-coordinate.');

-- misc
