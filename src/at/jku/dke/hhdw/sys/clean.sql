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

-- Run this file to uninstall the prototype system.
-- Note that object types created by user-created data warehouses 
-- (including samples) are not purged.

DROP TYPE error_ty FORCE;

DROP TABLE errors;

DROP TYPE names_tty FORCE;

DROP PACKAGE identifiers;

DROP TABLE dimensions;
DROP TABLE mcubes;

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
DROP TYPE attribute_unit_ty FORCE;
DROP TYPE attribute_unit_tty FORCE;
DROP TYPE measure_unit_ty FORCE;
DROP TYPE measure_unit_tty FORCE;
DROP TYPE attribute_meta_ty FORCE;
DROP TYPE attribute_meta_tty FORCE;
DROP TYPE mobject_value_ty FORCE;
DROP TYPE mobject_value_tty FORCE;
DROP TYPE attribute_ty FORCE;
DROP TYPE attribute_tty FORCE;
DROP TYPE dimension_ty FORCE;

DROP PACKAGE collections;
DROP PACKAGE level_hierarchies;
DROP PACKAGE attribute_collections;
DROP PACKAGE data_types;

DROP PACKAGE consistent_mobj_concretization;

DROP TYPE dimension_trty FORCE;

DROP PACKAGE dimension;

DROP PACKAGE mcube;
DROP PACKAGE mcube_ddl;

DROP TYPE mcube_ty FORCE;
DROP TYPE mrel_ty FORCE;

DROP TYPE slice_predicate_ty FORCE;
DROP TYPE boolean_expression_tty FORCE;
DROP TYPE boolean_expression_ty  FORCE;
