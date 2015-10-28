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

DROP PACKAGE identifiers;

CREATE OR REPLACE PACKAGE identifiers AS
    TYPE key_value_tty IS TABLE OF VARCHAR2(30) INDEX BY VARCHAR2(30);
    FUNCTION get_unique_short_name(name_length    INTEGER,
                                   long_name      VARCHAR2,
                                   table_name     VARCHAR2,
                                   attribute_name VARCHAR2)
        RETURN VARCHAR2;
    FUNCTION get_unique_short_name(name_length      INTEGER,
                                   long_name        VARCHAR2,
                                   table_name       VARCHAR2,
                                   attribute_name   VARCHAR2,
                                   attr_constraints key_value_tty)
        RETURN VARCHAR2;
END;
/

CREATE OR REPLACE PACKAGE BODY identifiers AS    
    FUNCTION get_unique_short_name(name_length    INTEGER,
                                   long_name      VARCHAR2,
                                   table_name     VARCHAR2,
                                   attribute_name VARCHAR2) 
                      RETURN VARCHAR2 IS
        i INTEGER;
        digits INTEGER;
        
        row_count INTEGER;
        
        unique_short_name VARCHAR2(30);
    BEGIN
        -- 0 signals that we start by trying if the long_name as a short_name
        -- is still unique.
        IF LENGTH(long_name) <= name_length THEN
            i := 0;
        ELSE
            i := 1;
        END IF;
        
        LOOP
            IF i > 0 THEN
                -- get the number of digits --> this is needed to determine the
                -- length of the substring that is taken from the long name
                digits := LOG(10, i) + 1;
                
                unique_short_name := SUBSTR(long_name, 1, name_length-digits-1);
                unique_short_name := unique_short_name || '#' || i;
            ELSIF i = 0 THEN
                -- for the first iteration and if the long name is shorter
                -- than the limit, do not add anything or chop something off.
                unique_short_name := long_name;
            END IF;
            
            -- check if the short name is unique
            EXECUTE IMMEDIATE
                'SELECT COUNT(*)' || chr(10) ||
                'FROM   ' || table_name || ' t' || chr(10) ||
                'WHERE  ' || attribute_name || ' = :1'
                INTO  row_count
                USING unique_short_name;
            
            -- exit if the short version name is unique in the table
            EXIT WHEN row_count = 0;
            i := i + 1;
        END LOOP;
        
        RETURN unique_short_name;
    END;
    
    FUNCTION get_unique_short_name(name_length      INTEGER,
                                   long_name        VARCHAR2,
                                   table_name       VARCHAR2,
                                   attribute_name   VARCHAR2,
                                   attr_constraints key_value_tty) 
                      RETURN VARCHAR2 IS
        i INTEGER;
        digits INTEGER;
        
        row_count INTEGER;
        
        unique_short_name VARCHAR2(30);
        
        where_clause VARCHAR2(4000);
        attr VARCHAR2(30);
    BEGIN
        attr := attr_constraints.FIRST;
        WHILE attr IS NOT NULL LOOP
            where_clause := ' AND ' ||
                't.' || attr || ' = ' || attr_constraints(attr);
            
            attr := attr_constraints.NEXT(attr);
        END LOOP;
        
        -- 0 signals that we start by trying if the long_name as a short_name
        -- is still unique.
        IF LENGTH(long_name) <= name_length THEN
            i := 0;
        ELSE
            i := 1;
        END IF;
        
        LOOP
            IF i > 0 THEN
                -- get the number of digits --> this is needed to determine the
                -- length of the substring that is taken from the long name
                digits := LOG(10, i) + 1;
                
                unique_short_name := SUBSTR(long_name, 1, name_length-digits-1);
                unique_short_name := unique_short_name || '#' || i;
            ELSIF i = 0 THEN
                -- for the first iteration and if the long name is shorter
                -- than the limit, do not add anything or chop something off.
                unique_short_name := long_name;
            END IF;
            
            -- check if the short name is unique
            EXECUTE IMMEDIATE
                'SELECT COUNT(*)' || chr(10) ||
                'FROM   ' || table_name || ' t' || chr(10) ||
                'WHERE  ' || attribute_name || ' = :1' || where_clause
                INTO  row_count
                USING unique_short_name;
            
            -- exit if the short version name is unique in the table
            EXIT WHEN row_count = 0;
            i := i + 1;
        END LOOP;
        
        RETURN unique_short_name;
    END;
END;
/
