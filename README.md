# The Hetero-Homogeneous Data Warehouse Project (HH-DW)

Copyright (C) 2010-2015 Department of Business Informatics -- Data & Knowledge Engineering

Johannes Kepler University Linz, Altenberger Str. 69, 4040 Linz, Austria

HH-DW is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

HH-DW is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with HH-DW. If not, see <http://www.gnu.org/licenses/>.

##Installation

###Prerequisites

The HH-DW prototype runs on the Oracle database 11g. Create a separate user for the hetero-homogeneous data warehouse system. Ensure that the user has sufficient privileges for creating tables, views, indexes, sequences, packages, procesdures, and types. The user also needs access privileges on the data dictionary as well as execute privileges for procedures, packages, and types.

###PL/SQL Scripts
Run the PL/SQL scripts supplied in the src/at/jku/dke/hhdw/sys directory in the following order:

1.) error_code.sql

2.) identifiers.sql

3.) mobject.sql

4.) dimension.sql

5.) mcube.sql

###SQL(M) Interpreter
The SQL(M) interpreter is a more intuitive user interface for the hetero-homogeneous data warehouse system. In order to install the interpreter, first run the src/at/jku/dke/hhdw/sqlm/interpreter.sql PL/SQL script under the same user as the core HH-DW system.

##Contributors

Christoph G. Schuetz (Project Leader)

Thomas Pecksteiner
