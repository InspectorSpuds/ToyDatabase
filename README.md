# 4300-Cheetah
## Description
DB Relation Manager project for CPSC4300 at Seattle U, Spring 2023, Project Echidna. This is a fork of the original repository I contributed to. 

## New Features
### Milestone 3
Executes `CREATE`, `DROP`, and `SHOW` SQL statements in a Berkeley DB Database. `SHOW` statements can handle displaying tables and columns.

### Milestone 4
Builds off of Milestone 3 to `CREATE`, `DROP`, and `SHOW` indices as well as tables.

### Milestone 5
Evaluation plan added for statements like insert and select, initiali support for transactions added

### Milestone 6
Created a transaction system using stacks and a lock file (through the flock() system call), where on a rollback, 
statements that were added to the rollbackStack (statements that counter the effects of statements made during the transaction)
are executed for some given transaction level

## Installation
1. Clone the repository on CS1

` git clone https://github.com/BguardiaOpen/4300-Echidna23SQ.git `

2. Ensure the ` .bash_profile ` path is configured correctly

```
export PATH=/usr/local/db6/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/db6/lib:$LD_LIBRARY_PATH
export PYTHONPATH=/usr/local/db6/lib/site-packages:$PYTHONPATH 
```

## Usage
1. Create a directory to hold the database (first time usage only)
2. Compile the program with ` make `
3. Run the program with ` ./cpsc4300 path_to_database_directory `
    
    * The path must be the path to the directory from the root user@cs1
4. Other ``` make ``` options
    
    * ` make clean `: removes the object code files
5. User input options

    * SQL `CREATE`, `DROP`, and `SHOW` statements (see example)
    * ` quit ` exits the program

## Example

```
$ ./cpsc4300 cpsc4300/data
SQL> create table foo (a text, b integer)
CREATE TABLE foo (a TEXT, b INT)
SQL> show tables
SHOW TABLES
table_name 
+----------+
"foo" 
SQL> show columns from foo
SHOW COLUMNS FROM foo
table_name column_name data_type 
+----------+----------+----------+
"foo" "a" "TEXT" 
"foo" "b" "INT"
SQL> drop table foo
DROP TABLE foo
SQL> show tables
SHOW TABLES
table_name 
+----------+
SQL> create index fx on foo (a)
CREATE INDEX fx ON foo USING BTREE (a)
created index fx
SQL> show index from foo
SHOW INDEX FROM goober
table_name index_name column_name seq_in_index index_type is_unique 
+----------+----------+----------+----------+----------+----------+
"foo" "fx" "a" 1 "BTREE" true
SQL> not real sql
Invalid SQL: not real sql
SQL> quit
```

## Acknowledgements
* [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html)
* [Berkeley DB Dbt](https://docs.oracle.com/cd/E17076_05/html/api_reference/CXX/frame_main.html)
* [Hyrise SQL Parser](https://github.com/klundeen/sql-parser)
* [Professor Lundeen's 5300-Instructor base code](https://github.com/klundeen/5300-Instructor/releases/tag/Milestone2h) 🙏🙏
* [The Python equivalent](https://github.com/BguardiaOpen/cpsc4300py)
