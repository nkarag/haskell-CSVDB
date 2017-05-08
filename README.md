**CSVDB - A "thin" Database layer over CSV files written in Haskell**
---------

Main Features*
-------------

 - No DB-specific storage (that is why we call it a "thin" database layer). CSV files stay as-is on your disk
 - Relational Table abstraction over CSV files
 - Relational Algebra operations over CSV files -  exposed as haskell functions
 - Perform ETL over your CSV files via the ETL mapping concept, which is exposed as a Haskell data type. 
 - **Any** type of data transformation can be easily implemented through this ETL Mapping construct
 - SQL interface over CSV files
 - REPL with available ETL operations and Relational Algebra Operators and SQL queries
 - Lazy evaluation of data pipelines
 - Composition of ETL operations and Relational Algebra operations
 - Parallel processing of large CSV files
 - Notebook style GUI (e.g., like Apache Zeppelin) over CSV files

-----------
(*) CSVDB is still work in progress.

> Written with [StackEdit](https://stackedit.io/).