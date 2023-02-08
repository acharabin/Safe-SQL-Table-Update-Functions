# Safe-SQL-Table-Update-Functions
Engine class to initialize connection to Redshift or other PostgreSQL databases &amp; functions to safely update production tables.

## Summary of Approach Used
- An archive schema is used to house the latest version of a production table prior to the current update. If something goes wrong with a production table, it can be immediately overwritten with the respective table in the archive schema. 
- A new copy of a table is made for updates. If the new table updates successfully (results in at least 1 row of data), the production table is dropped and the updating table is instantaneously altered to be become the production table. 
- Processes to update archive and production tables are packaged in functions so they can be replicated consistantly across different production tables. 
- An engine class is created an initialized in order to simplify reading, writing, and archiving in predominantly separate search paths/schemas.