-- drop existing table
DROP TABLE my_catalog.my_schema.my_table;

-- create temporary table
CREATE TABLE my_catalog.my_schema.temporary_table AS
SELECT * FROM another_catalog.schema_a.table_a

-- create output table
create table my_catalog.my_schema.my_table AS
SELECT a.*, b.name
FROM my_catalog.my_schema.temporary_table AS a
LEFT JOIN another_catalog.schema_b.table_b AS b ON a.id = b.id

-- drop temporary table
drop table my_catalog.my_schema.temporary_table;
