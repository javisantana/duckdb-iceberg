
INSTALL './build/release/extension/iceberg/iceberg.duckdb_extension';
LOAD iceberg;
SELECT count(*) FROM iceberg_scan('data/iceberg/lineitem_iceberg', allow_moved_paths = true);

--INSERT INTO example_storage_sink() VALUES (1), (2), (3);
--CALL iceberg_attach('data/iceberg/lineitem_iceberg');
ATTACH 's3://test'  as dbi (TYPE ICEBERG);
show tables;
--describe icebeg_test;
--select * from sqlite_master;
--select * from icebeg_test limit 5;
insert into lineitem_iceberg values (1);
