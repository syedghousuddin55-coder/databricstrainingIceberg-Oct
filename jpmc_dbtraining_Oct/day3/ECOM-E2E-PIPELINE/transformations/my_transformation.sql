CREATE STREAMING TABLE sales as
select * from STREAM read_files("/Volumes/dev/bronze/raw/sales/",format=>"csv");

CREATE STREAMING TABLE products as
select * from STREAM read_files("/Volumes/dev/bronze/raw/products/",format=>"csv");

CREATE STREAMING TABLE customers as
select * from STREAM read_files("/Volumes/dev/bronze/raw/customers/",format=>"csv");

