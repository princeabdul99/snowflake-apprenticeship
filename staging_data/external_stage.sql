-- create a storage integration
-- using Amazon S3

/* Create a Cloud Storage Integration in Snowflake*/
use role ACCOUNTADMIN;

create storage integration BISTRO_INTEGRATION
    type = external_stage
    storage_provider = 'S3'
    enabled = true
    storage_aws_role_arn = '<your-role-arn>'
    storage_allowed_locations = ('*')
   
-- describe the storage integration and take note of the following parameters:
 -- Retrieve the AWS IAM user for the snowflake account
 DESC INTEGRATION BISTRO_INTEGRATION;

 /* Granting usage previledge on the storage integration so that the SYSADMIN role can use it */
 grant usage on integration BISTRO_INTEGRATION to role SYSADMIN; 

-- create a new schema in the BAKERY_DB database 
 use role SYSADMIN;
 create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
 create database if not exists BAKERY_DB;
 use database BAKERY_DB;
 create schema EXTERNAL_ORDERS;
 use schema EXTERNAL_ORDERS;

 /* Create a named file format */
 create file format ORDERS_CSV_FORMAT
    type = csv
    field_delimiter = ','
    skip_header = 1;

 -- Creating or Replace external Stage using a storage integration with named file format --
 create or replace stage BISTRO_STAGE
    storage_integration = BISTRO_INTEGRATION
    url = '<your-s3-bucket-url>'
    file_format = ORDERS_CSV_FORMAT;

-- Upload a sample file named Orders_2023-08-04.csv to the s3 bucket

-- Testing: View list of files in the external stage
 list @BISTRO_STAGE

/* Create External Staging Table for restaurant orders */
use database BAKERY_DB;
use schema EXTERNAL_ORDERS;
create table ORDERS_BISTRO_STG (
    customer varchar,
    order_date date,
    delivery_date date,
    baked_good_type varchar,
    quantity number,
    source_file_name varchar,
    load_ts timestamp
);

/* Loading data from external stage into a staging table */
-- load data from the stage into the staging table by specifying a path
copy into ORDERS_BISTRO_STG
from (
    select $1, $2, $3, $4, $5, metadata$filename, current_timestamp()
    from @BISTRO_STAGE/202308
)
on_error = abort_statement
--purge = true;


/* Testing: view data in the staging table */
select * from ORDERS_BISTRO_STG;

/* View load history for the ORDERS_BISTRO_STG */
select *
from information_schema.load_history
where schema_name = 'EXTERNAL_ORDERS' and table_name = 'ORDERS_BISTRO_STG'
order by last_load_time desc;

/* stage metadata with directory tables */
-- Add directory table to external stage
alter stage BISTRO_STAGE
set directory = (enable = true);

-- refresh directory table manually
alter stage BISTRO_STAGE refresh;

-- query the directory table
select * 
from directory (@BISTRO_STAGE);



/*=== CREATING EXTERNAL TABLE FROM EXTERNAL STAGE ===*/
create external table ORDERS_BISTRO_EXT (
    customer varchar as (VALUE:c1::varchar),
    order_date date as (VALUE:c2::date),
    delivery_date date as (VALUE:c3::date),
    baked_good_type varchar as (VALUE:c4::varchar),
    quantity number as (VALUE:c5::number),
    source_file_name varchar as metadata$filename
)
location = @BISTRO_STAGE
auto_refresh = FALSE
file_format = ORDERS_CSV_FORMAT;


/* Testing : query the external table */
select * from ORDERS_BISTRO_EXT;

-- refresh external table manually
alter external table ORDERS_BISTRO_EXT refresh;


/*=== CREATING MATERIALIZE VIEW TO IMPROVE QUERY PERFORMANCE ===*/
create materialized view ORDERS_BISTRO_MV as
select customer, order_date, delivery_date, baked_good_type, quantity, source_file_name
from ORDERS_BISTRO_EXT;

/* Testing: query the materialized view */
select * from ORDERS_BISTRO_MV;
















 
