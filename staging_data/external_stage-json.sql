/* Create a Cloud Storage Integration in Snowflake*/
use role ACCOUNTADMIN;
create storage integration HOTEL_INN_INTEGRATION
    type = external_stage
    storage_provider = 'S3'
    enabled = true
    storage_aws_role_arn = 'arn:aws:iam::<arn-role>'
    storage_allowed_locations = ('s3://<bucket>')


/* View Details of storage integration */
describe storage integration HOTEL_INN_INTEGRATION;

 /* Granting usage previledge on the storage integration */
grant usage on integration HOTEL_INN_INTEGRATION to role SYSADMIN; 

/* Create an External Stage */
use role SYSADMIN;
use warehouse BAKERY_WH;
use database BAKERY_DB;
create schema EXTERNAL_JSON_ORDERS;
use schema EXTERNAL_JSON_ORDERS;

/*  -- Creating or Replace external Stage using a storage integration with named file format */
create stage HOTEL_INN_STAGE
    storage_integration = HOTEL_INN_INTEGRATION
    url = '<bucket>'
    file_format = (type = json);

-- Testing: View list of files in the external stage
list @HOTEL_INN_STAGE;

-- Testing: query data in the external table
select $1 from @HOTEL_INN_STAGE;

/* Ingesting Json data into snowflake and flatten it into relational table */

-- Create table in snowflake to store JSON data
use database BAKERY_DB;
use schema EXTERNAL_JSON_ORDERS;
create table ORDERS_HOTEL_INN_RAW_STG (
    customer_orders variant,
    source_file_name varchar,
    load_ts timestamp
);

-- load data from external stage into the table
copy into ORDERS_HOTEL_INN_RAW_STG
from (
    select 
        $1,
        metadata$filename,
        current_timestamp()
    from @HOTEL_INN_STAGE  
)
on_error = abort_statement;


-- Testing: query stage table --
select * from ORDERS_HOTEL_INN_RAW_STG;

-- Flattening semistructured data into relational tables

    -- Selecting values from keys at the highest level of the hierarchy
    select
        customer_orders:"Customer"::varchar as customer,
        customer_orders:"Order date"::date as order_date,
        customer_orders:"Orders"
    from ORDERS_HOTEL_INN_RAW_STG;   

     -- Selecting values from keys at the second level of the hierarchy
    select
        customer_orders:"Customer"::varchar as customer,
        customer_orders:"Order date"::date as order_date,
        value:"Delivery date"::date as delivery_date,
        value:"Orders by day"
    from ORDERS_HOTEL_INN_RAW_STG,
    lateral flatten (input => customer_orders:"Orders");

    -- Selecting values from keys at the third level of the hierarchy
    select
        customer_orders:"Customer"::varchar as customer,
        customer_orders:"Order date"::date as order_date,
        CO.value:"Delivery date"::date as delivery_date,
        DO.value:"Baked good type"::varchar as baked_good_type,
        DO.value:"Quantity"::number as quantity
    from ORDERS_HOTEL_INN_RAW_STG,
    lateral flatten (input => customer_orders:"Orders") CO,
    lateral flatten (input => CO.value:"Orders by day") DO;

    -- Create view representing the raw JSON data in relational format 
    -- *** This view represent staged data from JSON files flattend into relational format
    create view ORDERS_HOTEL_INN_STG as 
    select
        customer_orders:"Customer"::varchar as customer,
        customer_orders:"Order date"::date as order_date,
        CO.value:"Delivery date"::date as delivery_date,
        DO.value:"Baked good type"::varchar as baked_good_type,
        DO.value:"Quantity"::number as quantity,
        source_file_name,
        load_ts
    from ORDERS_HOTEL_INN_RAW_STG,
    lateral flatten (input => customer_orders:"Orders") CO,
    lateral flatten (input => CO.value:"Orders by day") DO;

    -- Testing: query stage view --
    select * from ORDERS_HOTEL_INN_STG;
        
    



