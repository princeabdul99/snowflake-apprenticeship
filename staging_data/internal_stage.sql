/* 
* INITIAL SETUP
* Create Database, Schema, virtual warehouse using sysadmin role
*/
use role SYSADMIN;
create database BAKERY_DB;
create schema ORDERS;
create warehouse BAKERY_WH with warehouse_size = 'XSMALL';

/* Create Internal Stage */
use database BAKERY_DB;
use schema ORDERS;
create stage ORDERS_STAGE;

-- view the contents of the stage (will be empty upon creation)
list @ORDERS_STAGE;

-- manually upload file Orders_2023-07-07.csv into the ORDERS_STAGE internal stage using the Snowsight user interface

-- view the contents of the stage again (should show the file that was just uploaded)
list @ORDERS_STAGE;

/*View all column data in the internal stage */
select $1, $2, $3, $4, $5 from @ORDERS_STAGE;

/* Create Staging Table */
use database BAKERY_DB;
use schema ORDERS;
create table ORDERS_STG (
    customer varchar,
    order_date date,
    delivery_date date,
    baked_good_type varchar,
    quantity number,
    source_file_name varchar,
    load_ts timestamp
);

/*COPY Command to load data from staged file into Snowflake*/
-- copy data from the internal stage to the staging table using parameters:
-- - file_format to specify that the header line is to be skipped
-- - on_error to specify that the statement is to be aborted if an error is encountered
-- - purge the csv file from the internal stage after loading data

use database BAKERY_DB;
use schema ORDERS;
copy into ORDERS_STG
from (
    select $1, $2, $3, $4, $5, metadata$filename, current_timestamp() 
    from @ORDERS_STAGE
)
file_format = (type = CSV, skip_header = 1)
on_error = abort_statement
purge = true;


/* Testing - View the data that was loaded */
select * from ORDERS_STG;
-- view the contents of the stage again (should be empty again because the file was purged after loading)
list @ORDERS_STAGE;

/* === Merging data from the staging table into target table === */
/* Create Target Table */
use database BAKERY_DB;
use schema ORDERS;
create table CUSTOMER_ORDERS (
    customer varchar,
    order_date date,
    delivery_date date,
    baked_good_type varchar,
    quantity number,
    source_file_name varchar,
    load_ts timestamp
);

-- merge data from the staging table into the target table
-- the targe table
merge into CUSTOMER_ORDERS tgt

-- the source table
using ORDERS_STG as src

-- the columns that ensure uniqueness
on src.customer = tgt.customer
    and src.delivery_date = tgt.delivery_date
    and src.baked_good_type = tgt.baked_good_type
    
-- update the target table with the values from the source table
when matched then
    update set tgt.quantity = src.quantity,
    tgt.source_file_name = src.source_file_name,
    tgt.load_ts = current_timestamp()

-- insert new values from the source table into the target table
when not matched then
    insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
    values(src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_date());

    
/* Testing */
select * from CUSTOMER_ORDERS order by delivery_date desc;
    

/* ==== Transforming data ==== */
use database BAKERY_DB;
use schema ORDERS;
create table SUMMARY_ORDERS (
    delivery_date date,
    baked_good_type varchar,
    total_quantity number
);

-- Truncate Summary Order Table
truncate table SUMMARY_ORDERS;

/* Insert Summarized Data into Summary Table */
insert into SUMMARY_ORDERS (delivery_date, baked_good_type, total_quantity)
    select delivery_date, baked_good_type, sum(quantity) as total_quantity 
    from CUSTOMER_ORDERS
    group by all;

-- view data in the summary table
select * from SUMMARY_ORDERS;
    

/* Automating Process with Tasks */
-- create task that executes the previous steps on schedule:
-- - truncates the staging table
-- - loads data from the internal stage into the staging table using the COPY command
-- - merges data from the staging table into the target table
-- - truncates the summary table
-- - inserts summarized data into the summary table
-- - executes every 10 minutes (for testing) - later will be rescheduled to run once every evening

use database BAKERY_DB;
use schema ORDERS;
create task PROCESS_ORDERS
    warehouse = BAKERY_WH
    schedule = '10 M'
as
begin
    /* Truncate Staging Table */
    truncate table ORDERS_STG;
    
    /* Load Data from Internal Stage into Staging Table Using COPY*/
    copy into ORDERS_STG
        from (
            select $1, $2, $3, $4, $5, metadata$filename, current_timestamp() 
            from @ORDERS_STAGE
        )
        file_format = (type = CSV, skip_header = 1)
        on_error = abort_statement
        purge = true;
        
    /* Merge Data from Staging table to Target Table */
        -- the targe table
    merge into CUSTOMER_ORDERS tgt
    
    -- the source table
    using ORDERS_STG as src
    
    -- the columns that ensure uniqueness
    on src.customer = tgt.customer
        and src.delivery_date = tgt.delivery_date
        and src.baked_good_type = tgt.baked_good_type
        
    -- update the target table with the values from the source table
    when matched then
        update set tgt.quantity = src.quantity,
        tgt.source_file_name = src.source_file_name,
        tgt.load_ts = current_timestamp()
    
    -- insert new values from the source table into the target table
    when not matched then
        insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
        values(src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_date());

    /* Truncate Summary Table */
    truncate table SUMMARY_ORDERS;
    
    /* Insert Summarized Data into Summary Table */
    insert into SUMMARY_ORDERS (delivery_date, baked_good_type, total_quantity)
        select delivery_date, baked_good_type, sum(quantity) as total_quantity 
        from CUSTOMER_ORDERS
        group by all;
        
end;

-- granting execute task previledge to sysadmin
use role accountadmin;
grant execute task on account to role sysadmin;
use role sysadmin;

-- execute task a single time
execute task PROCESS_ORDERS;

-- View previous and scheduled task execustions --
select *
    from table(information_schema.task_history())
    order by scheduled_time desc;


-- resume task to enable run on defined schedule
alter task PROCESS_ORDERS resume;



/* Schedule Task Using CRON */
-- change the task schedule to run at 11PM using UTC timezone
-- must suspend task first and resume after changing the schedule

alter task PROCESS_ORDERS suspend;

alter task PROCESS_ORDERS
set schedule = 'USING CRON 0 23 * * * UTC';

alter task PROCESS_ORDERS resume;

-- when done, suspend the task so that it doesn't continue to execute and consume credits
alter task PROCESS_ORDERS suspend;
