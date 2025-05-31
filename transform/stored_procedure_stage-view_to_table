
/* TRANSFORMATION with Stored Procedures */

-- Create a schema
use role SYSADMIN;
use warehouse BAKERY_WH;
use database BAKERY_DB;
create schema TRANSFORM;
use schema TRANSFORM;

-- Create a view that combines all staging tables and views
create view ORDERS_COMBINED_STG as
select customer, order_date, delivery_date, baked_good_type, quantity,
    source_file_name, load_ts
from bakery_db.orders.ORDERS_STG
union all
select customer, order_date, delivery_date, baked_good_type, quantity,
    source_file_name, load_ts
from bakery_db.external_orders.ORDERS_BISTRO_STG
union all
select customer, order_date, delivery_date, baked_good_type, quantity,
    source_file_name, load_ts
from bakery_db.external_json_orders.ORDERS_HOTEL_INN_STG;

-- Testing: query combined staging table
select * from ORDERS_COMBINED_STG;

-- Create a combined target table
use database BAKERY_DB;
use schema TRANSFORM;
create or replace table CUSTOMER_ORDERS_COMBINED (
    customer varchar,
    order_date date,
    delivery_date date,
    baked_good_type varchar,
    quantity number,
    source_file_name varchar,
    load_ts timestamp
);

/* Creating Basic Stored Procedure */
-- stored procedure name LOAD_CUSTOMER_ORDERS that executes the MERGE statement

use database BAKERY_DB;
use schema TRANSFORM;
create or replace procedure LOAD_CUSTOMER_ORDERS()
returns varchar
language sql
as
$$
begin
    -- Adds Logging
    SYSTEM$LOG_DEBUG('LOAD_CUSTOMER_ORDERS begin ');

    -- Merging customer orders from staging table into target table --
    merge into CUSTOMER_ORDERS_COMBINED tgt
    using ORDERS_COMBINED_STG as src
    -- the columns that ensure uniqueness--
    on src.customer = tgt.customer
        and src.delivery_date = tgt.delivery_date
        and src.baked_good_type = tgt.baked_good_type
    -- update the target table with the values from the source table --
    when matched then
        update set tgt.quantity = src.quantity,
            tgt.source_file_name = src.source_file_name,
            tgt.load_ts = current_timestamp()
    -- insert new values from the source table into the target table --       
    when not matched then
        insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
        values(src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity,
                src.source_file_name, current_timestamp());
    return 'Load Completed. ' || SQLROWCOUNT || ' rows affected.';
    
 -- Exception Handling --   
exception
    when other then
        return 'Load failed with error message: ' || SQLERRM;
end;    
$$;

-- Execute: Call Command
call LOAD_CUSTOMER_ORDERS();


/* Testing */
select * 
from CUSTOMER_ORDERS_COMBINED 
order by delivery_date desc

/* Log View:  */
select * 
from BAKERY_EVENTS
order by timestamp desc;

truncate table CUSTOMER_ORDERS_COMBINED;

