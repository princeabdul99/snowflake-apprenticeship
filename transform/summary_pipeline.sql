/* Robust Data Pipeline*/
-- Creata table named SUMMARY_ORDERS that wll store summarized data

use role SYSADMIN;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema TRANSFORM;
create table SUMMARY_ORDERS (
    delivery_date date,
    baked_good_type varchar,
    total_quantity number
);

/* Truncating Table and Inserting Summarized data */
truncate table SUMMARY_ORDERS;
insert into SUMMARY_ORDERS(delivery_date, baked_good_type, total_quantity)
    select delivery_date, baked_good_type, sum(quantity) as total_quantity
    from CUSTOMER_ORDERS_COMBINED
    group by all;
    

/* Creating Basic Stored Procedure */
-- stored procedure name LOAD_CUSTOMER_SUMMARY_ORDERS that insert summary data
use database BAKERY_DB;
use schema TRANSFORM;
create or replace procedure LOAD_CUSTOMER_SUMMARY_ORDERS()
returns varchar
language sql
as
$$
begin
  SYSTEM$LOG_DEBUG('LOAD_CUSTOMER_SUMMARY_ORDERS begin');

  truncate table SUMMARY_ORDERS;
  insert into SUMMARY_ORDERS(delivery_date, baked_good_type, total_quantity)
        select delivery_date, baked_good_type, sum(quantity) as total_quantity
        from CUSTOMER_ORDERS_COMBINED
        group by all;
  return 'Load completed. ' || SQLROWCOUNT || ' rows inserted.';
  
exception
    when other then
        return 'Load failed with error message: ' || SQLERRM;
end;
$$;

call LOAD_CUSTOMER_SUMMARY_ORDERS();

select * 
from BAKERY_EVENTS
order by timestamp desc;
    

select *
from SUMMARY_ORDERS order by delivery_date desc;

