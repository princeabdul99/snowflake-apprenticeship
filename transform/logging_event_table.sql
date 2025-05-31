/* Logging: Create logging event table */
use role SYSADMIN;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema TRANSFORM;
create event table BAKERY_EVENTS;

use role ACCOUNTADMIN;
alter account set event_table = BAKERY_DB.TRANSFORM.BAKERY_EVENTS;

grant modify log level on account to role SYSADMIN;

use role SYSADMIN;
alter procedure LOAD_CUSTOMER_SUMMARY_ORDERS() set log_level = DEBUG;
--alter procedure LOAD_CUSTOMER_ORDERS() set log_level = DEBUG;
