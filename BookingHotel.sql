CREATE DATABASE HOTEL_DB;

CREATE OR REPLACE FILE FORMAT FF_CSV
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = ''''
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '');

ALTER FILE FORMAT FF_CSV
    SET FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE STAGE STG_HOTEL_BOOKINGS
    FILE_FORMAT= FF_CSV;

USE DATABASE HOTEL_DB;

CREATE TABLE BRONZE_HOTEL_BOOKING(
    booking_id STRING,
    hotel_id STRING,
    hotel_city STRING,
    customer_id STRING,
    customer_name STRING,
    customer_email STRING,
    check_in_date STRING,
    check_out_date STRING,
    room_type STRING,
    num_guests STRING,
    total_amount STRING,
    currency STRING,
    booking_status  STRING
);

COPY INTO BRONZE_HOTEL_BOOKING 
FROM @STG_HOTEL_BOOKINGS
FILE_FORMAT = (FORMAT_NAME = FF_CSV)
ON_ERROR = 'CONTINUE';

SELECT TOP 2* FROM BRONZE_HOTEL_BOOKING;

--HOTEL_DB.PUBLIC.SILVER_HOTEL_BOOKING;

CREATE TABLE SILVER_HOTEL_BOOKING
    (
    booking_id VARCHAR,
    hotel_id VARCHAR,
    hotel_city VARCHAR,
    customer_id VARCHAR,
    customer_name VARCHAR,
    customer_email VARCHAR,
    check_in_date DATE,
    check_out_date DATE,
    room_type VARCHAR,
    num_guests INTEGER,
    total_amount FLOAT,
    currency VARCHAR,
    booking_status  VARCHAR
     );

SELECT customer_email
from bronze_hotel_booking
where not customer_email like '%@%.%';

select total_amount
from bronze_hotel_booking
where try_to_number(total_amount) < 0;

select check_in_date, check_out_date
from bronze_hotel_booking
where try_to_date(check_in_date) > try_to_date(check_out_date);

select * from bronze_hotel_booking limit 10;

select distinct booking_status
from bronze_hotel_booking;

drop table silver_hotel_booking;
insert into silver_hotel_booking
select 
    booking_id,
    hotel_id,
    initcap(trim(hotel_city)) as hotel_city,
    customer_id,
    initcap(trim(customer_name)) as customer_name,
    case
        when customer_email like '%@%.%' then lower(customer_email)
        else NULL
        END AS CUSTOMER_EMAIL,
    TRY_TO_DATE(NULLIF(CHECK_IN_DATE, '')) AS CHECK_IN_DATE,
    TRY_TO_DATE(NULLIF(CHECK_OUT_DATE,'')) AS CHECK_OUT_DATE,
    room_type,
    num_guests,
    ABS(TRY_TO_NUMBER(TOTAL_AMOUNT)) AS TOTAL_AMOUNT,
    currency,
    CASE WHEN LOWER(BOOKING_STATUS) IN ('confirmeed', 'confirmd') then 'Confirmed'
    Else booking_status
    end as booking_status
from bronze_hotel_booking
where 
try_to_date(check_in_date) is not null
and try_to_date(check_out_date) is not null
and try_to_date(check_in_date) <= try_to_date(check_out_date);

select top 20* from silver_hotel_booking;


--Gold layer
CREATE TABLE GOLD_AGG_DAILY_BOOKING AS 
    SELECT 
        CHECK_IN_DATE AS DATE,
        COUNT(*) AS TOTAL_BOOKING,
        SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE
    FROM SILVER_HOTEL_BOOKING
    GROUP BY CHECK_IN_DATE
    ORDER BY DATE;
        
CREATE TABLE GOLD_AGG_HOTEL_CITY_SALES AS 
    SELECT 
        HOTEL_CITY,
        SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE
    FROM SILVER_HOTEL_BOOKING
    GROUP BY HOTEL_CITY
    ORDER BY TOTAL_REVENUE DESC;


CREATE TABLE GOLD_BOOKING_CLEAN AS 
SELECT 
    booking_id,
    hotel_id,
    hotel_city,
    customer_id,
    customer_name,
    customer_email,
    check_in_date,
    check_out_date,
    room_type,
    num_guests,
    total_amount,
    currency,
    booking_status
FROM SILVER_HOTEL_BOOKING;

