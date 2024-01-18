-- Do do that efficiently we need to run this stats daily and compare stats for the previous day 
-- with logs for the new day. So new logs will be some kind of the diff, that we will apply.
-- Below there are two queries. For the first day of transaction history, and for the each folowing day.
-- If there is a need to do that from beginning, airflow will allow us to calculate historical data. 

-- FIRST DATE 
WITH NEW_STATS AS (
    SELECT TO_DATE(stl.EVENT_TIMESTAMP) as DATE, stl.SHIP_ID, stl.USER_ID, SUM(stl.SC_AMOUNT) as SC_AMOUNT
    FROM SHIP_TRANSACTION_LOG stl 
    WHERE TO_DATE(stl.EVENT_TIMESTAMP) = {first_date}
    GROUP BY TO_DATE(stl.EVENT_TIMESTAMP), stl.SHIP_ID, stl.USER_ID
)
INSERT INTO Daily_Ships_By_User(DATE, USER_ID, SHIP_ID, SC_AMOUNT)
SELECT * FROM MERGED_STATS;

-- LATER EXECUTIONS
WITH NEW_STATS AS (
    SELECT TO_DATE(stl.EVENT_TIMESTAMP) as DATE, stl.SHIP_ID, stl.USER_ID, SUM(stl.SC_AMOUNT) as SC_AMOUNT
    FROM SHIP_TRANSACTION_LOG stl 
    WHERE TO_DATE(stl.EVENT_TIMESTAMP) = {period_key}
    GROUP BY TO_DATE(stl.EVENT_TIMESTAMP), stl.SHIP_ID, stl.USER_ID
)
MERGED_STATS AS (
    SELECT 
        CASE
            WHEN sbu.DATE is NULL THEN ns.DATE
            ELSE sbu.DATE
        END as DATE,
        CASE
            WHEN sbu.USER_ID is NULL THEN ns.USER_ID
            ELSE sbu.USER_ID
        END as USER_ID,
        CASE
            WHEN sbu.SHIP_ID is NULL THEN ns.SHIP_ID
            ELSE sbu.SHIP_ID
        END as SHIP_ID,
        IFNULL(ns.SC_AMOUNT, 0) + IFNULL(stl.SC_AMOUNT, 0) as SC_AMOUNT
    FROM NEW_STATS ns FULL JOIN Daily_Ships_By_User sbu
        on DATEADD(day, -1, {period_key}) = sbu.DATE 
        and sbu.SHIP_ID = stl.SHIP_ID and sbu.USER_ID = stl.USER_ID
    WHERE SC_AMOUNT > 0
)
INSERT INTO Daily_Ships_By_User(DATE, USER_ID, SHIP_ID, SC_AMOUNT)
SELECT * FROM MERGED_STATS;

-- To calculate ships popularity let's just calculate amount of each ship 
-- and divide it to amount of all ships owned by users in this day. 
-- To do this we will use Daily_Ships_By_User calculated before. 
-- That is also daily statistic, so we should store this in ships daily stats table.

WITH ALL_SHIPS (
    SELECT SUM(dsbu.SC_AMOUNT) AMOUNT
    FROM Daily_Ships_By_User dsbu
    WHERE dsbu.DATE = {period_key}
)
SELECT SHIP_ID, SUM(SC_AMOUNT) / (select AMOUNT from ALL_SHIPS) FROM Daily_Ships_By_User
WHERE DATE = {period_key}
GROUP BY SHIP_ID;
