-- It's good practice store to store all results of the queries below in special tables, 
-- which later will be used by analytics team. For simplicity i will omit this inserts

-- Runs daily calucales amount of battles for {day} days sinse registration. 
-- Later this data should be stored in user statistics, Battles_Day_{day} column for each user in the system. 

SELECT
    COUNT(CASE WHEN DATEDIFF(day, NU.EVENT_TIMESTAMP, MBS.EVENT_TIMESTAMP) = {DAY} THEN MBS.USER_ID END) AS Battles_Day_1
FROM
    MULTIPLAYER_BATTLE_STARTED MBS
JOIN
    NEW_USER NU ON MBS.USER_ID = NU.USER_ID
WHERE MBS.EVENT_TIMESTAMP <= {period_key} and MBS.EVENT_TIMESTAMP >= DATEADD(day, -{DAY}, {period_key})
GROUP BY
    MBS.USER_ID;

-- In this example let's define active users as users with active session
-- And with this assumption, a lot depends on session length.
-- For simplicity let's just take one day and calculate how many users in this day opened a session 
-- and had at least one battle
-- Later this data could be stored in battle statisctics daily table


WITH ACTIVE_USERS (
    SELECT COUNT(DISTINCT USER_ID) USERS_COUNT
    FROM SESSION_STARTED SS
    WHERE SS.EVENT_TIMESTAMP = {period_key}
)
BATTLED_USERS (
    SELECT COUNT(DISTINCT USER_ID) USERS_COUNT 
    FROM MULTIPLAYER_BATTLE_STARTED MBS 
        JOIN SESSION_STARTED SS ON MBS.USER_ID = SESSION_STARTED.USER_ID
    WHERE TO_DATE(SS.EVENT_TIMESTAMP) = {period_key} and TO_DATE(MBS.EVENT_TIMESTAMP) = {period_key}
)
SELECT (SELECT USERS_COUNT FROM BATTLED_USERS) / (SELECT USERS_COUNT FROM ACTIVE_USERS);