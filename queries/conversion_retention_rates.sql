WITH retention1 AS (
    SELECT 
        COUNT(DISTINCT nu.USER_ID) AS users_count
    FROM 
        NEW_USER nu
    LEFT JOIN 
        MULTIPLAYER_BATTLE_STARTED mbs ON nu.USER_ID = mbs.USER_ID 
    WHERE { TO_DATE(nu.EVENT_TIMESTAMP) } = {period_key} 
    and DATEADD(day, 1, TO_DATE(nu.EVENT_TIMESTAMP)) = TO_DATE(mbs.EVENT_TIMESTAMP);
)
WITH totalUsers AS (
    SELECT 
        COUNT(DISTINCT nu.USER_ID) AS users_count
    FROM
        NEW_USER nu
    WHERE { TO_DATE(nu.EVENT_TIMESTAMP) } = {period_key}
)
UPDATE {period_type}_Aggregation_General
SET
    day1retention = (select users_count from retention1)/(select users_count from totalUsers) * 100,
WHERE period_key = {period_key};

WITH retention3 AS (
    SELECT 
        COUNT(DISTINCT nu.USER_ID) AS users_count
    FROM 
        NEW_USER nu
    LEFT JOIN 
        MULTIPLAYER_BATTLE_STARTED mbs ON nu.USER_ID = mbs.USER_ID 
    WHERE { TO_DATE(nu.EVENT_TIMESTAMP) } = {period_key} 
    and DATEADD(day, 3, TO_DATE(nu.EVENT_TIMESTAMP)) = TO_DATE(mbs.EVENT_TIMESTAMP);
)
WITH totalUsers AS (
    SELECT 
        COUNT(DISTINCT nu.USER_ID) AS users_count
    FROM
        NEW_USER nu
    WHERE { TO_DATE(nu.EVENT_TIMESTAMP) } = {period_key}
)
UPDATE {period_type}_Aggregation_General
SET
    day3retention = (select users_count from retention3)/(select users_count from totalUsers) * 100,
WHERE period_key = {period_key};


WITH retention7 AS (
    SELECT 
        COUNT(DISTINCT nu.USER_ID) AS users_count
    FROM 
        NEW_USER nu
    LEFT JOIN 
        MULTIPLAYER_BATTLE_STARTED mbs ON nu.USER_ID = mbs.USER_ID 
    WHERE { TO_DATE(nu.EVENT_TIMESTAMP) } = {period_key} 
    and DATEADD(day, 7, TO_DATE(nu.EVENT_TIMESTAMP)) = TO_DATE(mbs.EVENT_TIMESTAMP);
)
WITH totalUsers AS (
    SELECT 
        COUNT(DISTINCT nu.USER_ID) AS users_count
    FROM
        NEW_USER nu
    WHERE { TO_DATE(nu.EVENT_TIMESTAMP) } = {period_key}
)
UPDATE {period_type}_Aggregation_General
SET
    day7retention = (select users_count from retention7)/(select users_count from totalUsers) * 100, 
WHERE period_key = {period_key};

-- Rate of users that paid at least once in 7 days after registration

WITH conversion7 AS (
    SELECT 
        COUNT(DISTINCT nu.USER_ID) AS users_count
    FROM
        NEW_USER nu
    INNER JOIN 
        IN_APP_PURCHASE_LOG_SERVER pls ON nu.USER_ID = pls.USER_ID 
    WHERE { TO_DATE(nu.EVENT_TIMESTAMP) } = {period_key} and pls.USER_IS_SPENDER = true
    and DATEADD(day, 7, TO_DATE(nu.EVENT_TIMESTAMP)) >= TO_DATE(pls.EVENT_TIMESTAMP)
)
WITH totalUsers AS (
    SELECT 
        COUNT(DISTINCT nu.USER_ID) AS users_count
    FROM
        NEW_USER nu
    WHERE { TO_DATE(nu.EVENT_TIMESTAMP) } = {period_key}
)
UPDATE {period_type}_Aggregation_General
SET
    day7conversion = (select users_count from conversion7)/(select users_count from totalUsers) * 100,
WHERE period_key = {period_key};