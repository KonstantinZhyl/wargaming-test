-- Active users could be defined differently. That could be users with active session, 
-- or users that participate in battle. In my query it's just a users, who logged in that time period. 
-- Later i will use different definition of active users just for variety.

-- In queries below i will use parameters like {period_key} that will go from airflow. 
-- Also for different time periods i will use different TIMESTAMP converting functions. 
-- for period_type = week I will use DATE_TRUNC('WEEK', MY_TIMESTAMP_TZ) 
-- and for month - DATE_TRUNC('MONTH', MY_TIMESTAMP_TZ). I also put this parameterised functions inside "{}"

-- I don't know what does USER_IS_SPENDER column mean, but i guess transactions can be from user and to user, 
-- so this column mark transactions, when user spent money.

-- I calculate revenue as sum of money spent by users. 
-- Also I assume, when user buys subscription, that transaction will also occur in IN_APP_PURCHASE_LOG_SERVER.

-- There is a suspicious field IS_FREETRIAL, but it's unclear what does that mean for revenue and conversion rate
-- so I don't conser it in my calculations


WITH ActiveUsers AS (
    SELECT 
        COUNT(DISTINCT l.USER_ID) as active_users
    FROM "LOGIN" l
    WHERE { TO_DATE(l.EVENT_TIMESTAMP) } = {period_key} and l.LOGIN_SUCCESSFUL = true
),
NewUsers AS (
    SELECT 
        COUNT(DISTINCT nu.USER_ID) as new_users
    FROM "NEW_USER" nu
    WHERE { TO_DATE(nu.EVENT_TIMESTAMP) } = {period_key}
),
RevenueUSD AS (
    SELECT 
        SUM(pl.USD_COST) as revenue_usd
    FROM "IN_APP_PURCHASE_LOG_SERVER" pl
    WHERE { TO_DATE(pl.EVENT_TIMESTAMP) } = {period_key} and pl.USER_IS_SPENDER = true
),
AverageRevenuePerUser AS (
    SELECT 
        AVG(user_revenue.revenue_per_user) as avg_revenue_per_user
    FROM (
        SELECT SUM(pl.USD_COST) as revenue_per_user
        FROM "IN_APP_PURCHASE_LOG_SERVER" pl
        WHERE { TO_DATE(pl.EVENT_TIMESTAMP) } = {period_key} and pl.USER_IS_SPENDER = true
        GROUP BY pl.USER_ID
    ) AS user_revenue
),
AverageRevenuePerPayingUser AS (
    SELECT 
        AVG(user_revenue.revenue_per_user) as avg_revenue_per_paying_user
    FROM (
        SELECT SUM(pl.USD_COST) as revenue_per_user
        FROM "IN_APP_PURCHASE_LOG_SERVER" pl
        WHERE { TO_DATE(pl.EVENT_TIMESTAMP) } = {period_key} and pl.USER_IS_SPENDER = true and pl.USER_IS_PREMIUM = true
        GROUP BY pl.USER_ID
    ) AS user_revenue
)
INSERT INTO {period_type}_Aggregation_General(period_key, active_users, new_users, revenue_usd, arpu, arppu)
VALUES (
    {period_key}, 
    (select active_users from ActiveUsers), 
    (select new_users from NewUsers), 
    (select revenue_usd from RevenueUSD), 
    (select avg_revenue_per_user from AverageRevenuePerUser), 
    (select avg_revenue_per_paying_user from AverageRevenuePerPayingUser)
);
