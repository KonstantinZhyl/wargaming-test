# wargaming-test

Hello! 

My first assumption will be about technologies, there is no input data about preffered technologies. So I guess the goal of this test task to focus more on data modelling and analytics skills. 
I did quick research and found out that Wargaming mostly uses Snowflake data warehouse.  
All tables provided are separate fact tables in a data warehouse. I would suggest to create separate aggregation tables for each of the time periods (daily, weekly, monthly). These tables will contain pre-calculated values for our KPIs.
Snowflake does not have its own built-in ETL, so (considering this Job Role description) we will use Airflow. Also there is a Snowflake connector for python. Pipeline schedule interval may be custom in case we need intermediate KPI values, but for now I suggest to do it based on aggregation table. E.g. daily calculate KPIs for daily table.

All thoughts regarding queries you can find inside scripts.

Question:
"Add any additional metrics which you think will be valuable for Game performance analysis."

Answer:
* Churn Rate: The rate of users stop playing the game.
* Overall Participation: by all users in battles daily/weekly/monthly.
* User Acquisition Cost: The cost of acquiring a new player using advertising and marketing.
* Player Progression Metrics: By tracking how players progress through levels can help us to understand game difficulty and player engagement.
* Revenue by ship (battle machine)
* Usage metrics across different devices
* Also a lot of metrics could be collected from data, i've suggested to collect in the next question.

Question:
"What would you change in source tables design and which additional data would you collect from a game."

Answer:
* Indexing: Ensure that frequently queried columns (like EVENT_UUID, USER_ID, SHIP_ID) are indexed.
* Data Types and Constraints: I can't see any constraints regarding provided columns.

* User Engagement Data: Session length, achievements.
* Player Demographics: Age, gender, language ...
* Multiplayer interactions, team activities.
* Technical Performance Data: Load times, crashes, and other technical issues encountered by players.
* Marketing and Acquisition Data: Source of game download (through an ad?, referral?).
* Battle Performance Metrics: Detailed combat statistics for each player in a battle (e.g., number of hits, misses, critical hits, ships sunk). Types and amounts of damage dealt (e.g., gunfire, torpedoes, aircraft). Patterns in gameplay style.
* Ship Usage and Performance: Performance metrics for each ship, such as win rate, average damage dealt, survival rate.
* Data on the matchmaking process: waiting time, player skill levels. Balance and fairness of matches.
