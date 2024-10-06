# **Badminton Analytics Project using Spark and SQL**

## **Project Overview**
This project solves real-world sports-related problems using **PySpark** and **Spark SQL**. The dataset contains information about badminton players, including their login dates, kit (device) used, and session counts. Using various PySpark and SQL operations such as **window functions**, we derive insights like the first login date, the first device used, and cumulative games played.

The project was implemented using Google Colab and integrated with PySpark.

## **Problem Statements**

### 1. **First Login Date of Each User**
In this task, the goal was to find the first login date of each user. Using **SQL GROUP BY** would result in loss of some other column information, so we utilized **window functions** instead to maintain additional columns like `kit_id` and `session_count`.

**SQL Approach:**
```sql
WITH cte AS (
    SELECT *, 
           RANK() OVER (PARTITION BY user_id ORDER BY login_date) AS rnk 
    FROM badminton_table
)
SELECT user_id, login_date 
FROM cte 
WHERE rnk = 1;
```

**PySpark Approach:**
```python
from pyspark.sql import Window
import pyspark.sql.functions as f

windowspec = Window.partitionBy(f.col('user_id')).orderBy(f.col('login_date'))
inputDF = inputDF.withColumn('rnk', f.rank().over(windowspec))
inputDF.filter(f.col('rnk') == 1).select('user_id', 'login_date').show()
```

### 2. **First Kit (Device) Used by Each User**
The second problem aimed to find the first kit (device) used by each user by login date. Similar to the first problem, **window functions** were used to rank the rows based on `login_date` for each `user_id`.

**SQL Approach:**
```sql
WITH kitcte AS (
    SELECT *, 
           RANK() OVER (PARTITION BY user_id ORDER BY login_date) AS rnks 
    FROM badminton_table
)
SELECT user_id, kit_id 
FROM kitcte 
WHERE rnks = 1;
```

**PySpark Approach:**
```python
windowspec = Window.partitionBy(f.col('user_id')).orderBy(f.col('login_date'))
inputDF = inputDF.withColumn('rnk', f.rank().over(windowspec))
inputDF.filter(f.col('rnk') == 1).select('user_id', 'kit_id').show()
```

### 3. **Total Games Played So Far for Each User**
In this task, we calculate the cumulative number of games (or sessions) played by each user, up to each login date.

**SQL Approach:**
```sql
SELECT user_id, login_date,
       SUM(session_count) OVER (PARTITION BY user_id ORDER BY login_date) AS games_played_so_far
FROM badminton_table;
```

**PySpark Approach:**
```python
windowspec = Window.partitionBy('user_id').orderBy('login_date')
inputDF = inputDF.withColumn('games_played_so_far', f.sum('session_count').over(windowspec))
inputDF.select('user_id', 'login_date', 'games_played_so_far').show()
```

### 4. **Players Logged in on Two Consecutive Days**
The final problem identifies players who logged in again on the day immediately following their previous login. We used the **lead()** window function to access the next row and then calculated the difference in login dates.

**PySpark Approach:**
```python
windowSpec = Window.partitionBy("user_id").orderBy("login_date")
df_with_lead = inputDF.withColumn("next_login_date", f.lead("login_date", 1).over(windowSpec))
df_with_lead = df_with_lead.withColumn('diff', f.datediff('next_login_date', 'login_date'))
df_with_lead.filter(f.col('diff') == 1).select('user_id').show()
```

## **Technologies Used**
- **PySpark**: For distributed data processing and implementing SQL queries and DataFrame API.
- **Spark SQL**: To write SQL queries for solving problems using window functions.
- **Google Colab**: As the primary environment for developing and running PySpark code.

## **Dataset**
The sample dataset consists of four fields:
- **user_id**: The unique identifier for each user.
- **kit_id**: The device used by the player.
- **login_date**: The date on which the user logged in.
- **session_count**: The number of sessions the player participated in on that day.

Example data:
```python
data = [
    (1, 2, "2016-03-01", 5),
    (1, 2, "2016-03-02", 6),
    (2, 3, "2017-06-25", 1),
    (3, 1, "2016-03-02", 0),
    (3, 4, "2018-07-03", 5)
]
```

## **Conclusion**
This project demonstrates the use of **window functions** in both **SQL** and **PySpark** to solve various sports analytics problems, like finding the first login date, tracking the first kit used, calculating cumulative sessions played, and detecting consecutive logins.
By using both **Spark SQL** and the **DataFrame API**, this project highlights how PySpark can efficiently handle large datasets and complex queries in a distributed environment.
