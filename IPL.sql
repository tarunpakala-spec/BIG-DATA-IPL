CREATE or replace database IPL;
CREATE or replace schema IPL_Schema;

USE DATABASE IPL;
USE SCHEMA IPL_SCHEMA;


CREATE OR REPLACE FILE FORMAT IPL_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL');


  CREATE OR REPLACE STAGE IPL_STAGE
  FILE_FORMAT = IPL_CSV_FORMAT;


  CREATE OR REPLACE TABLE DELIVERIES_RAW (
  match_id         NUMBER,
  inning           NUMBER,
  batting_team     STRING,
  bowling_team     STRING,
  over             NUMBER,
  ball             NUMBER,
  batsman          STRING,
  non_striker      STRING,
  bowler           STRING,
  is_super_over    NUMBER,
  wide_runs        NUMBER,
  bye_runs         NUMBER,
  legbye_runs      NUMBER,
  noball_runs      NUMBER,
  penalty_runs     NUMBER,
  batsman_runs     NUMBER,
  extra_runs       NUMBER,
  total_runs       NUMBER,
  player_dismissed STRING,
  dismissal_kind   STRING,
  fielder          STRING
);

SELECT * FROM "IPL"."IPL_SCHEMA"."BIGDATA_IPL" LIMIT 10;


SELECT * FROM BIGDATA_IPL_CLEAN_PY LIMIT 10;


SELECT
    batsman,
    COUNT(*) AS balls_faced,
    SUM(batsman_runs) AS total_runs
FROM BIGDATA_IPL_CLEAN_PY
GROUP BY batsman
ORDER BY total_runs DESC
LIMIT 10;


SELECT
    batsman,
    COUNT(*) AS balls_faced,
    SUM(batsman_runs) AS total_runs,
    ROUND( (SUM(batsman_runs) * 100.0) / COUNT(*), 2) AS strike_rate
FROM BIGDATA_IPL_CLEAN_PY
GROUP BY batsman
HAVING COUNT(*) >= 200
ORDER BY strike_rate DESC
LIMIT 10;


SELECT
    bowler,
    COUNT(*) AS balls_bowled,
    SUM(total_runs) AS runs_conceded,
    ROUND( (SUM(total_runs) * 6.0) / COUNT(*), 2) AS economy_rate
FROM BIGDATA_IPL_CLEAN_PY
GROUP BY bowler
HAVING COUNT(*) >= 300
ORDER BY economy_rate ASC
LIMIT 10;


SELECT
    phase,
    SUM(batsman_runs) AS total_batsman_runs,
    COUNT(*) AS balls_faced,
    ROUND( (SUM(batsman_runs) * 6.0) / COUNT(*), 2) AS run_rate
FROM BIGDATA_IPL_CLEAN_PY
GROUP BY phase;



SELECT
    batting_team,
    SUM(total_runs) AS total_runs_scored,
    COUNT(DISTINCT match_id) AS matches_batted,
    ROUND( SUM(total_runs) * 1.0 / COUNT(DISTINCT match_id), 2) AS avg_runs_per_match
FROM BIGDATA_IPL_CLEAN_PY
GROUP BY batting_team
ORDER BY avg_runs_per_match DESC;



  
