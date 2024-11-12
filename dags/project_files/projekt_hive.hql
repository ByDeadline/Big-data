DROP TABLE IF EXISTS map_reduce_result;
DROP TABLE IF EXISTS datasource4;
DROP TABLE IF EXISTS top_leagues_by_wage_json;


CREATE EXTERNAL TABLE IF NOT EXISTS map_reduce_result (
    league_id STRING,
    avg_wage DOUBLE,
    avg_age DOUBLE,
    count_players INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${input_dir3}/';

CREATE EXTERNAL TABLE IF NOT EXISTS datasource4 (
    league_id STRING,
    league_name STRING,
    league_level INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '${input_dir4}/';

CREATE EXTERNAL TABLE IF NOT EXISTS top_leagues_by_wage_json (
    league_id STRING,
    league_name STRING,
    league_level INT,
    avg_wage DOUBLE,
    avg_age DOUBLE,
    count_players INT
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${output_dir6}';

INSERT OVERWRITE TABLE top_leagues_by_wage_json
SELECT 
    ranked_leagues.league_id,
    ranked_leagues.league_name,
    ranked_leagues.league_level,
    ranked_leagues.avg_wage,
    ranked_leagues.avg_age,
    ranked_leagues.count_players
FROM 
    (SELECT 
        COALESCE(r.league_id, 'nulI') as league_id,
        r.avg_wage,
        r.avg_age,
        r.count_players,
        d.league_name,
        d.league_level,
        ROW_NUMBER() OVER (PARTITION BY d.league_level ORDER BY r.avg_wage DESC) AS rank
    FROM 
        map_reduce_result r
    LEFT JOIN 
        datasource4 d ON r.league_id = d.league_id
    WHERE
        d.league_id IS NOT NULL AND 
        d.league_name IS NOT NULL AND 
        d.league_level IS NOT NULL) ranked_leagues
WHERE 
    ranked_leagues.rank <= 3;
