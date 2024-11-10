
DROP TABLE IF EXISTS apperance;
CREATE TABLE apperance (
    match_code VARCHAR(40),
    player_id VARCHAR(40),
    date VARCHAR(40)
);

DROP TABLE IF EXISTS goal;
CREATE TABLE goal(
    player_id VARCHAR(40),
    time VARCHAR(200),
    match_code VARCHAR(40),
    date VARCHAR(20)
);

DROP TABLE IF EXISTS player_info;
CREATE TABLE player_info (
    player_id VARCHAR(40),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    position VARCHAR(50),
    club VARCHAR(50)
);

DROP TABLE IF EXISTS match_results;
CREATE TABLE match_results (
    match_code VARCHAR(40),
    date VARCHAR(20),
    home INT,
    away INT,
    match_score VARCHAR(10)
);

DROP TABLE IF EXISTS teams;
CREATE TABLE teams (
    team VARCHAR(50),
    abbreviated VARCHAR(10),
    stadium VARCHAR(100),
    city VARCHAR(50),
    capacity INT
);


DROP TABLE IF EXISTS cards;
CREATE TABLE cards (
    player_id VARCHAR(40),
    match_code VARCHAR(40),
    time Varchar(10),
    date VARCHAR(20),
    type VARCHAR(10)
);

