DROP TABLE IF EXISTS result;
CREATE TABLE result (
    match_code VARCHAR(100) PRIMARY KEY,
    date VARCHAR(100),
    home VARCHAR(100),
    away VARCHAR(100),
    home_score INT,
    away_score INT,
    stadium VARCHAR(100),
    city VARCHAR(100),
    capacity VARCHAR(100)
);


DROP TABLE IF EXISTS goal;
CREATE TABLE goal (
    player_id VARCHAR(100),
    time Varchar(100),
    match_code VARCHAR(100),
    date VARCHAR(100),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    club VARCHAR(100),
    PRIMARY KEY (match_code, player_id,time)
);         
                
DROP TABLE IF EXISTS appearance;
CREATE TABLE appearance (
    match_code VARCHAR(100),
    player_id VARCHAR(100),
    date VARCHAR(100),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    position VARCHAR(100),
    club VARCHAR(100),
    PRIMARY KEY (match_code, player_id) 
);
          
                
DROP TABLE IF EXISTS cards;
CREATE TABLE cards (
    player_id VARCHAR(100),
    match_code VARCHAR(100),
    date VARCHAR(100),
    type Varchar(10),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    club VARCHAR(100)
);         
    
                
                
                