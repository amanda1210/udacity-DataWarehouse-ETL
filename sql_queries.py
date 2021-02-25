import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
print(config['S3']['LOG_DATA'])

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events(
    artist_name VARCHAR(200), 
    auth VARCHAR(100), 
    first_name VARCHAR(200), 
    gender VARCHAR(50), 
    itemInSession INT, 
    last_name VARCHAR(200), 
    length FLOAT, 
    level VARCHAR(100) , 
    location VARCHAR(500), 
    method VARCHAR(50), 
    page VARCHAR(50), 
    registration FLOAT, 
    session_id VARCHAR(20) ,
    song VARCHAR(300), 
    status INT , 
    ts bigint , 
    user_agent VARCHAR(500), 
    user_id INT 
        )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INT, 
        artist_id VARCHAR(200), 
        artist_latitude FLOAT, 
        artist_longtitude FLOAT,
        artist_location VARCHAR(500), 
        artist_name VARCHAR(200), 
        song_id VARCHAR(100), 
        title VARCHAR(200),
        duration FLOAT, 
        year INT 
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
        songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id varchar NOT NULL,
        level VARCHAR(50) ,
        song_id VARCHAR(100) NOT NULL,
        artist_id VARCHAR(100) NOT NULL,
        session_id VARCHAR NOT NULL,
        location VARCHAR(500) ,
        user_agent VARCHAR(500) )
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users(
        user_id VARCHAR PRIMARY KEY,
        first_name VARCHAR(200) NOT NULL,
        last_name VARCHAR(200) NOT NULL,
        gender VARCHAR(50),
        level VARCHAR(50))
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR(100) PRIMARY KEY,
        title VARCHAR(200),
        artist_id VARCHAR(100), 
        year INT NOT NULL, 
        duration INT NOT NULL)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR(100) PRIMARY KEY,
        name VARCHAR(200), 
        location VARCHAR(500) , 
        lattitude  FLOAT, 
        longtitude FLOAT)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    json {};
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""   
    copy staging_songs from {}
    iam_role {}
    json 'auto';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(
    start_time, 
    user_id, 
    level,
    song_id,
    artist_id, 
    session_id,
    location,
    user_agent)(
    SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, e.user_id, e.level, s.song_id, s.artist_id, e.session_id,e.location, e.user_agent
    FROM staging_events e
    join staging_songs s
    on e.song=s.title and s.artist_name=e.artist_name
    WHERE e.page='NextSong')
""")

user_table_insert = (""" INSERT INTO users(
        user_id,
        first_name,
        last_name,
        gender,
        level)(
        SELECT DISTINCT user_id, first_name, last_name, gender,level 
        FROM staging_events
        WHERE user_id IS NOT NULL AND page='NextSong'
        )
""")

song_table_insert = ("""INSERT INTO songs(
        song_id,
        artist_id,
        title,
        year,
        duration)(
        SELECT DISTINCT song_id, artist_id, title, year, duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
  ) 
    
""")

artist_table_insert = ("""INSERT INTO artists(
        artist_id,
        name,
        location,
        lattitude,
        longtitude)(
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longtitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL)
""")

time_table_insert = ("""INSERT INTO time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
)(SELECT DISTINCT start_time,
    extract(hour from start_time)as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year,
    extract(weekday from start_time) as weekday
    FROM songplays  )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create
                        , staging_songs_table_create
                        , user_table_create
                        , song_table_create
                        , artist_table_create
                        , time_table_create
                        ,songplay_table_create
                       ]
drop_table_queries = [staging_events_table_drop
                      , staging_songs_table_drop
                      , songplay_table_drop
                      , user_table_drop
                      , song_table_drop
                      ,artist_table_drop
                      ,time_table_drop
                     ]
copy_table_queries = [staging_events_copy
                      , staging_songs_copy
                     ]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,songplay_table_insert, time_table_insert
                       ]
