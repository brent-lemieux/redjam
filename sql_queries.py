import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

AWS_REGION = 'us-west-2'

IAM_ARN = config.get("IAM_ROLE", "ARN")

SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSON_PATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist VARCHAR(100),
        auth VARCHAR(15),
        firstName VARCHAR(20),
        gender VARCHAR(1),
        itemInSession INT,
        lastName VARCHAR(20),
        length NUMERIC,
        level VARCHAR(4),
        location VARCHAR(60),
        method VARCHAR(3),
        page VARCHAR(20),
        registration BIGINT,
        session_id INT,
        song VARCHAR(200),
        status INT,
        ts BIGINT,
        userAgent VARCHAR(200),
        userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INT,
        artist_id VARCHAR(30),
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR(200),
        artist_name VARCHAR(200),
        song_id VARCHAR(25),
        title VARCHAR(200),
        duration NUMERIC,
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
        start_time BIGINT NOT NULL SORTKEY,
        user_id INT NOT NULL,
        level VARCHAR(4) NOT NULL,
        song_id VARCHAR(25) NOT NULL DISTKEY,
        artist_id VARCHAR(30) NOT NULL,
        session_id INT NOT NULL,
        location VARCHAR(60),
        user_agent VARCHAR(200)
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INT NOT NULL PRIMARY KEY,
        first_name VARCHAR(20),
        last_name VARCHAR(20),
        gender VARCHAR(1),
        level VARCHAR(4)
    )
    DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR(25) NOT NULL PRIMARY KEY DISTKEY,
        title VARCHAR(200) NOT NULL,
        artist_id VARCHAR(30) NOT NULL,
        year INT NOT NULL,
        duration NUMERIC NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR(50) NOT NULL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        location VARCHAR(200),
        latitude NUMERIC,
        longitude NUMERIC
    )
    DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time BIGINT NOT NULL PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    IAM_ROLE {}
    JSON {} maxerror as 250;
""").format(LOG_DATA, IAM_ARN, LOG_JSON_PATH)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    IAM_ROLE {}
    COMPUPDATE OFF REGION '{}'
    JSON 'auto' TRUNCATECOLUMNS;
""").format(SONG_DATA, IAM_ARN, AWS_REGION)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT e.ts AS start_time, e.userId AS user_id, e.level, s.song_id,
        s.artist_id, e.session_id, e.location, e.userAgent as user_agent
    FROM staging_events e
    INNER JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name)
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT e.userId as user_id, e.firstName as first_name,
        e.lastName as last_name, e.gender, e.level
    FROM staging_events e
    INNER JOIN (select userId, max(ts) as max_ts
                from staging_events
                group by userId) me
    ON (e.userId = me.userId AND e.ts = me.max_ts)
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT s.artist_id, s.artist_name as name,
        s.artist_location as location, s.artist_latitude as latitude,
        s.artist_longitude as longitude
    FROM staging_songs s
    INNER JOIN (SELECT artist_id, max(year) as max_year
                FROM staging_songs
                GROUP BY artist_id) ms
    ON (s.artist_id = ms.artist_id AND s.year = ms.max_year)
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts as start_time,
        EXTRACT(hour from (timestamp 'epoch' + ts/1000 *INTERVAL '1 second')) as hour,
        EXTRACT(day from (timestamp 'epoch' + ts/1000 *INTERVAL '1 second' )) as day,
        EXTRACT(week from (timestamp 'epoch' + ts/1000 *INTERVAL '1 second' )) as week,
        EXTRACT(month from (timestamp 'epoch' + ts/1000 *INTERVAL '1 second' )) as month,
        EXTRACT(year from (timestamp 'epoch' + ts/1000 *INTERVAL '1 second' )) as year,
        CASE WHEN EXTRACT(dow from (timestamp 'epoch' + ts/1000 *INTERVAL '1 second' )) BETWEEN 1 AND 5 THEN 1 ELSE 0 END as weekday
    FROM staging_events
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, staging_songs_table_create,
    songplay_table_create, user_table_create, song_table_create,
    artist_table_create, time_table_create
]

drop_table_queries = [
    staging_events_table_drop, staging_songs_table_drop,
    songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop,
    time_table_drop
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert, user_table_insert, song_table_insert,
    artist_table_insert, time_table_insert
]
