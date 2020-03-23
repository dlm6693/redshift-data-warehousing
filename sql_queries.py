import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
                                CREATE TABLE staging_events(
                                    artist VARCHAR(255),
                                    auth VARCHAR(10),
                                    firstName VARCHAR(25),
                                    gender VARCHAR(1),
                                    iteminSession INTEGER,
                                    lastName VARCHAR(25),
                                    length NUMERIC,
                                    level VARCHAR(4),
                                    location VARCHAR(255),
                                    method VARCHAR(3),
                                    page VARCHAR(20),
                                    registration NUMERIC,
                                    sessionId INTEGER,
                                    song VARCHAR(255),
                                    status INTEGER,
                                    ts BIGINT,
                                    userAgent TEXT,
                                    userId INTEGER)
                                """)

staging_songs_table_create = ("""
                                CREATE TABLE staging_songs (
                                    num_songs INTEGER,
                                    artist_id TEXT,
                                    artist_name VARCHAR(MAX),
                                    artist_latitude NUMERIC,
                                    artist_longitude NUMERIC,
                                    artist_location TEXT,
                                    song_id text,
                                    title text,
                                    duration numeric,
                                    year int)
                                """)

songplay_table_create = ("""
                            CREATE TABLE songplays(
                                songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
                                start_time TIMESTAMP NOT NULL SORTKEY,
                                user_id INTEGER NOT NULL REFERENCES users(user_id),
                                level VARCHAR(4),
                                song_id VARCHAR(18) NOT NULL REFERENCES songs(song_id),
                                artist_id VARCHAR(18) NOT NULL REFERENCES artists(artist_id),
                                session_id INTEGER,
                                location VARCHAR(MAX),
                                user_agent TEXT)
                            """)

user_table_create = ("""
                        CREATE TABLE users(
                            user_id INTEGER PRIMARY KEY DISTKEY,
                            first_name VARCHAR(25),
                            last_name VARCHAR(25) SORTKEY,
                            gender VARCHAR(1),
                            level VARCHAR(4))
                        """)

song_table_create = ("""
                        CREATE TABLE songs(
                            song_id VARCHAR(18) PRIMARY KEY,
                            title VARCHAR(MAX) NOT NULL,
                            artist_id VARCHAR(18) NOT NULL DISTKEY REFERENCES artists(artist_id),
                            year INTEGER NOT NULL SORTKEY,
                            duration NUMERIC NOT NULL)
                        """)

artist_table_create = ("""
                        CREATE TABLE artists(
                            artist_id VARCHAR(18) PRIMARY KEY DISTKEY,
                            name VARCHAR(MAX) SORTKEY,
                            location VARCHAR(MAX),
                            latitude NUMERIC,
                            longitude NUMERIC)
                        """)

time_table_create = ("""
                    CREATE TABLE time(
                        start_time TIMESTAMP PRIMARY KEY,
                        hour INTEGER,
                        day INTEGER,
                        week INTEGER,
                        month INTEGER,
                        year INTEGER,
                        weekday INTEGER)
                    """)

# STAGING TABLES

staging_events_copy = (f"""
                        COPY staging_events FROM {config['S3']['LOG_DATA']}
                        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
                        REGION 'us-west-2'
                        COMPUPDATE OFF
                        FORMAT AS JSON {config['S3']['LOG_JSONPATH']}
                        ;
""")

staging_songs_copy = (f"""
                        COPY staging_songs FROM {config['S3']['SONG_DATA']}
                        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
                        JSON 'auto'
                        REGION 'us-west-2';
""")

# FINAL TABLES

songplay_table_insert = ("""
                        INSERT INTO songplays(
                            start_time, 
                            user_id, 
                            level, 
                            song_id, 
                            artist_id, 
                            session_id, 
                            location, 
                            user_agent)
                        SELECT 
                            TIMESTAMP 'epoch' + e.ts/1000*INTERVAL '1 second' as start_time, 
                            e.userId as user_id, 
                            e.level, 
                            s.song_id, 
                            s.artist_id, 
                            e.sessionId as session_id, 
                            e.location, 
                            e.userAgent as user_agent
                        FROM staging_events e
                        JOIN staging_songs s
                        ON s.artist_name = e.artist AND s.title = e.song AND s.duration = e.length
                        WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
                    INSERT INTO users(
                        user_id, 
                        first_name, 
                        last_name,
                        gender, 
                        level)
                    SELECT DISTINCT 
                        userId as user_id, 
                        firstName as first_name, 
                        lastName as last_name, 
                        gender, 
                        level
                    FROM staging_events
                    WHERE page = 'NextSong'
""")

song_table_insert = ("""
                    INSERT INTO songs(
                        song_id, 
                        title, 
                        artist_id,
                        year, 
                        duration)
                    SELECT DISTINCT
                        song_id, 
                        title, 
                        artist_id, 
                        year, 
                        duration
                    FROM staging_songs
""")

artist_table_insert = ("""
                        INSERT INTO artists(
                            artist_id, 
                            name, 
                            location, 
                            latitude, 
                            longitude)
                        SELECT DISTINCT 
                            artist_id, 
                            artist_name as name, 
                            artist_location as location, 
                            artist_latitude as latitude, 
                            artist_longitude as longitude
                        FROM staging_songs
""")

time_table_insert = ("""
                    INSERT INTO time(
                        start_time, 
                        hour, 
                        day, 
                        week, 
                        month, 
                        year, 
                        weekday)
                    SELECT DISTINCT
                        start_time, 
                        EXTRACT(HOUR FROM start_time) as hour, 
                        EXTRACT(DAY FROM start_time) as day, 
                        EXTRACT(WEEK FROM start_time) as week, 
                        EXTRACT(MONTH From start_time) as month, 
                        EXTRACT(YEAR FROM start_time) as year, 
                        EXTRACT(DOW FROM start_time) as weekday
                    FROM songplays
""")

# QUERY LISTS

create_table_queries = [
                        staging_events_table_create, 
                        staging_songs_table_create, 
                        artist_table_create, user_table_create, 
                        time_table_create, song_table_create, 
                        songplay_table_create
                                            ]

drop_table_queries = [
                    staging_events_table_drop, 
                    staging_songs_table_drop, 
                    songplay_table_drop, 
                    user_table_drop, 
                    song_table_drop, 
                    artist_table_drop, 
                    time_table_drop
                                ]

copy_table_queries = [
                    staging_events_copy, 
                    staging_songs_copy
                                        ]

insert_table_queries = [
                        songplay_table_insert, 
                        user_table_insert, 
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert
                                        ]
