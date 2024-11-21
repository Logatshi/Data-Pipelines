class SqlQueries:
    # SQL query to insert records into the songplays table
    songplay_table_insert = ("""
        SELECT
            md5(events.sessionid || events.start_time) AS songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (
            SELECT 
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
                *
            FROM staging_events
            WHERE page='NextSong'
        ) events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    # SQL query to insert unique user records into the users dimension table
    user_table_insert = ("""
        SELECT DISTINCT userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    # SQL query to insert unique song records into the songs dimension table
    song_table_insert = ("""
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    # SQL query to insert unique artist records into the artists dimension table
    artist_table_insert = ("""
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    # SQL query to insert time records into the time dimension table
    time_table_insert = ("""
        SELECT start_time, 
            EXTRACT(hour FROM start_time) AS hour, 
            EXTRACT(day FROM start_time) AS day, 
            EXTRACT(week FROM start_time) AS week, 
            EXTRACT(month FROM start_time) AS month, 
            EXTRACT(year FROM start_time) AS year, 
            EXTRACT(dayofweek FROM start_time) AS weekday
        FROM songplays
    """)