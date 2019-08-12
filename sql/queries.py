
'''
list of insert queries executed directly via sql
stored in query list
application_insert_queries dict require loading data from table to memory then reinsertion
select queries used to select data from DB to be manipulated prior to transfer to dimension table
'''

artist_insert = '''INSERT INTO d_artist (artist_id, artist_latitude, artist_location, artist_longitude, artist_name)
SELECT
  artist_id,
  artist_latitude,
  artist_location,
  artist_longitude,
  artist_name
FROM
  song_staging;'''

song_insert = '''INSERT INTO d_song(song_id, title, duration, year, artist_id)
SELECT
  song_id,
  title,
  duration,
  year,
  artist_id
FROM
  song_staging;'''

app_user_staging_insert = '''INSERT INTO d_app_user_staging (app_user_id, first_name, last_name, gender, level, timestamp)
SELECT  
  CAST(user_id AS INT),
  first_name,
  last_name,
  gender,
  level,
  timestamp
FROM
  log_staging;'''

app_user_id_insert = '''CREATE TEMP TABLE d_app_user_id AS SELECT app_user_id FROM d_app_user_staging;'''

query = [artist_insert, song_insert, app_user_staging_insert, app_user_id_insert]

select_queries = {}

application_insert_queries = {}

select_queries['distinct_app_user'] = '''SELECT DISTINCT * from d_app_user_id;'''

select_queries['ordered_app_user'] = '''SELECT TOP 1 app_user_id, first_name, last_name, gender, level
   FROM d_app_user_staging
  WHERE
    app_user_id = {}
  ORDER BY timestamp desc
  ;'''

select_queries['songplay_fetch'] = '''SELECT first_name, last_name, level, artist, song_name, artist, session_id, location, user_agent, timestamp FROM log_staging;'''

select_queries['timestamp_query'] = '''SELECT timestamp from log_staging;'''

application_insert_queries['songplay_insert'] = '''INSERT INTO f_songplay 
    (user_id, level, artist_key, song_key, session_id, location, user_agent, start_time) VALUES (
    (select top 1 app_user_key from d_app_user where first_name = %s and last_name = %s order by app_user_id DESC),
    %s,
    (select top 1 artist_key from d_artist where artist_name = %s),
    (select song_key from d_song where title = %s and artist_id = (SELECT top 1 artist_id from d_artist where artist_name = %s)),
    %s,
    %s,
    %s,
    %s
    );'''

application_insert_queries['insert_app_user'] = '''INSERT INTO d_app_user (app_user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s);'''

