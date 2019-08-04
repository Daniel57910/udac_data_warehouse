
from collections import OrderedDict
query_names = ['artist_insert', 'song_insert', 'app_user_insert']

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

app_user_insert = '''INSERT INTO d_app_user (app_user_id, first_name, last_name, gender, level, timestamp)
SELECT  
  CAST(user_id AS INT),
  first_name,
  last_name,
  gender,
  level,
  timestamp
FROM
  log_staging;
'''

query_content = [artist_insert, song_insert, app_user_insert]
query = OrderedDict(zip(query_names, query_content))


