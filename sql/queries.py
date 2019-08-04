
from collections import OrderedDict
query_names = ['artist_insert']

artist_insert = '''INSERT INTO d_artist (artist_id, artist_latitude, artist_location, artist_longitude, artist_name)
SELECT
  artist_id,
  artist_latitude,
  artist_location,
  artist_longitude,
  artist_name
FROM
  song_staging;'''

query_content = [artist_insert]

query = OrderedDict(zip(query_names, query_content))
