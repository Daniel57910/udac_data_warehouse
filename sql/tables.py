song_staging_table = '''CREATE TABLE IF NOT EXISTS song_staging
(artist_id TEXT not null,
artist_latitude NUMERIC,
artist_location TEXT,
artist_longitude NUMERIC,
artist_name TEXT not null,
duration NUMERIC,
num_songs INT,
song_id TEXT not null,
title TEXT not null,
year INT);'''

song_insert_query = '''INSERT INTO song_staging VALUES
(%s, %s, %s, %s, %s, %s, %s, %s, %s);'''


table_commands = ['DROP TABLE IF EXISTS song_staging;']
create_all_tables = [song_staging_table]
table_commands.extend(create_all_tables)