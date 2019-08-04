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

log_staging_table = '''CREATE TABLE IF NOT EXISTS log_staging
(artist TEXT,
auth TEXT,
first_name TEXT,
gender TEXT,
item_in_session INT, 
last_name TEXT,
length NUMERIC,
level TEXT,
location TEXT,
method TEXT,
page TEXT,
registration NUMERIC,
session_id INT,
song_name TEXT,
status INT,
timestamp BIGINT,
user_agent TEXT,
user_id TEXT);'''

artist_table = '''CREATE TABLE IF NOT EXISTS d_artist
(artist_key BIGINT identity(0, 1),
artist_id TEXT not null,
artist_latitude NUMERIC,
artist_location TEXT,
artist_longitude TEXT,
artist_name TEXT not null,
PRIMARY KEY(artist_key))
COMPOUND SORTKEY(artist_key, artist_id, artist_name);'''

song_insert_query = '''INSERT INTO song_staging VALUES
(%s, %s, %s, %s, %s, %s, %s, %s, %s);'''


table_commands = ['DROP TABLE IF EXISTS song_staging;', 'DROP TABLE IF EXISTS log_staging;', 'DROP TABLE IF EXISTS d_artist;']
create_all_tables = [song_staging_table, log_staging_table, artist_table]
table_commands.extend(create_all_tables)


