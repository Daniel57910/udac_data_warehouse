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

d_artist_table = '''CREATE TABLE IF NOT EXISTS d_artist
(artist_key BIGINT identity(0, 1),
artist_id TEXT not null,
artist_latitude NUMERIC,
artist_location TEXT,
artist_longitude TEXT,
artist_name TEXT not null,
PRIMARY KEY(artist_key))
COMPOUND SORTKEY(artist_key, artist_id, artist_name);'''

d_song_table = '''CREATE TABLE IF NOT EXISTS d_song
(song_key BIGINT identity(0, 1),
song_id TEXT,
title TEXT,
duration NUMERIC,
year INT,
artist_id TEXT REFERENCES d_artist(artist_key),
PRIMARY KEY(song_key))
COMPOUND SORTKEY(artist_id, title, year);'''

d_app_user_table_staging = '''CREATE TABLE IF NOT EXISTS d_app_user_staging
(app_user_key BIGINT identity(0, 1),
app_user_id INT,
first_name TEXT,
last_name TEXT,
gender TEXT,
level TEXT,
timestamp BIGINT,
PRIMARY KEY(app_user_id))
COMPOUND SORTKEY(app_user_id, timestamp);'''

d_app_user_table = '''CREATE TABLE IF NOT EXISTS
d_app_user(
app_user_key BIGINT identity(0, 1),
app_user_id INT,
first_name TEXT,
last_name TEXT,
gender TEXT,
level TEXT,
PRIMARY KEY(app_user_id));'''

d_timestamp_table = '''CREATE TABLE IF NOT EXISTS
d_timestamp(
timestamp_key BIGINT identity(0, 1),
year INT, 
month INT,
day INT,
minute INT,
second INT,
hour INT,
weekday BOOL
);'''

table_commands = [
  'DROP TABLE IF EXISTS song_staging;', 
  'DROP TABLE IF EXISTS log_staging;', 
  'DROP TABLE IF EXISTS d_artist;', 
  'DROP TABLE IF EXISTS d_song;',
  'DROP TABLE IF EXISTS d_app_user_staging;',
  'DROP TABLE IF EXISTS d_app_user;',
  'DROP TABLE IF EXISTS d_app_user_id;',
  'DROP TABLE IF EXISTS d_timestamp;'
]

table_commands.reverse()
create_all_tables = [song_staging_table, log_staging_table, d_artist_table, d_song_table, d_app_user_table_staging, d_app_user_table, d_timestamp_table]
table_commands.extend(create_all_tables)


