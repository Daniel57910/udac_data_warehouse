CREATE TABLE f_songplay_temp as 
SELECT
  first_name,
  last_name,
  level,
  artist,
  song_name,
  session_id,
  location,
  user_agent,
  timestamp
FROM
  log_staging;


