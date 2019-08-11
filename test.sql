
INSERT INTO f_songplay
  (start_time, user_id, level, artist_key, song_key, session_id, location, user_agent)
VALUES
  (1541903636796,
    (select app_user_key
    from d_app_user
    where first_name = 'Anabelle' and last_name = 'Simpson'),
    'Free',
    (select artist_key
    from d_artist
    where artist_name = 'Frumpies'),
    (select song_key
    from d_song
    where title = 'Fuck Kitty'),
    455,
    'Blabla',
    'Bla3Bla'
    );