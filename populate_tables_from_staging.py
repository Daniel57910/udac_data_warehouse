from sql.queries import query

'''
insert queries that don't rely on applicaiton data iterated over to populate 
d_artist, d_song, d_app_user_staging, d_app_user_id
'''

def populate_tables_from_staging(database_wrapper):
  for q in query:
    database_wrapper.execute(q)