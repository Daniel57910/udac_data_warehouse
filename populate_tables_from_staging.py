from sql.queries import query

def populate_tables_from_staging(database_wrapper):
  for q in query:
    database_wrapper.execute(q)