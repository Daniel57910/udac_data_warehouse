import psycopg2
from psycopg2.extras import execute_batch

class DatabaseWrapper:

  def __init__(self, conn_string):

    try:
      self.conn = psycopg2.connect(conn_string)
      self.conn.autocommit=True
      self.cursor = self.conn.cursor()
    except:
      print('Unable to connect to the DB')
      exit(1)

  def execute(self, query):
    
    try:
      self.cursor.execute(query)
    except Exception as e:
      print('Unable to execute query')
      print(e)
      exit(1)


