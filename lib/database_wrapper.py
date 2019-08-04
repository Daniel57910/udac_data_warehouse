import psycopg2
from psycopg2.extras import execute_batch
import pdb

class DatabaseWrapper:

  def __init__(self, conn_string):

    try:
      self.conn = psycopg2.connect(conn_string)
      self.conn.autocommit=True
      self.cursor = self.conn.cursor()
    except:
      print('Unable to connect to the DB')

  def execute(self, query):
    
    try:
      self.cursor.execute(query)
    except Exception as e:
      print(f'Unable to execute query {query}: {e}')
  
  def select(self, query):
    try:
      self.execute(query)
      return self.cursor.fetchall()
    except Exception as e:
      print(f'Unable to execute {query}: {e}')


