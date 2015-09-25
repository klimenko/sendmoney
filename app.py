# coding=utf-8
import webapp2
import os
import MySQLdb
import json
#import sys

_INSTANCE_NAME = 'sendmoneyapi:money'

#create table accounts (name varchar(255) not null, balance int not null, primary key (name))
#create table history (id int auto_increment, sender varchar(255), receiver varchar(255), sum int, action int, time timestamp default current_timestamp, primary key (id))

def get_connector():
  if (os.getenv('SERVER_SOFTWARE') and
    os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
    db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='money', user='root')
  else:
    db = MySQLdb.connect(host='localhost', user='root')
  return db

class IndexPage(webapp2.RequestHandler):
  def get(self):
    
    self.response.headers['Content-Type'] = 'text/plain'

    try:
      p_pretty = bool(self.request.get('pretty'))
    except:
      p_pretty = False
    
    connector = get_connector()
    cursor = connector.cursor()

    cursor.execute('SELECT * FROM accounts')
    
    if p_pretty:
      self.response.write(json.dumps(cursor.fetchall(), indent=4))
    else:
      self.response.write(json.dumps(cursor.fetchall()))

    connector.close()

class Send(webapp2.RequestHandler):
  def get(self):
    
    self.response.headers['Content-Type'] = 'text/plain'
    
    try:
      p_from = self.request.get('from')
      p_to = self.request.get('to')
      p_sum = int(self.request.get('sum'))
    except:
      self.response.write('error: 1')
      return

    if p_sum <= 0:
      self.response.write('error: 2')
      return

    connector = get_connector()
  
    try:
      connector.begin()
      cursor = connector.cursor()
      cursor.execute("SELECT name, balance FROM accounts WHERE name = '%s' or name = '%s' FOR UPDATE" % (p_from, p_to)); #лочим сразу обе, чтобы исключить дедлоки для случая обратного списания в параллельной сессии
      rows = cursor.fetchall()
      if len(rows) != 2:
        self.response.write('error: 3')
        connector.rollback()
      else:
        ok = rows[0][0] == p_from and rows[0][1] >= p_sum  or  rows[1][0] == p_from and rows[1][1] >= p_sum
        if ok: 
          cursor.execute("UPDATE accounts SET balance = balance - %d WHERE name = '%s'" % (p_sum, p_from))
          cursor.execute("UPDATE accounts SET balance = balance + %d WHERE name = '%s'" % (p_sum, p_to))
          cursor.execute("INSERT INTO history (sender, receiver, sum, action) VALUES('%s', '%s', %d, 2)" % (p_from, p_to, p_sum))
          connector.commit();
          self.response.write('ok')
        else:
          connector.rollback()
          self.response.write('error: 4')
    except:
      #self.response.write(sys.exc_info())
      self.response.write('error: 4')

    connector.close()
    
class Create(webapp2.RequestHandler):
  def get(self):
    
    self.response.headers['Content-Type'] = 'text/plain'
    
    try:
      p_name = self.request.get('name')
      p_sum = int(self.request.get('sum'))
    except:
      self.response.write('error: 1')
      return

    if p_name == '':
      self.response.write('error: 2')
      return

    if p_sum < 0:
      self.response.write('error: 3')
      return
 
    connector = get_connector()
  
    try:
      connector.begin()
      cursor = connector.cursor()
      cursor.execute("INSERT INTO accounts (name, balance) VALUES ('%s', %d)" % (p_name, p_sum))
      cursor.execute("INSERT INTO history (receiver, sum, action) VALUES ('%s', %d, 1)" % (p_name, p_sum))
      connector.commit()
      self.response.write('ok')
    except:
      self.response.write('error: 4')
      #self.response.write(sys.exc_info())

    connector.close()

class Log(webapp2.RequestHandler):
  def get(self):
    
    self.response.headers['Content-Type'] = 'text/plain'

    try:
      p_pretty = bool(self.request.get('pretty'))
    except:
      p_pretty = False

    connector = get_connector()
    cursor = connector.cursor()

    cursor.execute('SELECT sender, receiver, sum, time FROM history')

    rows = [list(r) for r in cursor.fetchall()]
    for row in rows:
      row[3] = str(row[3])
    
    if p_pretty:
      self.response.write(json.dumps(rows, indent=4))
    else:
      self.response.write(json.dumps(rows))

    connector.close()


app = webapp2.WSGIApplication([('/', IndexPage), ('/send', Send), ('/create', Create), ('/log', Log)], debug=True)