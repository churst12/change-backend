import psycopg2
from flask import Flask


def dbconnect():
	# Connect to the database.
	conn = psycopg2.connect(
	    user='chhurst',
	    password='TheFour',
	    host='gcp-us-west2.change-db.crdb.io',
	    port=26257,
	    database='defaultdb',
	    sslmode='require',
	    sslrootcert='/Users/collinhurst/Documents/code/change/change-db-ca.crt'
	)

	# Make each statement commit immediately.
	conn.set_session(autocommit=True)
	return conn

def testdb(cur):

	# Create an "accounts" table.
	cur.execute("show tables;")

	rows = cur.fetchall()
	result = ""
	for row in rows:
	    result += "\n" + str(row)
	return result
	
def closedb():
	# Close the database connection.
	cur.close()
	conn.close()


conn = dbconnect()
cur = conn.cursor()




#booty
app = Flask(__name__)
@app.route('/')
def hello_world():
	s = testdb(cur)

	return s

if __name__ == '__main__':
	app.run()
