import psycopg2
from flask import Flask
import math
import numpy as np
import uuid
import json

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

conn = dbconnect()
cur = conn.cursor()



def create_bank_account(make, ID, userID, routing_number, bank_name, active, primary):
    with make.cursor() as cur:
        cur.execute("INSERT INTO bank_accounts values(%s, %s, %s, %s, %s, %s)", (ID, userID, routing_number, bank_name, active, primary))

def create_user(make, username, password, name, balance, location, email, phone, account_info, sub_1):
    with make.cursor() as cur:
        userID = str(uuid.uuid4())
        create_bank_account(make, account_info[0], userID, account_info[1], account_info[2], account_info[3], account_info[4])
        account_info_json = json.dumps({"id":account_info[0]})
        cur.execute("INSERT INTO users values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (username, password, name, balance, location, email, phone, account_info_json,userID, sub_1))

def create_transaction(make, ID, userID, storeID, store_name, store_loc, user_loc, time, store_to_person, change_amount, cash_amount, receipt):
    with make.cursor as cur:
        cur.execute("INSERT INTO transactions values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (ID, userID, storeID, store_name, store_loc, user_loc, time, store_to_person, change_amount, cash_amount, receipt))
        if store_to_person:
            cur.execute("UPDATE users SET balance = balance + %s WHERE id = " + str(userID), (change_amount))
            cur.execute("UPDATE users SET balance = balance - %s WHERE id = " + str(storeID), (change_amount))
        else:
            cur.execute("UPDATE users SET balance = balance - %s WHERE id = " + str(userID), (change_amount))
            cur.execute("UPDATE users SET balance = balance + %s WHERE id = " + str(storeID), (change_amount))
def create_transfer(make, ID, userID, amount, time, person_to_bank, account_id, routing_number, bank):
    pass
# The transaction we want to run.
def transfer_funds(txn, frm, to, amount):
    with txn.cursor() as cur:

        # Check the current balance.
        cur.execute("SELECT balance FROM accounts WHERE id = " + str(frm))
        from_balance = cur.fetchone()[0]
        if from_balance < amount:
            raise "Insufficient funds"

        # Perform the transfer.
        cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s",
                    (amount, frm))
        cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s",
                    (amount, to))

# Execute the transaction.
create_user(conn, "astarr1997", "8568574617","Alex Starr",10.87, json.dumps({"lat": "37.865259", "long": "-122.251892", "Address": "2514 Piedmont Avenue Berkeley CA"}), "astarr1997@berkeley.edu", \
                                               "6507145032", (str(uuid.uuid4()), "12345678", "Bank of the West Berkeley", True, True), True)




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

def displayusers():
	cur.execute("select * from users")

	rows = cur.fetchall()
	result = ""
	for row in rows:
	    result += "\n" + str(row)
	return result





#booty
app = Flask(__name__)
@app.route('/')
def hello_world():
	s = displayusers()

	return s

if __name__ == '__main__':
	app.run()
