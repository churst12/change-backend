import psycopg2
from flask import Flask
import math
import numpy as np
import uuid
import json
from datetime import datetime

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
    with make.cursor() as cur:
        cur.execute("INSERT INTO transactions values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (ID, userID, storeID, store_name, store_loc, user_loc, store_to_person, cash_amount, change_amount, time, receipt))
        if store_to_person:
            cur.execute("UPDATE users SET balance = balance + %s WHERE id = %s", (change_amount, str(userID)))
            cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (change_amount, str(storeID)))
        else:
            cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (change_amount, str(storeID)))
            cur.execute("UPDATE users SET balance = balance + %s WHERE id = %s", (change_amount, str(userID)))

def create_transfer(make, ID, userID, amount, time, person_to_bank, account_id, routing_number, bank):
    with make.cursor() as cur:
        cur.execute("INSERT INTO transfers values(%s, %s, %s, %s, %s, %s, %s, %s)", (str(ID), str(userID), amount, time, person_to_bank, account_id, routing_number, bank))
        if person_to_bank:
            cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (amount, str(userID)))
        else:
            cur.execute("UPDATE users SET balance = balance + %s WHERE id = %s", (amount, str(userID)))

# Execute the transaction.
#create_user(conn, "astarr1997", "8568574617","Alex Starr", 10.87, json.dumps({"lat": "37.865259", "long": "-122.251892", "Address": "2514 Piedmont Avenue Berkeley CA"}), "astarr1997@berkeley.edu", \
#                                               "6507145032", (str(uuid.uuid4()), "12345678", "Bank of the West Berkeley", True, True), True)
#create_user(conn, "Walmart", "000000000", "Walmart", 10000.00, json.dumps({"lat": "39.125124", "long": "-120.243523", "Address": "1400 Shattuck Avenue Berkeley CA"}), "walmart@gmail.com", \
#                                               "NA", (str(uuid.uuid4()), "12345678", "Walmart Bank", True, True), True)
#create_transaction(conn, str(uuid.uuid4()), '5e52a2f9-2e37-4699-94c9-165aa38a7271','1f56c45d-a707-4aa4-8f5a-7b3edf627a67', "Walmart", \
#                   json.dumps({"lat": "39.125124", "long": "-120.243523", "Address": "1400 Shattuck Avenue Berkeley CA"}), json.dumps({"lat": "37.865259", "long": "-122.251892", "Address": "2514 Piedmont Avenue Berkeley CA"}), \
#                              datetime.now(), True, 0.73, 10, "bananas")
#create_transfer(conn, uuid.uuid4(), '0d1986e1-4fdf-479d-bedb-6412975a09c4', 3.45, datetime.now(), True, str(uuid.uuid4()), "12851234", "Chase")

def get_user(userID):
    todo = "SELECT * FROM users WHERE id = '" + str(userID) + "'"
    cur.execute(todo)
    rows = cur.fetchall()
    newrows = []
    for i in range(len(rows)):
        row = list(rows[i])
        row[3] = float(row[3])
        newrows.append(row)
    return json.dumps(newrows)

def get_transa(transaID):
    todo = "SELECT * FROM transactions WHERE id = '" + str(transaID) + "'"
    cur.execute(todo)
    rows = cur.fetchall()
    newrows = []
    for i in range(len(rows)):
        row = list(rows[i])
        row[7] = float(row[7])
        row[8] = float(row[8])
        row[9] = str(row[9])
        newrows.append(row)
    return json.dumps(newrows)
    

def get_account(accountID):
    todo = "SELECT * FROM bank_accounts WHERE id = '" + str(accountID) + "'"
    cur.execute(todo)
    rows = cur.fetchall()
    return json.dumps(rows)

def get_account_user(userID):
    todo = "SELECT * FROM bank_accounts WHERE user_id = '" + str(userID) + "'"
    cur.execute(todo)
    rows = cur.fetchall()
    return json.dumps(rows)

def get_transf(transfID):
    todo = "SELECT * FROM transfers WHERE id = '" + str(transfID) + "'"
    cur.execute(todo)
    rows = cur.fetchall()
    newrows = []
    for i in range(len(rows)):
        row = list(rows[i])
        row[2] = float(row[2])
        row[3] = str(row[3])
        newrows.append(row)
    return json.dumps(newrows)

def get_user_transa(userID):
    todo = "SELECT * FROM transactions WHERE user_id = '" + str(userID) + "'"
    cur.execute(todo)
    rows = cur.fetchall()
    newrows = []
    for i in range(len(rows)):
        row = list(rows[i])
        row[7] = float(row[7])
        row[8] = float(row[8])
        row[9] = str(row[9])
        newrows.append(row)
    return json.dumps(newrows)

def get_store_transa(storeID):
    todo = "SELECT * FROM transactions WHERE store_id = '" + str(storeID) + "'"
    cur.execute(todo)
    rows = cur.fetchall()
    newrows = []
    for i in range(len(rows)):
        row = list(rows[i])
        row[7] = float(row[7])
        row[8] = float(row[8])
        row[9] = str(row[9])
        newrows.append(row)
    return json.dumps(newrows)
#print(get_user('5e52a2f9-2e37-4699-94c9-165aa38a7271'))
#print(get_account('0d1986e1-4fdf-479d-bedb-6412975a09c4'))
#print(get_transa('aee16de7-3dab-4018-87b4-a5041a8963a1'))
#print(get_transf('11f51d1a-bb61-4080-9e34-b54da97ad5b3'))
#print(get_user_transa('5e52a2f9-2e37-4699-94c9-165aa38a7271'))
#print(get_store_transa('1f56c45d-a707-4aa4-8f5a-7b3edf627a67'))
#print(get_account_user('5e52a2f9-2e37-4699-94c9-165aa38a7271'))
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

def displayaccs():
	cur.execute("select * from bank_accounts")
	rows = cur.fetchall()
	result = ""
	for row in rows:
	    result += "\n" + str(row)
	return result

def displaytransfs():
	cur.execute("select * from transfers")
	rows = cur.fetchall()
	result = ""
	for row in rows:
	    result += "\n" + str(row)
	return result

def displaytransas():
    cur.execute("select * from transactions")
    rows = cur.fetchall()
    newrows = []
    for i in range(len(rows)):
        row = list(rows[i])
        row[7] = float(row[7])
        row[8] = float(row[8])
        row[9] = str(row[9])
        newrows.append(row)
    return json.dumps(newrows)


#cur.execute("DELETE FROM transactions")
#cur.execute("DELETE FROM transfers")
#cur.execute("DELETE FROM bank_accounts")
#cur.execute("DELETE FROM users")
#print(displayusers())
#print(displayaccs())
#print(displaytransfs())
#print(displaytransas())



"""#booty
app = Flask(__name__)
@app.route('/')
def hello_world():
	s = displayusers()

	return s

if __name__ == '__main__':
	app.run()"""
