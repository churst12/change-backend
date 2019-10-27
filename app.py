import psycopg2
from flask import Flask, request
import math
import numpy as np
import uuid
import json
from datetime import datetime
from os.path import abspath, join, dirname

import random
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

VIPs = {'Alex': '5e52a2f9-2e37-4699-94c9-165aa38a7271', 'Darren': 'aee16de7-3dab-4018-87b4-a5041a8963a1', 'Vaughn': '1f56c45d-a707-4aa4-8f5a-7b3edf627a67'}
STOREs = {'Walmart': 'b7acdf48-cb62-4cf5-8389-61d361fa727f', 'Ikes': 'a391ef1f-c9e3-404b-9acb-76387592e61f', 'Starbucks': '9ecd2e44-a07c-4309-a4f2-1e4615fce016'}

def create_bank_account(make, ID, userID, routing_number, bank_name, active, primary):
    with make.cursor() as cur:
        cur.execute("INSERT INTO bank_accounts values(%s, %s, %s, %s, %s, %s)", (ID, userID, routing_number, bank_name, active, primary))

def create_user(make, username, password, name, balance, location, email, phone, account_info, sub_1, userID):
    with make.cursor() as cur:
        create_bank_account(make, account_info[0], userID, account_info[1], account_info[2], account_info[3], account_info[4])
        account_info_json = json.dumps({"id":account_info[0]})
        cur.execute("INSERT INTO users values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (username, password, name, balance, location, email, phone, account_info_json, userID, sub_1))

def create_transaction(make, userID, storeID, store_name, store_loc, user_loc, time, store_to_person, change_amount, cash_amount, receipt):
    with make.cursor() as cur:
        ID = str(uuid.uuid4())
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
#                                              "6507145032", (str(uuid.uuid4()), "12345678", "Bank of the West Berkeley", True, True), True)
#create_user(conn, "dawsoncreek", "carrying","Darren Dawson", 19.64, json.dumps({"lat": "33.847259", "long": "-119.963392", "Address": "111 Rose Drive Santa Cruz CA"}), "dwdawson@ucsc.edu", \
#                                              "6504657891", (str(uuid.uuid4()), "45674321", "Chase Bank Mountain View", True, True), True)
#create_user(conn, "wrathofvaughn", "mobileman","Vaughn Fisher", 21.14, json.dumps({"lat": "34.3435678", "long": "-120.123321", "Address": "417 Daisy Drive Santa Cruz CA"}), "vgfisher@ucsc.edu", \
#                                              "6503213456", (str(uuid.uuid4()), "19283746", "Chase Bank Mountain View", True, True), True)
#create_user(conn, "Walmart", "toystorey","Walmart", 215432.19, json.dumps({"lat": "35.123563", "long": "-122.986543", "Address": "1 Walmart Drive San Franscisco CA"}), "walmart@walmart.com", \
#                                              "4178964532", (str(uuid.uuid4()), "38749058", "Walmart Bank", True, True), True)
#create_user(conn, "Ikes", "sandwich", "Ikes", 16581.13, json.dumps({"lat": "34.536417", "long": "-118.145342", "Address": "1449 Shattuck Avenue Berkeley CA"}), "ikes@gmail.com", \
#                                              "5104935678", (str(uuid.uuid4()), "14142323", "Bank of America Berkeley", True, True), True)
#create_user(conn, "Starbucks", "coffee", "Starbucks", 122813.17, json.dumps({"lat": "35.125432", "long": "-120.435671", "Address": "Everywhere"}), "starbs@gmail.com", \
#                                              "5102314634", (str(uuid.uuid4()), "88899900", "Bank of America Berkeley", True, True), True)        
#create_user(conn, "Walmart", "000000000", "Walmart", 10000.00, json.dumps({"lat": "39.125124", "long": "-120.243523", "Address": "1400 Shattuck Avenue Berkeley CA"}), "walmart@gmail.com", \
#                                               "NA", (str(uuid.uuid4()), "12345678", "Walmart Bank", True, True), True)
#create_transaction(conn, str(uuid.uuid4()), '5e52a2f9-2e37-4699-94c9-165aa38a7271', 'a391ef1f-c9e3-404b-9acb-76387592e61f', "Ikes", \
#                   json.dumps({"lat": "34.536417", "long": "-118.145342", "Address": "1449 Shattuck Avenue Berkeley CA"}), json.dumps({"lat": "37.865259", "long": "-122.251892", "Address": "2514 Piedmont Avenue Berkeley CA"}), \
#                              datetime.now(), False, 0.11, 11.00, "Matt Cain")
#create_transfer(conn, uuid.uuid4(), 'aee16de7-3dab-4018-87b4-a5041a8963a1', 2.00, datetime.now(), True, str(uuid.uuid4()), "34564379", "Chase")
firsts = ['Alex', 'Bill', 'James', 'Juan', 'Pierre', 'Barney', 'Homer', 'Jake', 'Billy', 'William', 'Jim', 'Jimmy', 'John', 'Saul', 'Walter', 'Ted', 'Marshall', 'Evan', 'Phil', 'Joaquin', 'Daquan', 'Aaron', 'Adam', 'Collin', 'Lee', 'Tim', 'Matt', 'Timothy', 'Ricky', 'Rick', 'Richard', 'Matthew', 'Matty', 'Deron', 'Derek', 'Darren', 'Ferris', 'Kevin', 'Tod', 'Leigh', 'Abigail', 'Mary', 'Alexandra', 'Robin', 'Lily', 'Cynthia', 'Shayda', 'Stephanie', 'Kiana', 'Sam', 'Samuel', 'Samantha', 'Samantha', 'Annie', 'Beth', 'Bethanie', 'Jiwoon', 'Sheila', 'Ariana', 'Natalie', 'Tiffany', 'Jen', 'Jennnifer', 'Emma', 'Genivieve', 'Lady', 'Paul', 'Paula', 'Selena']
lasts = ['Smith', 'Denhow', 'Moir', 'Dawson', 'Beth', 'Kross', 'Smulders', 'Starr', 'Ko', 'Li', 'Lee', 'Fisher', 'Francis', 'Huang', 'Hurst', 'Hiroki', 'Edwards', 'Elder', 'Sheeran', 'Grande', 'Lorenzo', 'Bruhn', 'Denero', 'Hilfinger', 'Paulin', 'Romanov', 'Stark', 'Rogers', 'Bunny', 'Duck', 'Orange', 'Arkin', 'Malkovich', 'Stinson', 'Mosby', 'Gomez', 'Toy', 'Merlo', 'Adams', 'Wollstonecraft', 'Daddario', 'Aldrich', 'Aldrin', 'Moir', 'White', 'Hosaka', 'Mozer', 'Toben', 'Runke', 'Rubenstein', 'Goldman', 'Fraser', 'Steiner', 'Huang']
add = ['Lane', 'Street', 'Avenue', 'Way', 'Circle']
Banks = ["Chase", "Bank of the West", "Bank of America", "Bundesbanke", "Bank of Spain", "Bank of China", "Loyola Bank", "Bank of England"]
cities = ["San Francisco", "Oakland", "Mountain View", "Palo Alto", "Santa Cruz", "Ashville", "Los Altos", "Homestead", "Fremont", "San Jose", "Los Angeles", "Pierre", "Santa Barbara", "San Diego"]
items = ["Crayons", "Antifreeze", "Dolls", "Chocolate", "Protein Powder", "M&Ms", "DVDs", "Jam", "Cider", "Lawn Chair", "My Little Pony", "Pencils", "Pens", "Erasers", "Coffee", "Shirts", "Jeans", "Shorts", "Jacket", "Sweatshirt", "Raincoat", "Photos", "Picture Frame", "Root Beer", "Sprite", "Diet Coke", "Diet Coke Cherry", "Madden", "Starcraft II", "Dog food", "Cat Food"]
drinks = ["pumpkin spice latte", "latte", "cappucino", "black coffee", "frappucino", "macchiato"]
sandwiches = ["Spiffy Tiffy", "Damon Bruce", "Matt Cain", "Adam Richman", "Pocohontas", "Kryptonite", "Higgins", "Paul Reuben", "Patrick Marlow"]
"""def generate_db():
    for i in range(0, 10000):
        first = random.choice(firsts)
        last = random.choice(lasts)
        username = first + last
        password = str(math.ceil(np.random.random()*10000000))
        name = first + " " + last 
        balance = math.ceil(np.random.random()*10000)/100
        location = json.dumps({"lat": str(random.randint(25, 40) + math.ceil(np.random.random()*1000000)/1000000), "long": str(-1*random.randint(118, 124) + math.ceil(np.random.random()*1000000)/1000000), "Address": str(math.ceil(np.random.random()*1000)) + " " + random.choice(lasts) + " " + random.choice(add) + " " + random.choice(cities) + " " + "CA"})
        email = first + last + "@gmail.com"
        phone = str(math.ceil(np.random.random()*10000000000))
        account_info = (str(uuid.uuid4()), str(math.ceil(np.random.random()*100000000)), random.choice(Banks), True, True)
        sub_1 = True
        userID = str(uuid.uuid4())
        create_user(conn, username, password, name, balance, location, email, phone, account_info, sub_1, userID)
        create_transaction(conn, str(uuid.uuid4()), userID, 'b7acdf48-cb62-4cf5-8389-61d361fa727f', "Walmart", json.dumps({"lat": "39.125124", "long": "-120.243523", "Address": "Everywhere"}), location, datetime.now(), True, math.ceil(np.random.random()*100)/100, math.ceil(np.random.random()*100)/5, random.choice(items) + " " + random.choice(items))
        create_transaction(conn, str(uuid.uuid4()), userID, '9ecd2e44-a07c-4309-a4f2-1e4615fce016', "Starbucks", json.dumps({"lat": "35.125432", "long": "-120.435671", "Address": "Everywhere"}), location, datetime.now(), True, math.ceil(np.random.random()*100)/100, math.ceil(np.random.random()*10), random.choice(drinks))
        if i % 10 == 0:
            create_transaction(conn, str(uuid.uuid4()), userID, 'a391ef1f-c9e3-404b-9acb-76387592e61f', "Ikes", json.dumps({"lat": "34.536417", "long": "-118.145342", "Address": "1449 Shattuck Avenue Berkeley CA"}), location, datetime.now(), True, math.ceil(np.random.random()*100)/100, 11, random.choice(sandwiches))
        
generate_db()"""
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
print(get_user('5e52a2f9-2e37-4699-94c9-165aa38a7271'))
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
    cur.execute("SELECT * FROM users")
    rows = cur.fetchall()
    newrows = []
    for i in range(len(rows)):
        row = list(rows[i])
        row[3] = float(row[3])
        newrows.append(row)
    return json.dumps(newrows)

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



#booty
app = Flask(__name__)
@app.route('/')
def hello_world():
	s = displayusers()
	return s

#Get all transactions for a user.
@app.route('/get/transactions/user')
def get_user_transall():
    user_id = request.args.get('userID')
    s = get_user_transa(user_id)
    return s

#Get all transactions for a store.
@app.route('/get/transactions/store')
def get_store_transall():
    store_id = request.args.get('storeID')
    s = get_store_transa(store_id)
    return s

#Get all transactions for god mode.
@app.route('/get/transactions')
def get_transall():
    s = displaytransas()
    return s

#Get profile for a user.
@app.route('/get/user/profile')
def get_user_profile():
    user_id = request.args.get('userID')
    s = get_user(user_id)
    return s

#Get profile for a user.
@app.route('/get/user/bankaccount')
def get_user_bank_account():
    user_id = request.args.get('userID')
    s = get_account_user(user_id)
    return s

# create a transaction post method 
@app.route('/do/transaction', methods=['POST'])
def real_post():
    s = request.form
    create_transaction(conn, s['storeID'], s['store_name'], s['store_loc'], s['user_loc'], datetime.datetime(), s['store_to_person'], s['change_amount'], s['cash_amount'], s['receipt'])
    return "success"

if __name__ == '__main__':
	app.run()
