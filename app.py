import psycopg2

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

# Open a cursor to perform database operations.
cur = conn.cursor()

# Create an "accounts" table.
cur.execute("show tables;")

rows = cur.fetchall()

for row in rows:
    print(row)

# Close the database connection.
cur.close()
conn.close()