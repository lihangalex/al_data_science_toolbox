# pip install psycopg2-binary

from psycopg2 import pool

# Initialize the connection pool
connection_pool = pool.SimpleConnectionPool(
    minconn=1,  # Minimum connections in the pool
    maxconn=10, # Maximum connections in the pool
    user="username",
    password="password",
    host="localhost",
    port="5432",
    database="mydatabase",
)

# Get a connection from the pool
connection = connection_pool.getconn()

try:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM my_table")
        print(cursor.fetchall())
finally:
    # Return the connection to the pool
    connection_pool.putconn(connection)

# Close all connections when done
connection_pool.closeall()

# threaded connection_pooling
from psycopg2 import pool
import threading

# Initialize thread-safe connection pool
thread_safe_pool = pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=10,
    user="username",
    password="password",
    host="localhost",
    port="5432",
    database="mydatabase",
)

def query_database():
    # Fetch a connection from the pool
    connection = thread_safe_pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM my_table")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    finally:
        # Return the connection to the pool
        thread_safe_pool.putconn(connection)

# Example: Using threads to query the database
threads = []
for _ in range(5):
    thread = threading.Thread(target=query_database)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

# Close all connections when done
thread_safe_pool.closeall()
