import sqlite3
from sqlite3 import Error

DATABASE_NAME = "customer_service_v2.db"

def create_connection():
    """ create a database connection to the SQLite database
        specified by DATABASE_NAME
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        conn.row_factory = sqlite3.Row
        return conn
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def initialize_database():
    """
    Create the database and the necessary tables.
    """
    sql_create_customers_table = """ CREATE TABLE IF NOT EXISTS customers (
                                        id integer PRIMARY KEY,
                                        first_name text NOT NULL,
                                        last_name text NOT NULL,
                                        email text UNIQUE,
                                        phone text,
                                        address_line_1 text,
                                        address_line_2 text,
                                        city text,
                                        province text,
                                        postal_code text,
                                        country text,
                                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                                    ); """

    sql_create_products_table = """ CREATE TABLE IF NOT EXISTS products (
                                        id integer PRIMARY KEY,
                                        sku text NOT NULL UNIQUE,
                                        brand text,
                                        category text,
                                        description text
                                    ); """

    sql_create_orders_table = """CREATE TABLE IF NOT EXISTS orders (
                                    id integer PRIMARY KEY,
                                    order_number text NOT NULL UNIQUE,
                                    customer_id integer NOT NULL,
                                    product_id integer NOT NULL,
                                    quantity integer,
                                    unit_price real,
                                    total_amount real,
                                    commission real,
                                    taxes real,
                                    status text DEFAULT 'NEW',
                                    order_date DATETIME,
                                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    FOREIGN KEY (customer_id) REFERENCES customers (id),
                                    FOREIGN KEY (product_id) REFERENCES products (id)
                                );"""

    sql_create_shipments_table = """CREATE TABLE IF NOT EXISTS shipments (
                                    id integer PRIMARY KEY,
                                    order_id integer NOT NULL,
                                    canpar_shipment_id text,
                                    tracking_number text,
                                    label_path text,
                                    status text,
                                    error_details text,
                                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    FOREIGN KEY (order_id) REFERENCES orders (id)
                                );"""

    sql_create_conversations_table = """CREATE TABLE IF NOT EXISTS conversations (
                                        id integer PRIMARY KEY,
                                        customer_id integer NOT NULL,
                                        order_id integer,
                                        subject text,
                                        status text DEFAULT 'OPEN',
                                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                                        FOREIGN KEY (customer_id) REFERENCES customers (id),
                                        FOREIGN KEY (order_id) REFERENCES orders (id)
                                    );"""

    sql_create_messages_table = """CREATE TABLE IF NOT EXISTS messages (
                                    id integer PRIMARY KEY,
                                    conversation_id integer NOT NULL,
                                    sender text, -- 'customer' or 'agent'
                                    message text,
                                    is_read boolean DEFAULT 0,
                                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    FOREIGN KEY (conversation_id) REFERENCES conversations (id)
                                );"""

    sql_create_status_history_table = """CREATE TABLE IF NOT EXISTS status_history (
                                            id integer PRIMARY KEY,
                                            order_id integer NOT NULL,
                                            status text NOT NULL,
                                            notes text,
                                            changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                                            FOREIGN KEY (order_id) REFERENCES orders (id)
                                        );"""

    conn = create_connection()

    if conn is not None:
        create_table(conn, sql_create_customers_table)
        create_table(conn, sql_create_products_table)
        create_table(conn, sql_create_orders_table)
        create_table(conn, sql_create_shipments_table)
        create_table(conn, sql_create_conversations_table)
        create_table(conn, sql_create_messages_table)
        create_table(conn, sql_create_status_history_table)
        conn.close()
        print("Database initialized successfully.")
    else:
        print("Error! cannot create the database connection.")

def add_status_update(conn, order_id, status, notes=""):
    """Adds a status update to the history table."""
    sql = '''INSERT INTO status_history (order_id, status, notes) VALUES (?, ?, ?)'''
    cur = conn.cursor()
    cur.execute(sql, (order_id, status, notes))
    conn.commit()
    return cur.lastrowid

if __name__ == '__main__':
    initialize_database()