import sqlite3
import pandas as pd

OLD_DB = "customer_service.db"
NEW_DB = "customer_service_v2.db"

def migrate_customers():
    old_conn = sqlite3.connect(OLD_DB)
    new_conn = sqlite3.connect(NEW_DB)

    df = pd.read_sql_query("SELECT * FROM customers", old_conn)

    for _, row in df.iterrows():
        new_conn.execute("""
            INSERT OR IGNORE INTO customers (id, first_name, last_name, email, phone, address_line_1, address_line_2, city, province, postal_code, country)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (row['id'], row['first_name'], row['last_name'], row['email'], row['phone'], row['address_line_1'], row['address_line_2'], row['city'], row['province'], row['postal_code'], row['country']))

    new_conn.commit()
    old_conn.close()
    new_conn.close()
    print("Customers migrated successfully.")

def migrate_orders_and_products():
    old_conn = sqlite3.connect(OLD_DB)
    new_conn = sqlite3.connect(NEW_DB)

    df = pd.read_sql_query("SELECT * FROM orders", old_conn)

    for _, row in df.iterrows():
        # Insert product
        new_conn.execute("INSERT OR IGNORE INTO products (sku) VALUES (?)", (row['product_sku'],))
        product_id = new_conn.execute("SELECT id FROM products WHERE sku = ?", (row['product_sku'],)).fetchone()[0]

        # Insert order
        new_conn.execute("""
            INSERT OR IGNORE INTO orders (id, order_number, customer_id, product_id, quantity, total_amount, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (row['id'], row['order_number'], row['customer_id'], product_id, row['quantity'], row['total_amount'], row['status']))

        # Add status history
        new_conn.execute("INSERT INTO status_history (order_id, status) VALUES (?, ?)", (row['id'], row['status']))

    new_conn.commit()
    old_conn.close()
    new_conn.close()
    print("Orders and products migrated successfully.")

def migrate_shipments():
    old_conn = sqlite3.connect(OLD_DB)
    new_conn = sqlite3.connect(NEW_DB)

    df = pd.read_sql_query("SELECT * FROM shipments", old_conn)

    for _, row in df.iterrows():
        new_conn.execute("""
            INSERT OR IGNORE INTO shipments (id, order_id, canpar_shipment_id, tracking_number, label_path, status, error_details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (row['id'], row['order_id'], row['shipment_id'], row['tracking_number'], row['label_path'], row['status'], row['error_details'], row['timestamp']))

    new_conn.commit()
    old_conn.close()
    new_conn.close()
    print("Shipments migrated successfully.")

def migrate_conversations_and_messages():
    old_conn = sqlite3.connect(OLD_DB)
    new_conn = sqlite3.connect(NEW_DB)

    # Migrate conversations
    df_conv = pd.read_sql_query("SELECT * FROM conversations", old_conn)
    for _, row in df_conv.iterrows():
        new_conn.execute("""
            INSERT OR IGNORE INTO conversations (id, customer_id, order_id, subject, status)
            VALUES (?, ?, ?, ?, ?)
        """, (row['id'], row['customer_id'], row['order_id'], row['subject'], row['status']))

    # Migrate messages
    df_msg = pd.read_sql_query("SELECT * FROM messages", old_conn)
    for _, row in df_msg.iterrows():
        new_conn.execute("""
            INSERT OR IGNORE INTO messages (id, conversation_id, sender, message, is_read, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (row['id'], row['conversation_id'], row['sender'], row['message'], row['is_read'], row['timestamp']))

    new_conn.commit()
    old_conn.close()
    new_conn.close()
    print("Conversations and messages migrated successfully.")


if __name__ == "__main__":
    migrate_customers()
    migrate_orders_and_products()
    migrate_shipments()
    migrate_conversations_and_messages()
    print("Data migration complete.")