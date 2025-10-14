import streamlit as st
import pandas as pd
import database

st.set_page_config(page_title="Order Management", layout="wide")

st.title("Order Management")

conn = database.create_connection()

# Search and filter
search_term = st.text_input("Search by Order Number, Customer Name, or Product SKU")

query = """
    SELECT o.order_number, c.first_name || ' ' || c.last_name as customer_name, p.sku, o.status, o.total_amount, o.order_date
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    JOIN products p ON o.product_id = p.id
"""

if search_term:
    query += f" WHERE o.order_number LIKE '%{search_term}%' OR customer_name LIKE '%{search_term}%' OR p.sku LIKE '%{search_term}%'"

orders_df = pd.read_sql_query(query, conn)

st.dataframe(orders_df, use_container_width=True)

st.divider()

# View Order Details
st.subheader("View Order Details")
selected_order = st.selectbox("Select an order to view details", orders_df['order_number'])

if selected_order:
    order_details_query = f"""
        SELECT o.order_number, c.*, p.*, s.*
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN products p ON o.product_id = p.id
        LEFT JOIN shipments s ON o.id = s.order_id
        WHERE o.order_number = '{selected_order}'
    """
    order_details = pd.read_sql_query(order_details_query, conn)
    if not order_details.empty:
        st.write(order_details.iloc[0])

conn.close()