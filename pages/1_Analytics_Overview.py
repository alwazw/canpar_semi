import streamlit as st
import pandas as pd
import database

st.set_page_config(page_title="Analytics Overview", layout="wide")

st.title("Analytics Overview")

conn = database.create_connection()

# Key Metrics
total_orders = pd.read_sql_query("SELECT COUNT(id) FROM orders", conn).iloc[0, 0]
total_revenue = pd.read_sql_query("SELECT SUM(total_amount) FROM orders", conn).iloc[0, 0]
successful_shipments = pd.read_sql_query("SELECT COUNT(id) FROM shipments WHERE status = 'SUCCESS'", conn).iloc[0, 0]
failed_shipments = pd.read_sql_query("SELECT COUNT(id) FROM shipments WHERE status != 'SUCCESS'", conn).iloc[0, 0]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Orders", total_orders)
col2.metric("Total Revenue", f"${total_revenue:,.2f}")
col3.metric("Successful Shipments", successful_shipments)
col4.metric("Failed Shipments", failed_shipments)

st.divider()

# Sales Trends
st.subheader("Sales Trends")
sales_by_date = pd.read_sql_query("SELECT DATE(order_date) as date, SUM(total_amount) as daily_sales FROM orders GROUP BY date ORDER BY date", conn)
if not sales_by_date.empty:
    st.line_chart(sales_by_date.set_index('date'))

# Shipments by Status
st.subheader("Shipments by Status")
shipment_status = pd.read_sql_query("SELECT status, COUNT(id) as count FROM shipments GROUP BY status", conn)
if not shipment_status.empty:
    st.bar_chart(shipment_status.set_index('status'))

conn.close()