import streamlit as st
import pandas as pd
import database

st.set_page_config(page_title="Shipment Tracking", layout="wide")

st.title("Shipment Tracking")

conn = database.create_connection()

# Failed Shipments
st.subheader("Failed Shipments")
failed_shipments_df = pd.read_sql_query("""
    SELECT o.order_number, s.status, s.error_details, s.created_at
    FROM shipments s
    JOIN orders o ON s.order_id = o.id
    WHERE s.status != 'SUCCESS'
""", conn)

if not failed_shipments_df.empty:
    st.dataframe(failed_shipments_df, use_container_width=True)

    # Reprocess failed shipments
    selected_shipment = st.selectbox("Select a shipment to reprocess", failed_shipments_df['order_number'])
    if st.button("Reprocess Shipment"):
        st.write(f"Reprocessing shipment for order {selected_shipment}...")
        # This would call the shipping.py logic to reprocess the failed shipment
        # For now, we'll just display a message
        st.success(f"Reprocessing initiated for order {selected_shipment}.")
else:
    st.info("No failed shipments to display.")

st.divider()

# All Shipments
st.subheader("All Shipments")
all_shipments_df = pd.read_sql_query("""
    SELECT o.order_number, s.status, s.tracking_number, s.label_path, s.created_at
    FROM shipments s
    JOIN orders o ON s.order_id = o.id
""", conn)

st.dataframe(all_shipments_df, use_container_width=True)

conn.close()