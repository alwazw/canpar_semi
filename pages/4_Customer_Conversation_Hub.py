import streamlit as st
import pandas as pd
import database
from datetime import datetime

st.set_page_config(page_title="Customer Conversation Hub", layout="wide")

st.title("Customer Conversation Hub")

conn = database.create_connection()

# Unread Messages
st.subheader("Unread Messages")
unread_messages_df = pd.read_sql_query("""
    SELECT m.id, c.subject, m.sender, m.message, m.created_at
    FROM messages m
    JOIN conversations c ON m.conversation_id = c.id
    WHERE m.is_read = 0
""", conn)

if not unread_messages_df.empty:
    st.dataframe(unread_messages_df, use_container_width=True)

    selected_message_id = st.selectbox("Select a message to mark as read", unread_messages_df['id'])
    if st.button("Mark as Read"):
        conn.execute("UPDATE messages SET is_read = 1 WHERE id = ?", (selected_message_id,))
        conn.commit()
        st.success(f"Message {selected_message_id} marked as read.")
        st.experimental_rerun()
else:
    st.info("No unread messages.")

st.divider()

# View Conversation
st.subheader("View Conversation")
order_numbers = pd.read_sql_query("SELECT order_number FROM orders", conn)['order_number'].tolist()
selected_order_number = st.selectbox("Select an order to view conversation", order_numbers)

if selected_order_number:
    conversation_history_df = pd.read_sql_query(f"""
        SELECT m.sender, m.message, m.created_at
        FROM messages m
        JOIN conversations c ON m.conversation_id = c.id
        JOIN orders o ON c.order_id = o.id
        WHERE o.order_number = '{selected_order_number}'
        ORDER BY m.created_at
    """, conn)

    if not conversation_history_df.empty:
        for _, row in conversation_history_df.iterrows():
            st.chat_message(row['sender']).write(f"**{row['sender']}** ({row['created_at']}): {row['message']}")
    else:
        st.info(f"No conversation history for order {selected_order_number}.")

    # Reply to conversation
    reply_text = st.text_area("Reply to this conversation")
    if st.button("Send Reply"):
        order_id = pd.read_sql_query(f"SELECT id FROM orders WHERE order_number = '{selected_order_number}'", conn).iloc[0, 0]
        conversation_id = pd.read_sql_query(f"SELECT id FROM conversations WHERE order_id = {order_id}", conn).iloc[0, 0]

        message_data = (
            conversation_id,
            'agent',
            reply_text,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            True  # is_read
        )
        conn.execute("INSERT INTO messages (conversation_id, sender, message, created_at, is_read) VALUES (?, ?, ?, ?, ?)", message_data)
        conn.commit()
        st.success("Reply sent.")
        st.experimental_rerun()

conn.close()