import database
from datetime import datetime
import pandas as pd

def simulate_new_message(customer_id, order_id, message_text):
    """
    Simulates receiving a new message from a customer.
    This function will create a new conversation if one doesn't exist.
    """
    conn = database.create_connection()
    cur = conn.cursor()

    # Check if a conversation already exists for this order
    cur.execute("SELECT id FROM conversations WHERE order_id = ?", (order_id,))
    conversation = cur.fetchone()

    if conversation:
        conversation_id = conversation[0]
    else:
        # Create a new conversation
        subject = f"Inquiry about order {order_id}"
        conversation_data = (customer_id, order_id, subject, "OPEN")
        cur.execute("INSERT INTO conversations (customer_id, order_id, subject, status) VALUES (?, ?, ?, ?)", conversation_data)
        conversation_id = cur.lastrowid

    # Add the new message to the conversation
    message_data = (
        conversation_id,
        'customer',
        message_text,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        False  # is_read
    )
    cur.execute("INSERT INTO messages (conversation_id, sender, message, timestamp, is_read) VALUES (?, ?, ?, ?, ?)", message_data)

    conn.commit()
    conn.close()
    print(f"New message simulated for order {order_id}.")

def get_unread_messages():
    """
    Retrieves all unread messages.
    """
    conn = database.create_connection()
    query = """
        SELECT c.subject, m.sender, m.message, m.timestamp
        FROM messages m
        JOIN conversations c ON m.conversation_id = c.id
        WHERE m.is_read = 0
        ORDER BY m.timestamp
    """
    unread_messages = pd.read_sql_query(query, conn)
    conn.close()
    return unread_messages

def mark_message_as_read(message_id):
    """
    Marks a specific message as read.
    """
    conn = database.create_connection()
    cur = conn.cursor()
    cur.execute("UPDATE messages SET is_read = 1 WHERE id = ?", (message_id,))
    conn.commit()
    conn.close()

def get_conversation_history(order_id):
    """
    Retrieves the full conversation history for a given order.
    """
    conn = database.create_connection()
    query = f"""
        SELECT m.sender, m.message, m.timestamp
        FROM messages m
        JOIN conversations c ON m.conversation_id = c.id
        WHERE c.order_id = {order_id}
        ORDER BY m.timestamp
    """
    conversation_history = pd.read_sql_query(query, conn)
    conn.close()
    return conversation_history

if __name__ == '__main__':
    # Example usage:
    # Simulate a new message for order 1
    simulate_new_message(1, 1, "Hello, I have a question about my order.")

    # Get unread messages
    unread = get_unread_messages()
    print("\nUnread messages:")
    print(unread)

    # Get conversation history for order 1
    history = get_conversation_history(1)
    print("\nConversation history for order 1:")
    print(history)