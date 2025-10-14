import argparse
import pandas as pd
from tabulate import tabulate

import database
from customer_service import get_unread_messages, get_conversation_history, simulate_new_message
from shipping import import_orders_from_csv, process_new_orders


def main():
    parser = argparse.ArgumentParser(description="Customer Service Module")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- Importer Commands ---
    parser_import = subparsers.add_parser("import-orders", help="Import new orders from the CSV file.")
    parser_process = subparsers.add_parser("process-shipments", help="Process new orders and generate shipping labels.")

    # --- Customer Service Commands ---
    parser_unread = subparsers.add_parser("view-unread", help="View all unread messages.")
    parser_conversation = subparsers.add_parser("view-conversation", help="View the conversation history for a specific order.")
    parser_conversation.add_argument("order_id", type=int, help="The ID of the order to view the conversation for.")

    # --- Simulation Command ---
    parser_simulate = subparsers.add_parser("simulate-message", help="Simulate a new message from a customer.")
    parser_simulate.add_argument("customer_id", type=int, help="The ID of the customer.")
    parser_simulate.add_argument("order_id", type=int, help="The ID of the order.")
    parser_simulate.add_argument("message", type=str, help="The message text.")

    args = parser.parse_args()

    if args.command == "import-orders":
        import_orders_from_csv()
        print("Orders imported successfully.")

    elif args.command == "process-shipments":
        process_new_orders()
        print("Shipment processing complete.")

    elif args.command == "view-unread":
        unread_messages = get_unread_messages()
        if not unread_messages.empty:
            print(tabulate(unread_messages, headers='keys', tablefmt='psql'))
        else:
            print("No unread messages.")

    elif args.command == "view-conversation":
        conversation_history = get_conversation_history(args.order_id)
        if not conversation_history.empty:
            print(tabulate(conversation_history, headers='keys', tablefmt='psql'))
        else:
            print(f"No conversation history found for order ID: {args.order_id}")

    elif args.command == "simulate-message":
        simulate_new_message(args.customer_id, args.order_id, args.message)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()