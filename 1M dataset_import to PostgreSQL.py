import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# PostgreSQL Connection Details
db_params = {
     "dbname": "Ecommerce",
    "user": "postgres",
    "password": "henok15",
    "host": "localhost",
    "port": "5432"  # Default PostgreSQL port
}

# Read the CSV file
csv_file = r'C:\Users\Post Lab\Desktop\New folder\E-commerce dataset1.csv'

try:
    df = pd.read_csv(csv_file, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip()  # Remove leading/trailing spaces
    print("CSV loaded successfully!")
    print("CSV Columns:", df.columns.tolist())  # Debug: Check actual column names

    # Ensure 'working_date' exists
    if "working_date" not in df.columns:
        print("Missing column: 'working_date' in CSV. Creating an empty column.")
        df["working_date"] = None  # Add missing column to avoid errors

    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    print("Connected to PostgreSQL successfully!")

    # Define Table Schema (Ensure id is SERIAL and auto-generated)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS eco5 (
        id SERIAL PRIMARY KEY,
        item_id FLOAT,
        status TEXT,
        created_at TIMESTAMP,
        sku TEXT,
        price FLOAT,
        qty_ordered FLOAT,
        grand_total FLOAT,
        increment_id TEXT,
        category_name_1 TEXT,
        sales_commission_code TEXT,
        discount_amount FLOAT,
        payment_method TEXT,
        working_date TIMESTAMP NULL,
        bi_status TEXT,
        mv TEXT,
        year INT,
        month INT,
        customer_since TIMESTAMP,
        my TEXT,
        fy TEXT,
        customer_id FLOAT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Table 'eco5' created successfully (if not exists).")

    # Convert date columns to proper format (only if they exist)
    date_columns = ["created_at", "working_date", "customer_since"]
    for col in date_columns:
        if col in df.columns:  # Only process existing columns
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Define the exact columns for INSERT (excluding 'id' which is auto-generated)
    insert_columns = [
        "item_id", "status", "created_at", "sku", "price", "qty_ordered", "grand_total", 
        "increment_id", "category_name_1", "sales_commission_code", "discount_amount", 
        "payment_method", "working_date", "bi_status", "mv", "year", "month", 
        "customer_since", "my", "fy", "customer_id"
    ]

    # Convert DataFrame to List of Tuples (ensure column order matches insert_columns)
    data_tuples = [tuple(x) for x in df[insert_columns].to_numpy()]

    # Generate the dynamic INSERT query with correct column count
    insert_query = f"""
    INSERT INTO eco5 ({', '.join(insert_columns)}) 
    VALUES %s
    """

    # Insert Data Efficiently
    execute_values(cursor, insert_query, data_tuples)
    
    conn.commit()
    print(f"Successfully imported {len(df)} rows into PostgreSQL!")

except Exception as e:
    print(f"Error importing CSV: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Connection closed.")
