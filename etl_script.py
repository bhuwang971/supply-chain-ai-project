import pandas as pd
import sqlite3

# Connect to database
conn = sqlite3.connect('olist.db')

# Dictionary of CSV files and their corresponding table names
datasets = {
    'olist_orders_dataset.csv': 'orders',
    'olist_products_dataset.csv': 'products',
    'olist_sellers_dataset.csv': 'sellers',
    'product_category_name_translation.csv': 'product_category_translation',
    'olist_geolocation_dataset.csv': 'geolocation',
    'olist_order_items_dataset.csv': 'order_items',
    'olist_order_payments_dataset.csv': 'order_payments',
    'olist_order_reviews_dataset.csv': 'order_reviews',
    'olist_customers_dataset.csv': 'customers'
}

# Load each CSV into SQLite
for csv_file, table_name in datasets.items():
    df = pd.read_csv(csv_file)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Loaded {csv_file} into {table_name} table.")

# Clean and transform data
conn.execute("UPDATE products SET product_category_name = 'Unknown' WHERE product_category_name IS NULL;")
conn.execute("ALTER TABLE orders ADD COLUMN order_year TEXT;")
conn.execute("UPDATE orders SET order_year = strftime('%Y', order_purchase_timestamp);")

# Close connection
conn.close()
print("ETL process completed.")