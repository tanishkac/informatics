import pandas as pd
import sqlite3

df_csv = pd.read_csv("data/orders.csv")
df_json = pd.read_json("data/users.json")

conn = sqlite3.connect(':memory:')
with open('data/restaurants.sql', 'r') as f:
    sql_script = f.read()
try:
    conn.executescript(sql_script) 
    print("SQL script executed successfully.")
except Exception as e:
    print(f"Error running SQL script: {e}")
table_name = "restaurants"
try:
    df_sql = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    print("Data loaded into Pandas:")
    print(df_sql.head())
except Exception as e:
    print(f"Could not read table: {e}")
conn.close()

orders_with_users = pd.merge(df_csv, df_json, on='user_id', how='left')

final_df = pd.merge(orders_with_users, df_sql, on='restaurant_id', how='left')


print("\nMerge Complete!! ")
print(final_df.head())

final_df.to_csv("data/ final_food_delivery_dataset.csv", index=False)
print("\nSaved to data/merged_data.csv")