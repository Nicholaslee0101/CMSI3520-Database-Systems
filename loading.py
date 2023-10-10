import pandas as pd
import sqlite3

df = pd.read_csv("coinbase-extracted-second.csv")
print(df.head())
print(df.tail())


connection = sqlite3.connect("cryptocurrencies.db")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS cryptocurrencies")
df.to_sql("cryptocurrencies", connection)
connection.close()
