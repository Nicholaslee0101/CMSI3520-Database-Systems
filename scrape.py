import mechanicalsoup
import pandas as pd
import io
import sqlite3

url = "https://en.wikipedia.org/wiki/Fortune_500"
browser = mechanicalsoup.StatefulBrowser()
browser.open(url)
table = browser.page.find("table", {"class": "wikitable"})
html_content = str(table)
data_frame = pd.read_html(io.StringIO(html_content))[0]
data_frame = data_frame.head(20)
print(data_frame)

column_names = ["Rank", "Company", "State", "Industry", "Revenue in USD"]

connection = sqlite3.connect("fortune500.db")

cursor = connection.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS fortune500_top20 ("
               "Rank INT, "
               "Company TEXT, "
               "State TEXT, "
               "Industry TEXT, "
               "\"Revenue in USD\" REAL"
               ")")

for index, row in data_frame.iterrows():
    cursor.execute("INSERT INTO fortune500_top20 VALUES (?, ?, ?, ?, ?)", tuple(row))

connection.commit()
connection.close()
