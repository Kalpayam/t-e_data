import pandas as pd
import sqlite3

# Extract
ships = pd.read_csv("2019_AIS.csv")
ports = pd.read_csv("major_ports.csv")
ships.info()

# Transform
ships['LATITUDE'] = ships['LATITUDE'].astype(str).str.replace(',', '.').astype(float)
ships['LONGITUDE'] = ships['LONGITUDE'].astype(str).str.replace(',', '.').astype(float)

# Load
con = sqlite3.connect("dwh/db")

ships.to_sql("ships", con, if_exists="replace")
ports.to_sql("ports", con, if_exists="replace")
