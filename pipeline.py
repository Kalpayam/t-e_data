# I have data for hourly observations of emissions for 100 unique container ships for the year 2019. I also have a list of major ports in the world. I intend to present analysis on maritime CO2 emissions around ports.
import pandas as pd
import sqlite3

# Extract
ships = pd.read_csv("2019_AIS.csv")
ports = pd.read_csv("major_ports.csv")

# Transform
ships['LATITUDE'] = ships['LATITUDE'].astype(str).str.replace(',', '.').astype(float)
ships['LONGITUDE'] = ships['LONGITUDE'].astype(str).str.replace(',', '.').astype(float)


# The following function can be used to find the distance between ships and ports. I will use this to filter emissions occured close to ports. 
from geopy.distance import geodesic

def calculate_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).kilometers

# The data for ships has almost one million entries and filtering those that are close to ports by itterating a distance function would be extremely time-consuming.
# To solve this problem, I define a box around each observed ship position and use the greater than function which is a query and is significantly cheaper in computation terms. 
# 0.3 degrees of longitude and 0.2 degrees of latitude roughly correspond to 12 nautical miles which is the UN defined range for territorial waters (UNCLOS).
ships['ports_in_range_indexes'] = None

for index, row in ships.iterrows():
    lng_distance = 0.3
    lat_distance = 0.2
    condition1 = ports['Lng'] > row['LONGITUDE'] - lng_distance
    condition2 = ports['Lng'] < row['LONGITUDE'] + lng_distance
    condition3 = ports['Lat'] > row['LATITUDE'] - lat_distance
    condition4 = ports['Lat'] < row['LATITUDE'] + lat_distance

    result_indexes = ports[condition1 & condition2 & condition3 & condition4].index
    if result_indexes.empty:
        continue
    else:
        ships.at[index, 'ports_in_range_indexes'] = result_indexes.tolist()

# Next, I filter the observations where ships where close to ports.
ships_around_ports = ships[ships['ports_in_range_indexes'].apply(lambda x: x is not None)]


# I can now use the distance function defined earlier on and run a code with for loops applied on a smaller dataset. 
matches = []

for index, ship in ships_around_ports.iterrows():
    lat_ship = float(ship['LATITUDE'])
    lon_ship = float(ship['LONGITUDE'])
    min_distance = float('inf') 
    closest_port_index = None

    for value in ship['ports_in_range_indexes']:
        lat_port = float(ports.loc[value, 'Lat'])
        lon_port = float(ports.loc[value, 'Lng'])
        distance = calculate_distance(lat_ship, lon_ship, lat_port, lon_port)

        if distance < min_distance:
            min_distance = distance
            closest_port_index = value

    ships.at[index, 'closest_port'] = ports.loc[closest_port_index, 'Postal']
    ships.at[index, 'continent'] = ports.loc[closest_port_index, 'REGION']
    ships.at[index, 'distance_to_closest_port'] = min_distance
        
        
# Convert lists to strings
ships['ports_in_range_indexes'] = ships_around_ports['ports_in_range_indexes'].apply(lambda x: str(x))        

      
        
# Load
con = sqlite3.connect("dwh/db")


ships.to_sql('ships', con, if_exists='replace', index=False)
ports.to_sql("ports", con, if_exists="replace")
