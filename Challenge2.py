import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import SQL_connection
import sqlalchemy
import pymysql


def dms_to_decimal(dms_str):
    # Extract degrees, minutes, seconds, and direction
    pattern = r'(\d+)°(\d+)′(\d+)″([NSEW])'
    match = re.search(pattern, dms_str)

    if match:
        degrees = int(match.group(1))
        minutes = int(match.group(2))
        seconds = int(match.group(3))
        direction = match.group(4)

        # Calculate decimal degrees
        decimal = degrees + minutes / 60 + seconds / 3600

        # Make negative if South or West
        if direction in ['S', 'W']:
            decimal = -decimal

        return decimal

    # Handle simpler format like "53°33′N"
    pattern_simple = r'(\d+)°(\d+)′([NSEW])'
    match = re.search(pattern_simple, dms_str)

    if match:
        degrees = int(match.group(1))
        minutes = int(match.group(2))
        direction = match.group(3)

        # Calculate decimal degrees
        decimal = degrees + minutes / 60

        # Make negative if South or West
        if direction in ['S', 'W']:
            decimal = -decimal

        return decimal

    return None

def get_population(soup):
    population_year = soup.find(string="Population").find_next("div").get_text()
    # Remove brackets ()
    match = re.search(r'\((.*?)\)', population_year)
    if match:
        population_year = match.group(1)

    # Filter for year
    match = re.search(r'\b(1\d{3}|2\d{3})\b', population_year)
    if match:
        population_year = match.group(0)

    population = soup.find(string="Population").find_next("td").get_text()
    population = population.replace(",", "")
    population = population.replace(".", "")
    return population, population_year

def get_info(soup):
    try:
        city_name = soup.find(class_="mw-page-title-main").get_text()
        country = soup.find(string=re.compile("Country", re.IGNORECASE)).find_next().get_text()
        latitude = soup.find(class_="latitude").get_text()
        longitude = soup.find(class_="longitude").get_text()
        population, population_year = get_population(soup)
        df = pd.DataFrame({
            "city_name": [city_name],
            "country": [country],
            "latitude": [latitude],
            "longitude": [longitude],
            'Population': [population],
            'Population_year': [population_year]
        })
        # convert to decimal
        df['latitude_decimal'] = df['latitude'].apply(dms_to_decimal)
        df['longitude_decimal'] = df['longitude'].apply(dms_to_decimal)
        return df
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        return pd.DataFrame(columns=["city_name", "country", "latitude", "longitude"])

def crawl_data(names):
    cities_df = pd.DataFrame(columns=["city_name", "country", "latitude", "longitude", "Population", "Population_year"])
    for name in names:
        url = "https://en.wikipedia.org/wiki/" + name
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        cities_df = pd.concat([cities_df, get_info(soup)], ignore_index=True)

    return cities_df

def push_to_my_sql(df, table_name, connection_string = SQL_connection.get_sql_connection()):
    df.to_sql(table_name,
           if_exists='append',
           con=connection_string,
           index=False)

city_names_df = ["Berlin", "Hamburg", "Munich", "Santiago", "Paris", "Beijing", "New_York_City"]
cities_df = crawl_data(city_names_df)

connection_string = SQL_connection.get_sql_connection()

# drop duplicate countries and push them to the countries sql table
unique_countries = cities_df[['country']].drop_duplicates()
push_to_my_sql(unique_countries, 'countries')

# drop duplicate cities and push them to the cities sql table
unique_cities = cities_df[['city_name']].drop_duplicates()
push_to_my_sql(unique_cities, 'cities')

# pull the newly pushed sql tables to get the ID's
countries_from_sql = pd.read_sql("countries", con=connection_string)
cities_from_sql = pd.read_sql("cities", con=connection_string)

# create a new table with the ID's and remove the unecessary columns from it
cities_df_sqled = cities_df.merge(countries_from_sql,
                                  on="country",
                                  how="left")
cities_df_sqled = cities_df_sqled.merge(cities_from_sql,
                                  on="city_name",
                                  how="left")
cities_df_sqled = cities_df_sqled.drop(columns=["city_name", "country", "latitude", "longitude"])

# Push the updated table to the city_data SQL table
push_to_my_sql(cities_df_sqled, 'city_data')

