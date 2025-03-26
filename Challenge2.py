import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import SQL_connection
import sqlalchemy
import pymysql
from config import WEATHER_API_KEY, FLIGHTS_API_KEY
from datetime import datetime, timezone
import dateutil.parser


def convert_dates_for_sql(df, date_columns=None, target_format='%Y-%m-%d %H:%M:%S', to_utc=True):
    """
    Convert date strings in various formats to SQL-compatible format.

    Parameters:
    -----------
    df : pandas.DataFrame
        The dataframe containing date columns
    date_columns : list, optional
        List of column names to process. If None, will attempt to detect date columns.
    target_format : str, optional
        The target date format for SQL
    to_utc : bool, optional
        Whether to convert all dates to UTC time

    Returns:
    --------
    pandas.DataFrame
        DataFrame with converted date columns
    """
    # Create a copy to avoid modifying the original
    result_df = df.copy()

    # If date_columns not provided, try to detect them
    if date_columns is None:
        date_columns = []
        date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')

        for col in df.columns:
            # Skip non-string columns
            if df[col].dtype != 'object':
                continue

            # Check if column has date-like strings
            sample_values = df[col].dropna().head(10).astype(str)
            if any(date_pattern.search(val) for val in sample_values):
                date_columns.append(col)

    # Process each date column
    for col in date_columns:
        result_df[col] = result_df[col].apply(
            lambda x: format_date_for_sql(x, target_format, to_utc) if pd.notna(x) else x
        )

    return result_df


def format_date_for_sql(date_string, target_format='%Y-%m-%d %H:%M:%S', to_utc=True):
    """
    Parse a date string with timezone and convert to SQL format.

    Parameters:
    -----------
    date_string : str
        The date string to convert
    target_format : str
        The target date format for SQL
    to_utc : bool
        Whether to convert to UTC time

    Returns:
    --------
    str
        Formatted date string for SQL
    """
    try:
        # Parse the date string with timezone information
        dt = dateutil.parser.parse(date_string)

        # Convert to UTC if required
        if to_utc and dt.tzinfo is not None:
            dt = dt.astimezone(datetime.timezone.utc)

        # Format to target format (removing timezone info)
        return dt.strftime(target_format)
    except (ValueError, TypeError):
        # Return original value if parsing fails
        return date_string

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

def get_weather_data(lat, lon):
    weather_json = requests.get(
        f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}&units=metric")
    weather_json = weather_json.json()

    # Create empty lists for collecting data
    dates = []
    temperatures = []
    weather_descriptions = []
    rain_values = []

    # Extract data from each forecast element
    for element in weather_json['list']:
        dates.append(element['dt_txt'])
        temperatures.append(element['main']['temp'])
        weather_descriptions.append(element['weather'][0]['description'])

        # Check if rain data exists and extract it
        if 'rain' in element and '3h' in element['rain']:
            rain_values.append(element['rain']['3h'])
        else:
            rain_values.append(0)

    # Create DataFrame from the collected data
    df = pd.DataFrame({
        'date': dates,
        'temperature': temperatures,
        'weather': weather_descriptions,
        'rain': rain_values
    })

    return df

def push_weather_to_sql(connection_string = SQL_connection.get_sql_connection()):
    cities_df = pd.read_sql('city_data', con=connection_string)

    # List to collect all weather data
    all_weather_data = []
    # Iterate through city rows
    for index, city in cities_df.iterrows():
        try:
            lat = city['latitude_decimal']
            lon = city['longitude_decimal']

            # Get weather data for this city
            city_weather = get_weather_data(lat, lon)

            # Add city_id to each weather row
            city_weather['city_id'] = city['city_id']

            # Append to our list of DataFrames
            all_weather_data.append(city_weather)

            print(f"Successfully retrieved weather data for city ID {city['city_id']}")

        except Exception as e:
            print(f"Error retrieving weather data for city ID {city['city_id']}: {str(e)}")

    # Check if we have any successful weather data
    if all_weather_data:
        # Combine all weather data into one DataFrame
        weather_df = pd.concat(all_weather_data, ignore_index=True)

        # Reorder columns to put city_id first
        weather_df = weather_df[['city_id', 'date', 'temperature', 'weather', 'rain']]

        print(f"Total weather entries collected: {len(weather_df)}")
        print(weather_df.head())

        # Save to database
        push_to_my_sql(weather_df, 'weather_data')
    else:
        print("No weather data was collected.")


def get_flights_data(connection_string = SQL_connection.get_sql_connection()):
    cities_df = pd.read_sql('city_data', con=connection_string)

    querystring = {"withFlightInfoOnly": "true"}
    headers = {
        "X-RapidAPI-Key": FLIGHTS_API_KEY,
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
    }

    # List to collect all weather data
    all_airports = pd.DataFrame()

    for index, city in cities_df.iterrows():
        try:
            lat = city['latitude_decimal']
            lon = city['longitude_decimal']
            url = f"https://aerodatabox.p.rapidapi.com/airports/search/location/{lat}/{lon}/km/50/16"
            response = requests.get(url, headers=headers, params=querystring)

            if response.status_code == 200:
                data = response.json()

                # Get the keys from the first item (if available)
                if data.get('items') and len(data.get('items')) > 0:
                    # Print the keys of the first item
                    first_item_keys = data['items'][0].keys()

                # Convert JSON to DataFrame for this city
                airports = pd.json_normalize(data.get('items', []))

                # Add the city_id to every row in the airports DataFrame
                if not airports.empty:
                    airports['city_id'] = city['city_id']

                # Append to the main DataFrame, not to a list
                all_airports = pd.concat([all_airports, airports], ignore_index=True)
            else:
                print(f"Error status code: {response.status_code}")

        except Exception as e:
            print(f"Error retrieving data for city ID {city['city_id']}: {str(e)}")

    print(all_airports.columns)
    all_departures = pd.DataFrame()
    all_arrivals = pd.DataFrame()
    for index, airport in all_airports.iterrows():
        # access DataFrame column values
        iata = airport['iata']
        url = f'https://aerodatabox.p.rapidapi.com/flights/airports/iata/{iata}?offsetMinutes=-120&durationMinutes=720&withLeg=true&direction=Both&withCancelled=true&withCodeshared=true&withCargo=false&withPrivate=true&withLocation=false'
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code == 200:
            data = response.json()
            departures = pd.json_normalize(data.get('departures', []))
            arrivals = pd.json_normalize(data.get('arrivals', []))
            departures['iata'] = airport['iata']
            arrivals['iata'] = airport['iata']
            all_departures = pd.concat([all_departures, departures], ignore_index=True)
            all_arrivals = pd.concat([all_arrivals, arrivals], ignore_index=True)

    cols_to_drop_all_departures = ['codeshareStatus', 'isCargo', 'departure.runwayTime.utc',
                                           'departure.runwayTime.local', 'departure.terminal', 'departure.checkInDesk', 'departure.gate',
                                           'departure.runway', 'departure.quality', 'arrival.airport.icao',
                                           'arrival.airport.iata', 'arrival.airport.name',
                                           'arrival.airport.timeZone', 'arrival.scheduledTime.utc',
                                           'arrival.scheduledTime.local', 'arrival.revisedTime.utc',
                                           'arrival.revisedTime.local', 'arrival.terminal', 'arrival.gate',
                                           'arrival.quality', 'aircraft.reg', 'aircraft.modeS', 'aircraft.model',
                                           'airline.name', 'airline.iata', 'airline.icao', 'arrival.baggageBelt',
                                           'arrival.runwayTime.utc', 'arrival.runwayTime.local', 'arrival.runway']
    cols_to_drop_filtered = [col for col in cols_to_drop_all_departures if col in all_departures.columns]
    all_departures = all_departures.drop(cols_to_drop_filtered, axis=1)
    cols_to_drop_all_arrivals = ['codeshareStatus', 'isCargo',
                                       'departure.airport.icao', 'departure.airport.iata',
                                       'departure.airport.name', 'departure.airport.timeZone',
                                       'departure.terminal', 'departure.quality','arrival.baggageBelt', 'arrival.runway', 'arrival.quality',
                                       'aircraft.reg', 'aircraft.modeS', 'aircraft.model', 'airline.name',
                                       'airline.iata', 'airline.icao', 'departure.checkInDesk',
                                       'departure.gate', 'departure.runway']
    cols_to_drop_arrivals_filtered = [col for col in cols_to_drop_all_arrivals if col in all_arrivals.columns]
    all_arrivals = all_arrivals.drop(cols_to_drop_arrivals_filtered, axis=1)
    all_airports = all_airports.rename(columns=lambda x: x.replace('.', '_'))
    all_departures = all_departures.rename(columns=lambda x: x.replace('.', '_'))
    all_arrivals = all_arrivals.rename(columns=lambda x: x.replace('.', '_'))
    push_to_my_sql(all_airports, 'airports')
    push_to_my_sql(all_departures, 'departures')
    push_to_my_sql(all_arrivals, 'arrivals')




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

# Get weather data for cities
push_weather_to_sql()
get_flights_data()
