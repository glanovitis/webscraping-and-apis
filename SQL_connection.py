import pandas as pd


def get_sql_connection():
    schema = "cities_worldwide"
    host = "127.0.0.1"
    user = "root"
    password = "root"
    port = 3306

    return f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'