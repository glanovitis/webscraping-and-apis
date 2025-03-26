from config import ROOT_PW


def get_sql_connection():
    schema = "cities_worldwide"
    host = "127.0.0.1"
    user = "root"
    password = ROOT_PW
    port = 3306

    return f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'