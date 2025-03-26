# config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Configuration
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')
ROOT_PW = os.getenv('ROOT_PW')
FLIGHTS_API_KEY = os.getenv('FLIGHTS_API_KEY')
