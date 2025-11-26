import json

import requests

import pandas as pd

import numpy as np

import sqlalchemy

import pymysql

def lambda_handler(event, context):

    city = "Buenos Aires"

    country = "ARG"

    OWM_key = "9c1a4d5aef2beb215814f3db7fa3a39d"

    response = requests.get(f'http://api.openweathermap.org/data/2.5/forecast/?q={city},{country}&appid={OWM_key}&units=metric&lang=en')

    data = response.json()

    # Extract forecast list

    forecast_list = data.get('list', [])

    # Prepare lists for DataFrame columns

    times = []

    temperatures = []

    humidities = []

    weather_statuses = []

    wind_speeds = []

    rain_volumes = []

    snow_volumes = []

    for entry in forecast_list:

        times.append(entry.get('dt_txt', np.nan))

        temperatures.append(entry.get('main', {}).get('temp', np.nan))

        humidities.append(entry.get('main', {}).get('humidity', np.nan))

        weather_statuses.append(entry.get('weather', [{}])[0].get('main', np.nan))

        wind_speeds.append(entry.get('wind', {}).get('speed', np.nan))

        rain_volumes.append(entry.get('rain', {}).get('3h', np.nan))

        snow_volumes.append(entry.get('snow', {}).get('3h', np.nan))

    # Create DataFrame

    df = pd.DataFrame({

        'weather_datetime': times,

        'temperature': temperatures,

        'humidity': humidities,

        'weather_status': weather_statuses,

        'wind': wind_speeds,

        'rain_qty': rain_volumes,

        'snow': snow_volumes,

        'municipality_iso_country': f"Berlin,DE"

    })

    print(df.head())

    schema = "gans"

    host = "database-2.chyuwquw4m0o.us-east-2.rds.amazonaws.com"

    user = "admin"

    password = "fer261165"

    port = 3306

    con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

    # send the weather data to the database

    result = df.to_sql('weather_data', if_exists = 'append', con = con, index=False)

    

    return {

        'statusCode': 200,

        'body': json.dumps('Data was sent to db {result}')

    }