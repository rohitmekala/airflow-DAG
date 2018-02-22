
from airflow import DAG
from airflow.operators import DummyOperator
from airflow.operators import PythonOperator
from airflow.operators import EmailOperator

from datetime import datetime, timedelta
import json
import requests
import datetime as dt

    
def fetchWeather(city):
        user_api = '92f29ce0c3311c84e903556bcafcc523'
        unit = 'kelvin'
        api = 'http://api.openweathermap.org/data/2.5/weather?q='
        api_url = api + str(city) + '&mode=json&units=' + unit + 'us&APPID=' + user_api
        response = requests.get(api_url)
        print(response)
        return response.json()


def data_organizer(raw_api_dict):
    data = dict(
        city=raw_api_dict.get('name'),
        country=raw_api_dict.get('sys').get('country'),        
        temp=raw_api_dict.get('main').get('temp'),
        temp_max=raw_api_dict.get('main').get('temp_max'),
        temp_min=raw_api_dict.get('main').get('temp_min'),
        sky=raw_api_dict['weather'][0]['main'],
  
    )
    return data

def data_output(data):
    print('Current weather in: {}, {}:'.format(data['city'], data['country']))
    print(data['temp'], data['sky'])
    print('Max: {}, Min: {}'.format(data['temp_max'], data['temp_min']))
    
def printweatherHandler(*args, **kwargs):
    task_instance = kwargs['ti']
    print('------------------------------>', task_instance)
    task_result = task_instance.xcom_pull(task_ids='getweather')
    print('task_result,------------------------------>', task_result)
    data_output(task_result)

def weatherHandler(*args, **kwargs):
    print('------------------------------>', kwargs['dag_run'].conf)
    params =  kwargs['dag_run'].conf

    city =  kwargs['params']['defaultCity']
    if params is not None and 'city' in params:
        city=params['city']
       
    print('getting city weather ------------------------------>',city)
    weather_data = fetchWeather(city)
    weatherobj = data_organizer(weather_data)
    return weatherobj
    
