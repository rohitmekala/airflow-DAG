
from airflow import DAG
from airflow.operators import DummyOperator
from airflow.operators import PythonOperator
from airflow.operators import EmailOperator

from datetime import datetime, timedelta
import json
import requests
import datetime as dt
from handlers import weatherhandler

default_args = {
    'owner': 'rohit.reddy',
    'depends_on_past': False,
    'start_date': dt.datetime(2018, 1, 1),
    'email': ['Rohit521994@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'city':'Austin'
    }
dag = DAG('LocalWeather', default_args=default_args, schedule_interval=None)


dummy = DummyOperator(
    task_id='dummy',
    trigger_rule='one_success',
    dag=dag
)

getweather=PythonOperator(
    task_id='getweather',
    provide_context=True,
    python_callable=weatherhandler.weatherHandler,
    params={'defaultCity': default_args['city']},
    dag=dag)

printweather=PythonOperator(
    task_id='printweather',
    provide_context=True,
    python_callable=weatherhandler.printweatherHandler,
    dag=dag)

emailNotify = EmailOperator(
   task_id='email_notification',
   to = 'rohit521994@gmail.com',
   subject = 'Weather Job Done',
   html_content = "The weather in {{ ti.xcom_pull(task_ids='printweather',key='city') }} is {{ ti.xcom_pull(task_ids='printweather',key='temp') }}",   
   dag=dag)

dummy>>getweather>>printweather>>emailNotify