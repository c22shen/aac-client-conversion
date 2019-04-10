from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator

def print_hello():
   return 'Hello world!'

dag=DAG('hello_sftp', description='Hello SFTP', schedule_interval='0 7 * * 1-5', start_date=datetime(2017,3,20), catchup = False)
dummy_operator = DummyOperator(task_id='dummy_task', dag=dag)
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
put_test_file = SFTPOperator(task_id='test_sftp', ssh_conn_id='COOP_SFTP_PROD', local_filepath='./xiao2.txt', remote_filepath='/xiao.txt', operation='GET', dag=dag)
dummy_operator >> hello_operator >> put_test_file
