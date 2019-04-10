from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import MyFirstOperator, MyFirstSensor
from airflow.contrib.sensors.sftp_sensor import SFTPSensor

dag = DAG('my_test_dag', description='Another tutorial DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

gcp_sftp_check= SFTPSensor(task_id='sftp_sensor_task', path='/home/xshn/blockspring-results.csv', sftp_conn_id='GCP_SFTP')

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

sensor_task = MyFirstSensor(task_id='my_sensor_task', poke_interval=30, dag=dag)

operator_task = MyFirstOperator(my_operator_param='This is a test.',
                                task_id='my_first_operator_task', dag=dag)

dummy_task >> gcp_sftp_check >> sensor_task >> operator_task 