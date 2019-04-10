from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import MyFirstOperator, MyFirstSensor
from airflow.contrib.sensors.sftp_sensor import SFTPSensor
from airflow.contrib.operators.sftp_operator import SFTPOperator

dag = DAG('my_test_dag', description='Another tutorial DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

gcp_sftp_check= SFTPSensor(task_id='ftp_sensor_task', path='/xiao.txt', sftp_conn_id='COOP_SFTP_PROD')

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

sensor_task = MyFirstSensor(task_id='my_sensor_task', poke_interval=30, dag=dag)

operator_task = MyFirstOperator(my_operator_param='This is a test.',
                                task_id='my_first_operator_task', dag=dag)

sftp_test_file=SFTPOperator(task_id='test_sftp', ssh_conn_id='COOP_SFTP_PROD', local_filepath='./xiaoreceive.txt', remote_file_path='/xiao.txt', operation='GET', create_intermediate_dirs=True, dag=dag)
dummy_task >> sftp_test_file >> sensor_task >> operator_task 
