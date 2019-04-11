from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import MyFirstOperator, MyFirstSensor, FTPGetFileSensor
from airflow.contrib.sensors.sftp_sensor import SFTPSensor
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.utils.dates import days_ago

args= {
    'owner': 'CPOC',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email': ['xiaoshen2009@gmail.com', 'xiao_shen@cooperators.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('my_test_dag', description='Another tutorial DAG',
          schedule_interval='0 7 * * 1-5', start_date=days_ago(1), catchup=False, default_args = args)

# gcp_sftp_check= SFTPSensor(task_id='ftp_sensor_task', path='/xiao.txt', sftp_conn_id='COOP_SFTP_PROD')

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

sensor_task = MyFirstSensor(task_id='my_sensor_task', poke_interval=30, dag=dag)

operator_task = MyFirstOperator(my_operator_param='This is a test.',
                                task_id='my_first_operator_task', dag=dag, execution_timeout=timedelta(minutes=30))

ftp_get_regular_file_sensor_task = FTPGetFileSensor(task_id='get_regular_file_sftpsensor', ssh_conn_id='COOP_SFTP_PROD', extract_file_type='regular', poke_interval=30, dag=dag)

ftp_get_urgent_file_sensor_task = FTPGetFileSensor(task_id='get_urgent_file_sftpsensor', ssh_conn_id='COOP_SFTP_PROD', extract_file_type='urgent', poke_interval=30, dag=dag)

# sftp_test_file=SFTPOperator(task_id='test_sftp', ssh_conn_id='COOP_SFTP_PROD', local_filepath='./xiaoreceive.txt', remote_file_path='/xiao.txt', operation='GET', create_intermediate_dirs=True, dag=dag)
put_test_file = SFTPOperator(task_id='test_sftp', ssh_conn_id='COOP_SFTP_PROD', local_filepath='./xiao3.txt', remote_filepath='/xiao.txt', operation='GET', dag=dag)
sftp_sensor_file = SFTPSensor(task_id='sftp_sensor', sftp_conn_id='COOP_SFTP_PROD', path='/home/xshn/blockspring-resultsput.csv')
# dummy_task >> put_test_file >> sftp_sensor_file >> sensor_task >>  operator_task 
# dummy_task >> ftp_get_file_sensor_task >> sensor_task >>  operator_task 

dummy_task.set_upstream([ftp_get_regular_file_sensor_task, ftp_get_urgent_file_sensor_task])

