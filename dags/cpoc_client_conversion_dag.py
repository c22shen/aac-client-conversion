
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import FTPGetFileSensor, ClientConversionOperator, SFTPUploadOperator
from airflow.contrib.sensors.sftp_sensor import SFTPSensor
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.utils.dates import days_ago

import pytz
# tz = pytz.timezone('America/Toronto')

# def localize_utc_tz(d):
#     return tz.fromutc(d)

default_args= {
    'owner': 'CPOC',
    'start_date': days_ago(2),
    'depends_on_past': False,
    'email': ['xiaoshen2009@gmail.com', 'xiao_shen@cooperators.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('cpoc_client_conversion', description='Another tutorial DAG', schedule_interval='0 12 * * 1-5', start_date=days_ago(1), catchup=False, default_args = default_args)

regular_cleanup_task = DummyOperator(task_id='regular_cleanup_task', dag=dag)

urgent_cleanup_task = DummyOperator(task_id='urgent_cleanup_task', dag=dag)

regular_upload_task = SFTPUploadOperator(task_id='regular_upload_task', ssh_conn_id='COOP_SFTP_PROD', dag=dag)

urgent_upload_task = SFTPUploadOperator(task_id='urgent_upload_task', ssh_conn_id='COOP_SFTP_PROD', dag=dag)

time_task = BashOperator(
        task_id='utc_time_task',
        dag=dag,
        bash_command='echo UTC {{ ts }} {{ execution_date }} {{ ds }}',
)

ftp_get_regular_file_sensor_task = FTPGetFileSensor(task_id='get_regular_file_sftpsensor', ssh_conn_id='COOP_SFTP_PROD', regular_or_urgent='regular', extract_or_email='extract', poke_interval=30, dag=dag)

ftp_get_urgent_file_sensor_task = FTPGetFileSensor(task_id='get_urgent_file_sftpsensor', ssh_conn_id='COOP_SFTP_PROD', regular_or_urgent='urgent', extract_or_email='extract', poke_interval=30, dag=dag)

ftp_get_regular_email_sensor_task = FTPGetFileSensor(task_id='get_regular_email_sftpsensor', ssh_conn_id='COOP_SFTP_PROD', regular_or_urgent='regular', extract_or_email='email', poke_interval=30, dag=dag)

ftp_get_urgent_email_sensor_task = FTPGetFileSensor(task_id='get_urgent_email_sftpsensor', ssh_conn_id='COOP_SFTP_PROD', regular_or_urgent='urgent', extract_or_email='email', poke_interval=30, dag=dag)

regular_client_conversion_task = ClientConversionOperator(task_id='regular_client_conversion_task', dag=dag)

urgent_client_conversion_task = ClientConversionOperator(task_id='urgent_client_conversion_task', dag=dag)

time_task.set_downstream([ftp_get_regular_file_sensor_task, ftp_get_urgent_file_sensor_task, ftp_get_regular_email_sensor_task, ftp_get_urgent_email_sensor_task])

regular_client_conversion_task.set_upstream([ftp_get_regular_file_sensor_task, ftp_get_regular_email_sensor_task])

urgent_client_conversion_task.set_upstream([ftp_get_urgent_file_sensor_task, ftp_get_urgent_email_sensor_task])

regular_client_conversion_task >> regular_upload_task >> regular_cleanup_task
# initiation_task.set_upstream(time_task)



urgent_client_conversion_task >> urgent_upload_task >> urgent_cleanup_task
# initiation_task.set_upstream(time_task)