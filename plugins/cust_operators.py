import logging
import pendulum
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from datetime import datetime
from airflow.operators.sensors import BaseSensorOperator


log = logging.getLogger(__name__)

class MyFirstOperator(BaseOperator):

    @apply_defaults
    def __init__(self, my_operator_param, *args, **kwargs):
        self.operator_param = my_operator_param
        super(MyFirstOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Hello World!")
        log.info('operator_param: %s', self.operator_param)
        task_instance = context['task_instance']
        sensors_minute = task_instance.xcom_pull('my_sensor_task', key='sensors_minute')
        log.info('Valid minute as determined by sensor: %s', sensors_minute)


class MyFirstSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(MyFirstSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        current_minute = datetime.now().minute
        execute_date = context.get('execution_date')

        log.info("Execution date is: (%s)", execute_date)
        if current_minute % 3 != 0:
            log.info("Current minute (%s) not is divisible by 3, sensor will retry.", current_minute)
            return False
        local_est_tz = pendulum.timezone("America/Toronto")
        execution_date = context.get('execution_date')
        execution_date_est = local_est_tz.convert(execution_date)
        log.info("execution time est is %s",  execution_date_est)
        log.info("Current minute (%s) is divisible by 3, sensor finishing.", current_minute)
        task_instance = context['task_instance']
        task_instance.xcom_push('sensors_minute', current_minute)
        return True

class ExtractFileType(object):
    REGULAR='regular'
    URGENT = 'urgent'

class FTPGetFileSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, 
                ssh_conn_id=None,
                extract_file_type=ExtractFileType.REGULAR,
                *args, 
                **kwargs):
        super(FTPGetFileSensor, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.extract_file_type= extract_file_type
    
    def poke(self, context):
        local_est_tz = pendulum.timezone("America/Toronto")
        file_msg = None
        execute_date = context.get('execution_date')
        current_execution_date = execute_date.add(days=1)
        current_execution_date_est = local_est_tz.convert(current_execution_date)
        self.log.info("Execution date is: (%s)", execute_date)
        self.log.info("Today's date should be: (%s)", current_execution_date)
        self.log.info("Today's date eastern should be: (%s)", current_execution_date_est)
        self.log.info("Today's date eastern in proper format should be: (%s)", current_execution_date_est.strftime("%Y%m%d"))

        extract_file_name = _construct_input_file_name(self.extract_file_type, current_execution_date_est.strftime("%Y%m%d"))
        self.log.info("file name is now: (%s)", extract_file_name)
        try: 
            self.log.info("Trying ssh_conn_id to create SSHHook.")
            self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)           
            local_filepath = './staging/' + extract_file_name
            remote_filepath = '/' + extract_file_name
            
            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                file_msg = "from {0} to {1}".format(remote_filepath,
                                                    local_filepath)
                self.log.info("Starting to transfer %s", file_msg)
                sftp_client.get(remote_filepath, local_filepath)
                task_instance = context['task_instance']
                task_instance.xcom_push(self.extract_file_type + '_extract_file_name', extract_file_name)
                return True
        except Exception as e: 
            self.log.error("Error while transferring {0}, error: {1}. Retrying..."
                                   .format(file_msg, str(e)))
            return False

class MyFirstPlugin(AirflowPlugin):
    name = "my_first_plugin"
    operators = [MyFirstOperator, MyFirstSensor, FTPGetFileSensor]

def _construct_input_file_name(extractFileType, currentExecutionDate):
    return 'ECMExtract.DB2Data'+ currentExecutionDate + '.' + extractFileType + '.csv'