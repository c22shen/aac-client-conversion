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

class UrgentOrRegular(object):
    REGULAR='regular'
    URGENT = 'urgent'

class ExtractOrEmail(object):
    EMAIL='emaill'
    EXTRACT = 'extract'

class FTPGetFileSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, 
                ssh_conn_id=None,
                regular_or_urgent=UrgentOrRegular.REGULAR,
                extract_or_email=ExtractOrEmail.EXTRACT,
                *args, 
                **kwargs):
        super(FTPGetFileSensor, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.regular_or_urgent= regular_or_urgent
        self.extract_or_email = extract_or_email
    
    def poke(self, context):
        local_est_tz = pendulum.timezone("America/Toronto")
        input_transfer_msg = None
        execute_date = context.get('execution_date')
        current_execution_date = execute_date.add(days=1)
        current_execution_date_est = local_est_tz.convert(current_execution_date)
        self.log.info("Execution date is: (%s)", execute_date)
        self.log.info("Today's date should be: (%s)", current_execution_date)
        self.log.info("Today's date eastern should be: (%s)", current_execution_date_est)
        self.log.info("Today's date eastern in proper format should be: (%s)", current_execution_date_est.strftime("%Y%m%d"))

        input_file_name = _construct_input_file_name(self.regular_or_urgent, self.extract_or_email, current_execution_date_est.strftime("%Y%m%d"))
        
        self.log.info("input file name is: (%s)", input_file_name)
        try: 
            self.log.info("Trying ssh_conn_id to create SSHHook.")
            self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)           
            local_filepath = './' + input_file_name
            remote_filepath = '/' + input_file_name
            
            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                input_transfer_msg = "from {0} to {1}".format(remote_filepath,
                                                    local_filepath)
                self.log.info("Starting to transfer extract file  %s", input_transfer_msg)
                sftp_client.get(remote_filepath, local_filepath)

                task_instance = context['task_instance']
                task_instance.xcom_push(self.regular_or_urgent + '_' + self.extract_or_email +'_file_name', input_file_name)

                return True
        except Exception as e: 
            self.log.error("Error while transferring {0}, error: {1}. Retrying..."
                                   .format(input_transfer_msg, str(e)))
            return False

def _construct_input_file_name(file_urgency_level, file_type, currentExecutionDate):
    extract_or_email_file_string = ''

    if file_type == ExtractOrEmail.EMAIL:
       extract_or_email_file_string='.AlternativeEmail'

    return 'ECMExtract.DB2Data'+ currentExecutionDate + '.' + file_urgency_level + extract_or_email_file_string + '.csv'

class MyFirstPlugin(AirflowPlugin):
    name = "my_first_plugin"
    operators = [MyFirstOperator, MyFirstSensor, FTPGetFileSensor]