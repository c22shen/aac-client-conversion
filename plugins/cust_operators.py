import logging
import pendulum
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator
from coseco2cifpython import python_execute


log = logging.getLogger(__name__)

class SFTPUploadOperator(BaseOperator):

    @apply_defaults
    def __init__(self, 
                ssh_conn_id=None,
                *args,
                **kwargs):
        super(SFTPUploadOperator, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context):
        task_instance = context['task_instance']
        
        upstream_tasks = self.get_flat_relatives(upstream=True)
        upstream_task_ids = [task.task_id for task in upstream_tasks]
        generated_output_files_list = task_instance.xcom_pull(task_ids=upstream_task_ids, key='generated_output_files')
        log.info('The generated_ouputfile list names are: %s', generated_output_files_list)
        log.info('The generated_ouputfile list names type are: %s', type(generated_output_files_list))
        
        generated_output_files = next((item for item in generated_output_files_list if item is not None), {})
        log.info('The generated_ouputfile names are: %s', generated_output_files)
        log.info('The generated_ouputfile names type are: %s', type(generated_output_files))
        output_transfer_msg = None
        try: 
            self.log.info("Trying ssh_conn_id to create SSHHook.")
            self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)                       
            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                for output_file_name in generated_output_files.values(): 
                    local_filepath = './' + output_file_name
                    remote_filepath = '/AI_Output/' + output_file_name
                    output_transfer_msg = "from {1} to {0}".format(remote_filepath,
                                                    local_filepath)
                    self.log.info("Starting to transfer output file  %s", output_transfer_msg)
                    sftp_client.put(local_filepath, remote_filepath, confirm=True)
        except Exception as e: 
            raise AirflowException("Error while transferring {0}, error: {1}. Retrying..."
                                   .format(output_transfer_msg, str(e)))

class ClientConversionOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ClientConversionOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Client Conversion Initiation")
        task_instance = context['task_instance']
        
        upstream_tasks = self.get_flat_relatives(upstream=True)
        upstream_task_ids = [task.task_id for task in upstream_tasks]


        # This will be dynamic later, or not make it so complicated
        file_names = task_instance.xcom_pull(task_ids=upstream_task_ids, key='extract_input_extract_file')
        log.info('The file names are: %s', file_names)
        file_name =  next((item for item in file_names if item is not None), '')
         log.info('The file name is: %s', file_name)
        generated_output_files = python_execute(file_name)
        task_instance.xcom_push('generated_output_files', generated_output_files)

class UrgentOrRegular(object):
    REGULAR= 'regular'
    URGENT = 'urgent'

class ExtractOrEmail(object):
    EMAIL= 'email'
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
        # local_est_tz = pendulum.timezone("America/Toronto")
        input_transfer_msg = None
        execute_date = context.get('execution_date')
        # current_execution_date = execute_date.add(days=1)
        # current_execution_date_est = local_est_tz.convert(current_execution_date)
        self.log.info("Execution date is: (%s)", execute_date)
        # self.log.info("Today's date should be: (%s)", current_execution_date)
        # self.log.info("Today's date eastern should be: (%s)", current_execution_date_est)
        # self.log.info("Today's date eastern in proper format should be: (%s)", current_execution_date_est.strftime("%Y%m%d"))

        input_file_name = _construct_input_file_name(self.regular_or_urgent, self.extract_or_email, execute_date.strftime("%Y%m%d"))
        
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
                task_instance.xcom_push(self.extract_or_email + '_input_extract_file', input_file_name)

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

class CustomPlugins(AirflowPlugin):
    name = "cpoc_custom_plugin"
    operators = [FTPGetFileSensor, ClientConversionOperator, SFTPUploadOperator]