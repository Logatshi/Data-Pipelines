from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook   

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_conn_id='',
                 s3_bucket='',
                 s3_key='',
                 redshift_table_name='',
                 copy_options='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id   
        self.aws_conn_id = aws_conn_id   
        self.s3_bucket = s3_bucket   
        self.s3_key = s3_key  
        self.redshift_table_name = redshift_table_name   
        self.copy_options = copy_options   

    def execute(self, context):
        self.log.info(f'Staging data from S3 to Redshift table: {self.redshift_table_name}')

        # Instantiate the Redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Fetch AWS credentials from Airflow connection
        aws_conn = BaseHook.get_connection(self.aws_conn_id)
        aws_access_key = aws_conn.login
        aws_secret_key = aws_conn.password

        # Clear the staging table
        self.log.info(f'Deleting data from {self.redshift_table_name} table')
        redshift.run(f"DELETE FROM {self.redshift_table_name};")

        # Construct the COPY command
        copy_sql = f"""
            COPY {self.redshift_table_name}
            FROM 's3://{self.s3_bucket}/{self.s3_key}'
            ACCESS_KEY_ID '{aws_access_key}'
            SECRET_ACCESS_KEY '{aws_secret_key}'
            {self.copy_options}
        """

        # Execute the COPY command
        self.log.info(f'Copying data to Redshift table {self.redshift_table_name} from S3')
        redshift.run(copy_sql)