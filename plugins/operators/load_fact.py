from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_name='',
                 sql_statement='',
                 append_data=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id  # Connection ID for Redshift
        self.table_name = table_name  # Name of the table to load data into
        self.sql_statement = sql_statement  # SQL statement to execute for loading data
        self.append_data = append_data  # Whether to append data or overwrite

    def execute(self, context):
        self.log.info(f'Loading data into fact table {self.table_name}')

        # Instantiate the Redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_data:
            # Clear the fact table if not appending
            self.log.info(f'Deleting existing data from {self.table_name} table')
            redshift.run(f"DELETE FROM {self.table_name};")

        # Execute the SQL statement to load data into the fact table
        self.log.info(f'Executing SQL: {self.sql_statement}')
        redshift.run(self.sql_statement)