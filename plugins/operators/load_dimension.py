from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_name='',
                 sql_statement='',
                 append_data=True,  
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id   
        self.table_name = table_name   
        self.sql_statement = sql_statement  
        self.append_data = append_data   

    def execute(self, context):
        self.log.info(f'Loading data into dimension table {self.table_name}')

        # Instantiate the Redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_data:
            # Clear the dimension table if not appending
            self.log.info(f'Deleting existing data from {self.table_name} table')
            redshift.run(f"DELETE FROM {self.table_name};")

        # Execute the SQL statement to load data into the dimension table
        self.log.info(f'Executing SQL: {self.sql_statement}')
        redshift.run(self.sql_statement)