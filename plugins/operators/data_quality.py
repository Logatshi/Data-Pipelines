from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 data_quality_checks=None,   
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id   
        self.data_quality_checks = data_quality_checks or []   

    def execute(self, context):
        self.log.info('Starting data quality checks')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.data_quality_checks:
            dq_check_sql = check.get('dq_check_sql')
            expected_value = check.get('expected_value')

            self.log.info(f'Executing data quality check: {dq_check_sql}')
            result = redshift.get_records(dq_check_sql)

            if not result:
                raise ValueError(f'Data quality check failed: No results returned for query: {dq_check_sql}')

            actual_value = result[0][0]

            if actual_value != expected_value:
                raise ValueError(f'Data quality check failed: {dq_check_sql} returned {actual_value}, '
                                 f'expected {expected_value}')
            self.log.info(f'Data quality check passed: {dq_check_sql} returned {actual_value}')