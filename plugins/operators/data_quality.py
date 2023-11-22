from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator to perform data quality checks on a Redshift database.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 dq_checks=[],
                 redshift_conn_id="",
                 *args, **kwargs):
        """
        Initialize the DataQualityOperator.

        :param dq_checks: List of data quality check configurations.
        :param redshift_conn_id: Connection ID for the Redshift database.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        if not self.dq_checks:
            self.log.info("No data quality checks provided")
            return

        redshift_hook = PostgresHook(self.redshift_conn_id)
        error_count = 0
        failing_tests = []

        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')

            try:
                self.log.info(f"Running query: {sql}")
                records = redshift_hook.get_records(sql)

                if not records:
                    raise ValueError("No records returned for the data quality check")

                actual_result = records[0]
                if exp_result != actual_result:
                    error_count += 1
                    failing_tests.append({'sql': sql, 'expected': exp_result, 'actual': actual_result})

            except IndexError:
                self.log.info(f"No records returned for the data quality check: {sql}")
                error_count += 1
                failing_tests.append({'sql': sql, 'expected': exp_result, 'actual': None})

            except Exception as e:
                self.log.error(f"Query failed with exception: {e}")

        if error_count > 0:
            self.log.error('Data quality tests failed')
            for failure in failing_tests:
                self.log.error(f"Check: {failure['sql']}, Expected: {failure['expected']}, Actual: {failure['actual']}")
            raise DataQualityCheckError('Data quality check failed')

        else:
            self.log.info("All data quality checks passed")


class DataQualityCheckError(Exception):
    """
    Custom exception for data quality check failures.
    """
    pass

