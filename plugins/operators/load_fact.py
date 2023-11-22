from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator to load data into a fact table in Redshift.
    """

    ui_color = '#F98866'
    
    insert_table_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 load_sql="",
                 *args, **kwargs):
        """
        Initialize the LoadFactOperator.

        :param table: Target fact table name
        :param redshift_conn_id: Connection ID for Redshift
        :param load_sql: SQL query for loading data
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql = load_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        try:
            formatted_sql = LoadFactOperator.insert_table_sql.format(
                self.table,
                self.load_sql
            )
            
            self.log.info(f"Executing query: {formatted_sql}")
            redshift.run(formatted_sql)
            
            self.log.info(f"Fact table '{self.table}' loaded successfully.")
        except Exception as e:
            self.log.error(f"Error loading fact table {self.table}: {str(e)}")
            raise
