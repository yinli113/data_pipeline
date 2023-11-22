from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator to load data into a dimension table in Redshift.
    """

    ui_color = '#80BD9E'

    loading_table_sql_append = """
        INSERT INTO {}
        {};
    """
    
    loading_table_sql_delete_load = """
        DELETE FROM {};
        INSERT INTO {}
        {};
    """
    
    truncate_table_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 load_sql="",
                 truncate_table=False,
                 delete_load=False, 
                 *args, **kwargs):
        """
        Initialize the LoadDimensionOperator.

        :param table: Target dimension table name
        :param redshift_conn_id: Connection ID for Redshift
        :param load_sql: SQL query for loading data
        :param truncate_table: If True, truncate the table before loading
        :param delete_load: If True, perform delete-load operation
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql = load_sql
        self.truncate_table = truncate_table
        self.delete_load = delete_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        try:
            if self.truncate_table and not self.delete_load:
                self.log.info(f"Truncating dimension table: {self.table}")
                redshift.run(LoadDimensionOperator.truncate_table_sql.format(self.table))

            self.log.info(f"Loading dimension table {self.table}")
            
            if self.delete_load:
                formatted_sql = LoadDimensionOperator.loading_table_sql_delete_load.format(
                    self.table,
                    self.table,
                    self.load_sql
                )
            else:
                formatted_sql = LoadDimensionOperator.loading_table_sql_append.format(
                    self.table,
                    self.load_sql
                )
            
            self.log.info(f"Executing Query: {formatted_sql}")
            redshift.run(formatted_sql)
            
            self.log.info(f"Dimension table {self.table} loaded successfully.")
        except Exception as e:
            self.log.error(f"Error loading dimension table {self.table}: {str(e)}")
            raise

