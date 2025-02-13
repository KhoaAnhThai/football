import pandas as pd
from dagster import IOManager, OutputContext,InputContext,Definitions,AssetCheckExecutionContext
from sqlalchemy import create_engine
from contextlib import contextmanager
from sqlalchemy import text
from datetime import datetime

@contextmanager
def connect_mysql(config):
    conn_info = (
    f"mysql+pymysql://{config['user']}:{config['password']}"
    + f"@{config['host']}:{config['port']}"
    + f"/{config['database']}")
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise
    
class MySQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        with connect_mysql(self._config) as engine:
            obj.to_sql(f"{table}", con=engine, if_exists="append", index=False)
            
                    
    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass

    def extract_data(self, sql: str) -> pd.DataFrame:
        with connect_mysql(self._config) as db_conn:
            pd_data = pd.read_sql_query(sql, db_conn)
            return pd_data