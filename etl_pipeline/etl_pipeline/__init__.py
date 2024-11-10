from . import assets
from dagster import Definitions,load_assets_from_modules
from .resources.mysql_io_manager import MySQLIOManager
from .resources.minio_io_manager import MinIOIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from dagster import Definitions, ScheduleDefinition,AssetSelection, define_asset_job,DailyPartitionsDefinition, build_schedule_from_partitioned_job

MYSQL_CONFIG = {
    "host": "de_mysql",  # Thay localhost bằng tên service MySQL
    "port": 3306,
    "database": "premier_league_football",
    "user": "admin",
    "password": "admin123",
}

MINIO_CONFIG = {
    "endpoint": "minio:9000",  # Thay localhost bằng tên service MinIO
    "bucket_name": "warehouse",
    "access_key": "minio",
    "secret_key": "minio123",
    "secure": False
}
PSQL_CONFIG = {
    "host": "de_psql",
    "port": 5432,
    "database": "postgres",
    "user": "admin",
    "password": "admin123",
}

# Định nghĩa các tài nguyên
resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG)
}

assets = load_assets_from_modules([assets])  # Thay assets bằng module của bạn

# Define asset job for partitioned assets
partitioned_asset_job = define_asset_job(
    name= "partitioned_football", 
    selection=AssetSelection.all(),
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16")
)
# Schedule definition (replace `etl_pipeline` with actual schedule or job)
etl_schedule = build_schedule_from_partitioned_job(partitioned_asset_job)

# Hợp nhất tất cả vào một Definitions duy nhất
definition = Definitions(
    assets=assets,  # Kết hợp assets
    resources=resources,
    #schedules=[etl_schedule]
)
