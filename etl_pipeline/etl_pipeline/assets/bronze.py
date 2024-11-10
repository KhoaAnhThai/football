from dagster import Definitions, asset, Output, DailyPartitionsDefinition
from dagster import AssetIn, AssetOut, Output, multi_asset
from dagster import DailyPartitionsDefinition, build_schedule_from_partitioned_job
import pandas as pd
  

@asset(
    description= 'Extract data from MySQL',
    io_manager_key= "minio_io_manager",
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze','football'],
    compute_kind = 'MySQL',
)
def bronze_team(context)->Output:
    table  = 'teams'
    sql = f'Select * from {table}'
    context.log.info(f"Extract data from {table}")
    
    try:
        pd_data = context.resources.mysql_io_manager.extract_data(sql)
        context.log.info(f"Extract successfully from {table}")
        
    except Exception as e:
        context.log.error(f"Error while extracting data from MySQL: {e}")
        pd_data = pd.DataFrame(columns=['team',' abbreviated','stadium','city','capacity'])
        
    
    return Output(
        pd_data, 
        metadata={
            "table": table, 
            "column_count": len(pd_data.columns),
            "records": len(pd_data)
            }
        )
    
@asset(
    description= 'Extract data from MySQL',
    io_manager_key='minio_io_manager',
    required_resource_keys= {'mysql_io_manager'},
    key_prefix= ['bronze','football'],
    compute_kind='MySQL'
)
def bronze_player_info(context)->Output:
    table = 'player_info'
    sql = f'select * from player_info'
    context.log.info(f"Extract data from {table}")
    
    try:
        pd_data = context.resources.mysql_io_manager.extract_data(sql)
        context.log.info(f"Extract successfully from {table}")
    except Exception as e:
        context.log.error(f"Error while extracting data from MySQL: {e}")  
        pd_data = pd.DataFrame(columns=['player_id','first_name','last_name','position','club'])
         
         
    return Output(
        pd_data, 
        metadata={
            "table": table, 
            "column_count": len(pd_data.columns),
            "records": len(pd_data)
            }
        )
    

@asset(
    ins={
        "upstream": AssetIn(
            key=['crawl', 'football', 'goal']
        )
    },
    description= 'Extract data from MySQL',
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "football"],
    compute_kind="MySQL",
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16")
)
def bronze_goal(context,upstream) -> Output:
    table = "goal"  # Đảm bảo tên bảng đúng
    sql_stm = f"SELECT * FROM {table}"
    context.log.info(f"Extract data from {table}")
    
    try:
        # Lấy partition key hiện tại
        partition_date_str = context.asset_partition_key_for_output()
        sql_stm += f" WHERE STR_TO_DATE(date, '%%a %%d %%b %%Y') = '{partition_date_str}'"
        pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
        context.log.info(f"Extract successfully from {table}")
    except Exception as e:
        context.log.error(f"Error while extracting data from MySQL: {e}")
        pd_data = pd.DataFrame(columns = ['player_id','time','match_code','date'])  # Trả về DataFrame rỗng nếu có lỗi

    return Output(
        pd_data, 
        metadata={
            "table": table, 
            "column_count": len(pd_data.columns),
            "records": len(pd_data)
            }
        )
    
    
@asset(
    ins={
        "upstream": AssetIn(
            key=['crawl', 'football', 'match_results']
        )
    },
    description= 'Extract data from MySQL',
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "football"],
    compute_kind="MySQL",
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16")
)
def bronze_result(context,upstream) -> Output:
    table = "match_results"  # Đảm bảo tên bảng đúng
    sql_stm = f"SELECT * FROM {table}"
    context.log.info(f"Extract data from {table}")
    
    try:
        # Lấy partition key hiện tại
        partition_date_str = context.asset_partition_key_for_output()
        sql_stm += f" WHERE STR_TO_DATE(date, '%%a %%d %%b %%Y') = '{partition_date_str}'"
        pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
        context.log.info(f"Extract successfully from {table} date: {partition_date_str}")

    except Exception as e:
        context.log.error(f"Error while extracting data from MySQL: {e}")
        pd_data = pd.DataFrame(columns = ['match_code','date','home','away','match_score'])  # Trả về DataFrame rỗng nếu có lỗi

    return Output(
        pd_data, 
        metadata={
            "table": table, 
            "column_count": len(pd_data.columns),
            "records": len(pd_data)
            }
        )

@asset(
      ins={
        "upstream": AssetIn(
            key=['crawl', 'football', 'cards']
        )
    },
    description= 'Extract data from MySQL',
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "football"],
    compute_kind="MySQL",
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16")
    
)
def bronze_card(context,upstream) -> Output:
    table = "cards"  # Đảm bảo tên bảng đúng
    sql_stm = f"SELECT * FROM {table}"
    context.log.info(f"Extract data from {table}")
    
    try:
        # Lấy partition key hiện tại
        partition_date_str = context.asset_partition_key_for_output()
        sql_stm += f" WHERE STR_TO_DATE(date, '%%a %%d %%b %%Y') = '{partition_date_str}'"
        pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
        context.log.info(f"Extract successfully from {table} date: {partition_date_str}")

    except Exception as e:
        context.log.error(f"Error while extracting data from MySQL: {e}")
        pd_data = pd.DataFrame(columns = ['player_id','match_code','time','date','type'])  # Trả về DataFrame rỗng nếu có lỗi

    return Output(
        pd_data, 
        metadata={
            "table": table, 
            "column_count": len(pd_data.columns),
            "records": len(pd_data)
            }
        )

