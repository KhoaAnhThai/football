from dagster import Definitions, asset, Output, DailyPartitionsDefinition
from dagster import AssetIn, AssetOut, Output, multi_asset
from dagster import DailyPartitionsDefinition
import pandas as pd


@asset(
    description='Data quality',
    io_manager_key='minio_io_manager',
    ins={
        "result": AssetIn(
            key=["bronze", "football", "bronze_result"],
        )
        ,
        "team": AssetIn(
            key=["bronze", "football", "bronze_team"],
        )
        
    },
    key_prefix=['silver','football'],
    compute_kind="Pandas",
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16")
)
def silver_result_cleaned(context, result,team) -> Output[pd.DataFrame]:
       # Giả sử upstream là DataFrame
    df = result # hoặc upstream.to_pandas() nếu cần thiết
    if df.empty != True:
        # Tách match_score thành home_score và away_score
        df[['home_score', 'away_score']] = df['match_score'].str.split('-', expand=True)

        # Chuyển đổi kiểu dữ liệu cho home_score và away_score nếu cần
        df['home_score'] = df['home_score'].astype(int)  # Chuyển đổi thành kiểu int
        df['away_score'] = df['away_score'].astype(int)  # Chuyển đổi thành kiểu int
        team.drop(columns = ['abbreviated'],inplace = True)
        team.rename(columns={'team':'home'},inplace = True)

        df = pd.merge(df,team,on= 'home')

    else:
        df = pd.DataFrame(columns=['match_code','date','home','away','home_score','away_score','stadium','city','capacity'])
    return Output(
        df,
        metadata={
            "schema": "public",
            "records_count": len(df),
            'columns': df.columns.to_list()
        },
    )
   
 

@asset(
    description= 'Clean goal data',
    ins={
        'upstream': AssetIn(
            key= ['bronze','football','bronze_goal']
        ),
        'player': AssetIn(
            key = ['bronze','football','bronze_player_info'],
        )
    },
    io_manager_key= "minio_io_manager",
    key_prefix=['silver','football'],
    
    compute_kind='Pandas',
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16")
)
def silver_goal_cleaned(context,upstream,player)->Output:
    
    df = pd.merge(upstream,player,on='player_id',how = 'left')
    
    df = df.drop(columns= 'position')

    df.loc[:, 'time'] = df['time'].str.split('-')


# Tạo một DataFrame mới bằng cách lặp qua từng dòng và tách các giá trị 'time_split'
    df = df.explode('time')
    
    return Output(
        df,
        metadata={
            "schema": "public",
            "records_count": len(df),
            'columns': df.columns.to_list()
        },
        
    )
    
@asset(
    description= 'Clean card data',
    ins={
        'upstream': AssetIn(
            key= ['bronze','football','bronze_card']
        ),
        'player': AssetIn(
            key = ['bronze','football','bronze_player_info'],
        )
    },
    io_manager_key= "minio_io_manager",
    key_prefix=['silver','football'],
    
    compute_kind='Pandas',
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16")
)
def silver_card_cleaned(context,upstream,player)->Output:
    
    df = pd.merge(upstream,player,on='player_id',how = 'left')
    
    df = df.drop(columns= 'position')
  
    return Output(
        df,
        metadata={
            "records_count": len(df),
            'columns': df.columns.to_list()
        },
        
    )