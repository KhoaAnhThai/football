import pandas as pd
from dagster import multi_asset,asset,AssetIn,AssetOut,DailyPartitionsDefinition,Output

@multi_asset(
    description= 'Load goal data to Postgres',
    
    ins={
        "goals": AssetIn(
            key=["silver", "football", "silver_goal_cleaned"],
        ),
        
    },
    outs={
        "goal": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "primary_keys": [
                    'match_code',"player_id",'time'
                ],
                "columns":["player_id","time","match_code","date","first_name","last_name","club"],
            },
        )
    },
    compute_kind="PostgreSQL",
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16")
)
def gold_goal(context, goals) -> Output[pd.DataFrame]:
    
    return Output(
        goals,
        metadata={
            "schema": "public",
            "records counts": len(goals),
        },
    )
    
# @multi_asset(
#     ins={
#         'appearance': AssetIn(
#             key= ['bronze','football','bronze_appearance']
#         ),
#         'player': AssetIn(
#             key = ['bronze','football','bronze_player_info']
#         )
        
#     }
#     ,
#     outs={
#         "appearance": AssetOut(
#             io_manager_key="psql_io_manager",
#             key_prefix=["warehouse", "public"],
#             metadata={
#                 "primary_keys": [
#                     "player_id",'match_code'
#                 ],
#                 "columns":['match_code','player_id','date','first_name','last_name','position','club'
#         ],
#             },
#         )
        
#     },
#     compute_kind="PostgreSQL",
#     partitions_def=DailyPartitionsDefinition(start_date="2024-08-16")
# )
# def gold_appearance(context, appearance,player) -> Output[pd.DataFrame]:
#     try:
        
#         df = pd.merge(appearance,player,on='player_id',how = 'left')
#     except Exception as e:
#         df = pd.DataFrame(columns=[])
#     return Output(
#         df,
#         metadata={
#             "schema": "public",
#             "records counts": len(df),
#         },
#     )


@multi_asset(
    description= 'Load result data to Postgres',
    ins={
        "upstream": AssetIn(
            key=["silver", "football", "silver_result_cleaned"],
        )
    },
    outs={
        "result": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "primary_keys": [
                    "match_code",
                ],
                "columns":['match_code','date','home','away','home_score','away_score','stadium','city','capacity'],
            },
        )
    },
    compute_kind="PostgreSQL",
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16")
)
def gold_result(context, upstream) -> Output[pd.DataFrame]:
    return Output(
        upstream,
        metadata={
            "schema": "public",
            "records counts": len(upstream),
        },
    )


@multi_asset(
    description= 'Load card data to Postgres',
    ins={
        "upstream": AssetIn(
            key=["silver", "football", "silver_card_cleaned"],
        )
    },
    outs={
        "cards": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "primary_keys": ['player_id','match_code','time','type'],
                "columns":['player_id','match_code','time','date','type','first_name','last_name','club'],
            },
        )
    },
    compute_kind="PostgreSQL",
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16")
)
def gold_card(context, upstream) -> Output[pd.DataFrame]:

    return Output(
        upstream,
        metadata={
            "schema": "public",
            "records counts": len(upstream),
        },
    )
