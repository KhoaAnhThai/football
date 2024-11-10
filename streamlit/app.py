import streamlit as st
from sqlalchemy import create_engine
import pandas as pd

# Set up connection parameters
db_username = "admin"
db_password = "admin123"
db_host = "de_psql"
db_port = "5432"
db_name = "postgres"

# Initialize connection
engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Perform query
@st.cache_data  # Caches data to improve performance
def load_data(table: str):
    query = f'SELECT * FROM {table};'
    return pd.read_sql(query, engine)

rank = load_data("rankings")
rank.rename(columns={"team": "Club", "won": "Won","drawn":"Drawn","lost": 'Lost', 'total_match':'Played','gd': 'GD','points': 'Points'}, inplace=True)
rank.index = rank.index + 1

top_goal = load_data("top_goal")
top_goal.index = top_goal.index + 1
top_goal.rename(columns={'player': 'Player','club': 'Club', 'stat': 'Stat'},inplace= True)

red = load_data("red_card")
red.index = red.index + 1
red.rename(columns={'player': 'Player','club': 'Club', 'stat': 'Stat'},inplace= True)

yellow = load_data("yellow_card")
yellow.index = yellow.index + 1
yellow.rename(columns={'player': 'Player','club': 'Club', 'stat': 'Stat'},inplace= True)

st.image("https://resources.premierleague.com/premierleague/competitions/competition_1.png",  width=210)
# Horizontal page selection

page = st.selectbox('', ["Tables", "Stats"])

# Kiểm tra nếu người dùng chọn "Stats"
if page == "Stats":
    # Tạo selectbox phụ để hiển thị các lựa chọn con khi "Stats" được chọn
    stat_option = st.radio('Choose an option in Stats', ["Red", "Goal", "Yellow"],horizontal = True)
    if stat_option == "Goal":
        st.title("Goal Scores")
        st.dataframe(top_goal,width=1000)
    if stat_option == "Red":
        st.title("Red Card")
        
        st.dataframe(red,width=1000)
        
    if stat_option == "Yellow":
        st.title("Yellow Card")
        st.dataframe(yellow,width=1000)
            
        
elif page == "Tables":
    st.title("Tables")
    st.dataframe(rank,width=1000)  # Sử dụng chiều rộng container