from dagster import Definitions, asset, Output, DailyPartitionsDefinition, AssetIn, AssetOut,multi_asset
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import re

@asset(
    description= 'fetch data from www.premierleague.com',
    
    name = 'match_results',
    io_manager_key="mysql_io_manager",
    key_prefix=['crawl', 'football'],
    compute_kind="Python",
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16"),
)
def crawl_result(context):
    url = 'https://www.premierleague.com/results'
    pause_time = 5
    
    # Cấu hình các tùy chọn cho Chrome
    options = Options()
    options.add_argument('--headless')  # Chạy Chrome ở chế độ không có giao diện
    options.add_argument('--no-sandbox')  # Bỏ qua sandbox
    options.add_argument('--disable-dev-shm-usage')  # Khắc phục vấn đề bộ nhớ

    driver =  webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Mở trang web
    url = 'https://www.premierleague.com/results'
    driver.get(url)

    # Đặt thời gian chờ để cuộn trang
    pause_time = 5

    # Cuộn trang nhiều lần để tải hết dữ liệu
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        # Cuộn xuống cuối trang
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # Chờ cho nội dung tải thêm
        time.sleep(pause_time)

        # Lấy chiều cao mới của trang
        new_height = driver.execute_script("return document.body.scrollHeight")

        # Nếu chiều cao không thay đổi, đã cuộn hết trang
        if new_height == last_height:
            break

        last_height = new_height

    # Lấy nội dung HTML sau khi cuộn
    html_content = driver.page_source

    # Sử dụng BeautifulSoup để phân tích HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    raw_data = pd.DataFrame(columns=['match_code', 'date', 'home', 'away', 'match_score'])
    doc = soup.find_all('div', class_='fixtures__date-container')

    for i in doc:
        dates = i.find('time', class_='fixtures__date fixtures__date--short')
        matches = i.find_all('li', class_='match-fixture')

        for match in matches:
            match_id = match.get('data-comp-match-item', 'NaN')
            home_team = match.get('data-home', 'NaN')
            away_team = match.get('data-away', 'NaN')
            score = match.find('span', class_='match-fixture__score').text if match.find('span', class_='match-fixture__score') else 'NaN'
        
            date = dates.text.strip() if dates else 'NaN'
            
            data = {
                'match_code': match_id,
                'date': date,
                'home': home_team,
                'away': away_team,
                'match_score': score,
            }
            raw_data.loc[len(raw_data)] = data

    driver.quit()

    try:
        # Lọc theo ngày của partition
        #partition_date_str = context.asset_partition_key_for_output()
        
        #raw_data['date'] = pd.to_datetime(raw_data['date'])  # Chuyển đổi kiểu dữ liệu
        #raw_data = raw_data[pd.to_datetime(raw_data['date']) == partition_date_str]
        #context.log.info(f"Data for partition date {partition_date_str} extracted successfully.")
        pass

    except Exception as e:
        context.log.error(f"Error while filtering data by) partition date: {e}")
        #raw_data = pd.DataFrame(columns=['match_code', 'date', 'home', 'away', 'match_score'])

    return Output(
        raw_data,
        metadata={
            "table": "match_results",
            "column_count": len(raw_data.columns),
            "records": len(raw_data)
        }
    )

# @multi_asset(
#     ins={
#         "upstream": AssetIn(
#             key=['crawl', 'football', 'match_results']
#         )
#     },
#     outs={
#         "apperance": AssetOut(
#             key_prefix=['crawl', 'football'],
#             io_manager_key='mysql_io_manager',
#         )
#     },
#     required_resource_keys={'mysql_io_manager'},
#     compute_kind='MySQL' and 'Python',
#     partitions_def=DailyPartitionsDefinition(start_date="2024-08-16"),
# )
# def crawL_appearance(context, upstream):
#     table = 'match_results'
#     sql_stm = f"Select * from {table}"
    
#     try:
#         context.log.warning(f"Extract data from {table}")
#         partition_date_str = context.partition_key
#         context.log.info(f"Extract temp data from {table} date: {partition_date_str}")
        
#         sql_stm += f" WHERE STR_TO_DATE(date, '%%a %%d %%b %%Y') = '{partition_date_str}'"
#         context.log.info(f"{sql_stm}")
#         df = context.resources.mysql_io_manager.extract_data(sql_stm)
#         context.log.info(f"Extract successfully from {table} date: {partition_date_str}")
#         all_data = []
#         if df.empty:
#             context.log.warning(f"No data found for the date: {partition_date_str}. Returning empty DataFrame.")
#         context.log.info(f'{df.shape}')

#         n = df.shape[0]
#         for i in range(n):
#             match_id = df['match_code'][i]
#             url = f'https://www.premierleague.com/match/{match_id}'

#             # Khởi động trình duyệt Chrome
#             options = Options()
#             options.add_argument('--headless')  # Chạy Chrome ở chế độ không có giao diện
#             options.add_argument('--no-sandbox')  # Bỏ qua sandbox
#             options.add_argument('--disable-dev-shm-usage')  # Khắc phục vấn đề bộ nhớ

#             all_data = []
#             driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options = options)
#             driver.get(url)

#             # Lấy nội dung HTML
#             html_content = driver.page_source
            

#             # Phân tích nội dung HTML
#             soup = BeautifulSoup(html_content, 'html.parser')
            
#             teams = soup.find_all('div',class_= 'matchLineupTeamContainer')
            
#             for team in teams:
#                 stats= team.find_all('li',class_='player')
#                 for player in stats:
#                     href = player.find('a')['href']
#                     player_id = href.split('/')[2]
                    
#                     data = {
#                         'match_code': match_id,
#                         'player_id': player_id,
#                         'date': df['date'][i]
#                     }
#                     all_data.append(data)
#         raw_data = pd.DataFrame(all_data)
        
#     except Exception as e:
#         context.log.error(f"Error while extracting data from MySQL: {e}")
#         raw_data = pd.DataFrame(columns = ['match_code','player_id','date'])
        
#     return Output(
#         raw_data, 
#         metadata={
#             "table": 'apperance', 
#             "column_count": len(raw_data.columns),
#             "column_names": raw_data.columns.to_list(),
#             "records": len(raw_data)
#             }
#         ) 
# @asset(
#     description= 'fetch data from www.premierleague.com',
#     name = 'player_info',
#     io_manager_key="mysql_io_manager",
#     key_prefix=['crawl', 'football'],
#     compute_kind="Python",
# )
# def crawl_player_info(context):
#     clubs = [
#         (1, 'Arsenal'),
#         (2, 'Aston-Villa'),
#         (127, 'Bournemouth'),
#         (130, 'Brentford'),
#         (131, 'Brighton-and-Hove-Albion'),
#         (4, 'Chelsea'),
#         (6, 'Crystal-Palace'),
#         (7, 'Everton'),
#         (34, 'Fulham'),
#         (8, 'Ipswich-Town'),
#         (26, 'Leicester-City'),
#         (10, 'Liverpool'),
#         (11, 'Manchester-City'),
#         (12, 'Manchester-United'),
#         (13, 'Newcastle-United'),
#         (15, 'Nottingham-Forest'),
#         (20, 'Southampton'),
#         (21, 'Tottenham-Hotspur'),
#         (25, 'West-Ham-United'),
#         (38, 'Wolverhampton-Wanderers')
#     ]

#     # Danh sách để lưu dữ liệu cầu thủ
#     players_data = []

#     # Khởi động WebDriver
#     options = Options()
#     options.add_argument('--headless')  # Chạy Chrome ở chế độ không có giao diện
#     options.add_argument('--no-sandbox')  # Bỏ qua sandbox
#     options.add_argument('--disable-dev-shm-usage')  # Khắc phục vấn đề bộ nhớ

#     driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options= options)

#     for club_id, club_name in clubs:
#         url = f'https://www.premierleague.com/clubs/{club_id}/{club_name}/squad?se=719'
#         driver.get(url)

#         # Lấy nội dung HTML sau khi cuộn
#         html_content = driver.page_source

#         # Sử dụng BeautifulSoup để phân tích HTML
#         soup = BeautifulSoup(html_content, 'html.parser')
#         squad = soup.find_all('li', class_='stats-card')
        
#         for player in squad:
#             info = player.find('div', class_='stats-card__player-info')
#             # Khai thác thông tin cầu thủ với xử lý lỗi
#             first_name = info.find('div', class_='stats-card__player-first').text.strip() if info.find('div', class_='stats-card__player-first') else 'NaN'
#             last_name = info.find('div', class_='stats-card__player-last').text.strip() if info.find('div', class_='stats-card__player-last') else 'NaN'
#             position = info.find('div', class_='stats-card__player-position').text.strip() if info.find('div', class_='stats-card__player-position') else 'NaN'
#             try:
#                 href = player.find('a')['href']
#                 player_id = href.split('/')[2]
#             except:
#                 player_id = 'NaN'
#             # Thêm thông tin cầu thủ vào danh sách
#             players_data.append({
#                 'player_id': player_id,
#                 'first_name': first_name,
#                 'last_name': last_name,
#                 'position': position,
#                 'club': club_name
#             })
#         context.log.info(f"Succesfully fetch data of {club_name}")

#     # Tạo DataFrame từ danh sách
#     players_df = pd.DataFrame(players_data)
    
#     # Đóng trình duyệt
#     driver.quit()
#     return Output(
#         players_df, 
#         metadata={
#             "table": 'player_info', 
#             "column_count": len(players_df.columns),
#             "column_names": players_df.columns.to_list(),
#             "records": len(players_df)
#             }
#         ) 


@multi_asset(
    ins={
        "upstream": AssetIn(
            key=['crawl', 'football', 'match_results']
        )
    },
    outs={
        "goal": AssetOut(
            key_prefix=['crawl', 'football'],
            io_manager_key='mysql_io_manager',
        )
    },
    description= 'fetch data from www.premierleague.com',
    required_resource_keys={'mysql_io_manager'},
    compute_kind='Python',
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16"),
)
def crawl_goal(context,upstream):
    table = 'match_results'
    sql_stm = f"Select * from {table}"
    
    try:
        context.log.warning(f"Extract data from {table}")
        partition_date_str = context.partition_key
        context.log.info(f"Extract temp data from {table} date: {partition_date_str}")
        
        sql_stm += f" WHERE STR_TO_DATE(date, '%%a %%d %%b %%Y') = '{partition_date_str}'"
        context.log.info(f"{sql_stm}")
        df = context.resources.mysql_io_manager.extract_data(sql_stm)
        context.log.info(f"Extract successfully from {table} date: {partition_date_str}")
        
        raw_data = pd.DataFrame(columns=['player_id', 'time', 'match_code','date'])
        # Tạo danh sách để lưu trữ dữ liệu
        all_data = []

        # Duyệt qua từng dòng trong DataFrame
        n = df.shape[0]

        for i in range(n):
            match_id = df['match_code'][i]
            
            url = f'https://www.premierleague.com/match/{match_id}'
            options = Options()
            options.add_argument('--headless')  # Chạy Chrome ở chế độ không có giao diện
            options.add_argument('--no-sandbox')  # Bỏ qua sandbox
            options.add_argument('--disable-dev-shm-usage')  # Khắc phục vấn đề bộ nhớ


            # Khởi động trình duyệt Chrome
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options= options)
            driver.get(url)

            # Lấy nội dung HTML
            html_content = driver.page_source
            

            # Phân tích nội dung HTML
            soup = BeautifulSoup(html_content, 'html.parser')

            # Lấy tên đội
            summary = soup.find('div', class_='mc-summary__scorebox-container')
            team_containers = summary.find_all('div', class_='mc-summary__team-container')
            teams = [container.find('span', class_='mc-summary__team-name u-hide-phablet').text for container in team_containers]

            # Hàm để trích xuất sự kiện cho một đội
            def extract_events(container, team_name, match_id):
                events = container.find_all('div', class_='mc-summary__event')
                for event in events:
                    first = event.find('div', class_='mc-summary__scorer-name-first')
                    last = event.find('div', class_='mc-summary__scorer-name-last')
                    time = event.find('span', class_='mc-summary__event-time')
                    href = event.find('a')['href']
                    player_id = href.split('/')[2]

                    event_data = {
                        'player_id': player_id,
                        'time': re.sub(r'\s+', ' ', time.text.strip().replace('\n', '')).replace(',', '-') if time else '',
                        'match_code': match_id,
                        'date': df['date'][i]
                    }
                    raw_data.loc[len(raw_data)] = event_data

            # Trích xuất sự kiện cho đội nhà
            home_container = soup.find('div', class_='matchEvents matchEventsContainer home')
            extract_events(home_container, teams[0],match_id)

            # Trích xuất sự kiện cho đội khách
            away_container = soup.find('div', class_='matchEvents matchEventsContainer away')
            extract_events(away_container, teams[1],match_id)
        driver.quit()  # Đóng trình duyệt sau khi lấy nội dung   
        
    except Exception as e:
        context.log.error(f"Error while extracting data from MySQL: {e}")
        raw_data = pd.DataFrame(columns = ['player_id', 'time', 'match_code','date'])
        
    return Output(
        raw_data, 
        metadata={
            "table": 'goal', 
            "column_count": len(raw_data.columns),
            "column_names": raw_data.columns.to_list(),
            "records": len(raw_data)
            }
        ) 
    
@multi_asset(
    ins={
        "upstream": AssetIn(
            key=['crawl', 'football', 'match_results']
        )
    },
    outs={
        "cards": AssetOut(
            key_prefix=['crawl', 'football'],
            io_manager_key='mysql_io_manager',
        )
    },
    required_resource_keys={'mysql_io_manager'},
    compute_kind='Python',
    partitions_def=DailyPartitionsDefinition(start_date="2024-08-16"),
    description= 'fetch data from www.premierleague.com',
)
def crawl_card(context,upstream):
    table = 'match_results'
    sql_stm = f"Select * from {table}"
    
    try:
        context.log.warning(f"Extract data from {table}")
        partition_date_str = context.partition_key
        context.log.info(f"Extract temp data from {table} date: {partition_date_str}")
        
        sql_stm += f" WHERE STR_TO_DATE(date, '%%a %%d %%b %%Y') = '{partition_date_str}'"
        context.log.info(f"{sql_stm}")
        df = context.resources.mysql_io_manager.extract_data(sql_stm)
        context.log.info(f"Extract successfully from {table} date: {partition_date_str}")
        
        raw_data = pd.DataFrame()
            # Tạo danh sách để lưu trữ dữ liệu
        all_data = []

            # Duyệt qua từng dòng trong DataFrame
        n = df.shape[0]

        for i in range(n):
            match_id = df['match_code'][i]
            date = df['date'][i]
            
            url = f'https://www.premierleague.com/match/{match_id}'
            #url = 'https://www.premierleague.com/match/115873'
                # Khởi động trình duyệt Chrome
            options = Options()
            options.add_argument('--headless')  # Chạy Chrome ở chế độ không có giao diện
            options.add_argument('--no-sandbox')  # Bỏ qua sandbox
            options.add_argument('--disable-dev-shm-usage')  # Khắc phục vấn đề bộ nhớ

            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options= options)
            driver.get(url)

                # Lấy nội dung HTML
            html_content = driver.page_source
                
                # Phân tích nội dung HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            try:
                event_line = soup.find('div', class_="eventLine timeLineEventsContainer is-completed")
                event_away = event_line.find_all('ul', class_="event__icons event__icons--away")
                event_home = event_line.find_all('ul', class_="event__icons event__icons--home")

                for i in event_away:
                    # Lặp qua từng <ul> và tìm các div chứa thông tin sự kiện
                    b = i.find_all('div', class_="eventInfo")
                    
                    for info in b:  # Duyệt qua từng sự kiện trong danh sách b
                        # Kiểm tra xem có thông tin Yellow Card hay không
                        if "Yellow Card" in info.text:
                            player_name = info.find('a', class_='name').text.strip()

                            # Lấy ID cầu thủ từ href
                            player_id = info.find('a', class_='name')['href'].split('/')[2]
                            minute = info.find('time', class_='min').text.strip()

                    
                            data = {
                                'player_id': player_id,
                                'match_code': match_id,
                                'time': minute,
                                'date': date,
                                'type': "yellow"
                            }
                            all_data.append(data)
                        if "Red Card" in info.text:
                            player_name = info.find('a', class_='name').text.strip()

                            # Lấy ID cầu thủ từ href
                            player_id = info.find('a', class_='name')['href'].split('/')[2]
                            minute = info.find('time', class_='min').text.strip()


                    
                            data = {
                                'player_id': player_id,
                                'match_code': match_id,
                                'time': minute,
                                'date': date,
                                'type': "red"
                            }
                            all_data.append(data)
                            
                for i in event_home:
                    # Lặp qua từng <ul> và tìm các div chứa thông tin sự kiện
                    b = i.find_all('div', class_="eventInfo")
                    
                    for info in b:  # Duyệt qua từng sự kiện trong danh sách b
                        # Kiểm tra xem có thông tin Yellow Card hay không
                        if "Yellow Card" in info.text:
                            player_name = info.find('a', class_='name').text.strip()

                            # Lấy ID cầu thủ từ href
                            player_id = info.find('a', class_='name')['href'].split('/')[2]
                            
                            minute = info.find('time', class_='min').text.strip()

                            
                            data = {
                                'player_id': player_id,
                                'match_code': match_id,
                                'time': minute,
                                'date': date,
                                'type': "yellow"
                            }
                            all_data.append(data)
                        if "Red Card" in info.text:
                            player_name = info.find('a', class_='name').text.strip()

                            # Lấy ID cầu thủ từ href
                            player_id = info.find('a', class_='name')['href'].split('/')[2]
                            minute = info.find('time', class_='min').text.strip()


                    
                            data = {
                                'player_id': player_id,
                                'match_code': match_id,
                                'time': minute,
                                'date': date,
                                'type': "red"
                            }
                            all_data.append(data)
            except:
                continue
            raw_data = pd.DataFrame(all_data)
            
        driver.quit()

        
    except Exception as e:
        context.log.error(f"Error while extracting data from MySQL: {e}")
        raw_data = pd.DataFrame(columns = ['player_id', 'time', 'match_code','date'])
        
    return Output(
        raw_data, 
        metadata={
            "table": 'card', 
            "column_count": len(raw_data.columns),
            "column_names": raw_data.columns.to_list(),
            "records": len(raw_data)
            }
        ) 
    
