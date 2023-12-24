import psycopg2
import pybaseball
import requests

api_url = "https://statsapi.mlb.com/api/v1/schedule"
params = {
    "sportId": 1,
    "startDate": "2023-01-01",
    "endDate": "2023-12-31",
    "gameType": "R",
    "fields": "dates,date,games,gamePk,status,abstractGameState,teams,away,home,team,id,name,gameDate",
}
response = requests.get(api_url, params=params)
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    pks = []
    # Extract relevant information from the response
    # For example, printing the game dates
    for date_info in data['dates']:
        for game_info in date_info['games']:
            pks.append(game_info['gamePk'])
else:
    print(f"Error: {response.status_code} - {response.text}")


#PostgreSQL connection details
db_params = {
    "host": "localhost",
    "database": "mlb",
    "user": "mlb_user",
    "password": "baseball",
}

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)

# Cursor
cursor = conn.cursor()

data = pybaseball.statcast_single_game(529429)

create_table_query = """
CREATE TABLE IF NOT EXISTS game_pitches (
    player_game_id SERIAL PRIMARY KEY,
    game_pk INT,
    game_date VARCHAR(20),
    batter INT,
    pitcher INT,
    home_team VARCHAR(10), 
    away_team VARCHAR(10),
    inning INT,
    top_bot VARCHAR(3),
    pitch_type VARCHAR(10),
    balls INT,
    strikes INT,
    hit_location FLOAT,
    pitch_number INT,
    on_first FLOAT,
    on_second FLOAT,
    on_third FLOAT,
    home_score INT,
    away_score INT, 
    post_home_score INT,
    post_away_score INT,
    launch_speed FLOAT,
    launch_angle FLOAT,
    hit_distance_sc FLOAT,
    CONSTRAINT batter_exists FOREIGN KEY (batter) REFERENCES batters(mlbID),
    CONSTRAINT pitcher_exists FOREIGN KEY (pitcher) REFERENCES pitchers(mlbID)
);
"""

#     CONSTRAINT home_exists FOREIGN KEY (home_team) REFERENCES baseball_teams(abbreviation),
#    CONSTRAINT away_exists FOREIGN KEY (away_team) REFERENCES baseball_teams(abbreviation)
cursor.execute(create_table_query)

#single_games = pybaseball.statcast_single_game(529429)
print(len(pks))
count = 0
for pk in pks:
    count += 1
    single_games = pybaseball.statcast_single_game(pk)
    for _, pitch in single_games.iterrows():
        insert_query = """
        INSERT INTO game_pitches (game_pk, game_date, batter, pitcher, home_team, away_team, inning, top_bot, pitch_type, balls, strikes, hit_location, pitch_number, on_first, on_second, on_third, home_score, away_score, post_home_score, post_away_score, launch_speed, launch_angle, hit_distance_sc)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        #print(pitch['home_team'])
        cursor.execute(insert_query, (pk, pitch['game_date'], pitch['batter'], pitch['pitcher'], pitch['home_team'], pitch['away_team'], pitch['inning'], pitch['inning_topbot'], pitch['pitch_type'], pitch['balls'], pitch['strikes'], pitch['hit_location'], pitch['pitch_number'], pitch['on_1b'], pitch['on_2b'], pitch['on_3b'], pitch['home_score'], pitch['away_score'], pitch['post_home_score'], pitch['post_away_score'], pitch['launch_speed'], pitch['launch_angle'], pitch['hit_distance_sc']))
    if count % 100 == 0:
        print(count)



#sched =pybaseball.schedule_and_record(2023, "BOS")
#print(sched.columns)

conn.commit()
cursor.close()
conn.close()

print("Data loaded into the PostgreSQL database.")
