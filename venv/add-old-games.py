import psycopg2
import pybaseball

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

# API endpoint 
cle_2023 = pybaseball.team_game_logs(season=2023, team="CLE")
print(cle_2023.columns)

# Create a table to store the data
create_table_query = """
CREATE TABLE IF NOT EXISTS games (
    game_id SERIAL PRIMARY KEY,
    team VARCHAR(10),
    home BOOLEAN,
    opponent VARCHAR(10),
    date VARCHAR(10),
    result VARCHAR(10),
    PA INT,
    AB INT,
    R INT,
    H INT,
    TWOB INT,
    THREEB INT,
    HR INT,
    RBI INT,
    BB INT,
    IBB INT,
    SO INT,
    HBP INT,
    SH INT,
    SF INT,
    ROE INT,
    GDP INT,
    SB INT,
    CS INT,
    BA FLOAT,
    OBP FLOAT,
    SLG FLOAT,
    OPS FLOAT,
    LOB INT,
    NumPlayers INT,
    Thr CHAR,
    OppStart VARCHAR(40),
    CONSTRAINT team_constraint FOREIGN KEY (team) REFERENCES baseball_teams(abbreviation),
    CONSTRAINT opponent_constraint FOREIGN KEY (opponent) REFERENCES baseball_teams(abbreviation) 
);
"""
cursor.execute(create_table_query)

teams_2023 = pybaseball.team_ids(season=2019)
print(teams_2023)

for _, team in teams_2023.iterrows():
    print('here')
    abbr = team['teamIDBR']
    logs = pybaseball.team_game_logs(season=2023, team=abbr)
    for _, game in logs.iterrows():
        insert_query = """
        INSERT INTO games (team, home, opponent, date, result, PA, AB, R, H, TWOB, THREEB, HR, RBI, BB, IBB, SO, HBP, SH, SF, ROE, GDP, SB, CS, BA, OBP, SLG, OPS, LOB, NumPlayers, Thr, OppStart)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (abbr, game['Home'], game['Opp'], game['Date'], game['Rslt'][0], game['PA'], game['AB'], game['R'], game['H'], game['2B'], game['3B'], game['HR'], game['RBI'], game['BB'], game['IBB'], game['SO'], game['HBP'], game['SH'], game['SF'], game['ROE'], game['GDP'], game['SB'], game['CS'], game['BA'], game['OBP'], game['SLG'], game['OPS'], game['LOB'], game['NumPlayers'], game['Thr'], game['OppStart']))



# Insert data into the table
#or _, team in teams_2019.iterrows():
#    insert_query = """
#    INSERT INTO baseball_teams (abbreviation, league)
#    VALUES (%s, %s);
#    """
#    cursor.execute(insert_query, (team['teamIDBR'], team['lgID']))
# Commit the changes and close the connection
conn.commit()
cursor.close()
conn.close()

print("Data loaded into the PostgreSQL database.")
