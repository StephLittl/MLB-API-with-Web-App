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



# Create a table to store the data
create_table_query = """
CREATE TABLE IF NOT EXISTS batters (
    player_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    team VARCHAR(100),
    G INT,
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
    GDP INT,
    SB INT,
    CS INT,
    BA FLOAT,
    OBP FLOAT,
    SLG FLOAT,
    OPS FLOAT,
    mlbID INT
);
"""
cursor.execute(create_table_query)

batters = pybaseball.batting_stats_bref(2023)
print(batters)

for _, batter in batters.iterrows():
    insert_query = """
    INSERT INTO batters (first_name, last_name, age, team, G, PA, AB, R, H, TWOB, THREEB, HR, RBI, BB, IBB, SO, HBP, SH, SF, GDP, SB, CS, BA, OBP, SLG, OPS, mlbID)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    first_space_index = batter['Name'].find(' ')
    if first_space_index != -1:
        # Split the string at the first space
        first_name = batter['Name'][:first_space_index]
        last_name = batter['Name'][first_space_index + 1:]
    else:
        first_name = ""
        last_name = batter['Name']
    #27
    cursor.execute(insert_query, (first_name, last_name, batter['Age'], batter['Tm'], batter['G'], batter['PA'], batter['AB'], batter['R'], batter['H'], batter['2B'], batter['3B'], batter['HR'], batter['RBI'], batter['BB'], batter['IBB'], batter['SO'], batter['HBP'], batter['SH'], batter['SF'], batter['GDP'], batter['SB'], batter['CS'], batter['BA'], batter['OBP'], batter['SLG'], batter['OPS'], batter['mlbID']))




conn.commit()
cursor.close()
conn.close()

print("Data loaded into the PostgreSQL database.")
