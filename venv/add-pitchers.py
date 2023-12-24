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
CREATE TABLE IF NOT EXISTS pitchers (
    player_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    team VARCHAR(100),
    G INT,
    GS INT,
    W INT,
    L INT,
    SV INT,
    IP FLOAT,
    H INT,
    R INT,
    ER INT,
    BB INT,
    SO INT,
    HR INT,
    HBP INT,
    ERA FLOAT,
    AB INT,
    TWOB INT,
    THREEB INT,
    IBB INT,
    GDP INT,
    SF INT,
    SB INT,
    CS INT,
    PO INT,
    BF INT,
    Pit INT,
    Str FLOAT,
    StL FLOAT,
    StS FLOAT,
    GB_slash_FB FLOAT,
    LD FLOAT,
    PU FLOAT,
    WHIP FLOAT,
    BAbip FLOAT,
    SOnine FLOAT,
    SO_slash_W FLOAT,
    mlbID BIGINT
);
"""
cursor.execute(create_table_query)

pitchers = pybaseball.pitching_stats_bref(2023)
pitchers = pitchers.fillna(0.0)
print(pitchers['H'])
print(pitchers['R'])
print(pitchers.columns)

for _, pitcher in pitchers.iterrows(): 
    insert_query = """ 
    INSERT INTO pitchers (first_name, last_name, age, team, G, GS, W, L, SV, IP, H, R, ER, BB, SO, HR, HBP, ERA, AB, TWOB, THREEB, IBB, GDP, SF, SB, CS, PO, BF, Pit, Str, StL, StS, GB_slash_FB, LD, PU, WHIP, BAbip, SOnine, SO_slash_W, mlbID)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """   
    #40
    first_space_index = pitcher['Name'].find(' ')
    if first_space_index != -1:
        # Split the string at the first space
        first_name = pitcher['Name'][:first_space_index]
        last_name = pitcher['Name'][first_space_index + 1:]
    else:
        first_name = ""
        last_name = pitcher['Name']
    #27    

    cursor.execute(insert_query, (first_name, last_name, pitcher['Age'], pitcher['Tm'], pitcher['G'], pitcher['GS'], int(pitcher['W']), int(pitcher['L']), int(pitcher['SV']), pitcher['IP'], int(pitcher['H']), int(pitcher['R']), int(pitcher['ER']), int(pitcher['BB']), int(pitcher['SO']), int(pitcher['HR']), int(pitcher['HBP']), pitcher['ERA'], int(pitcher['AB']), int(pitcher['2B']), int(pitcher['3B']), int(pitcher['IBB']), int(pitcher['GDP']), int(pitcher['SF']), int(pitcher['SB']), int(pitcher['CS']), int(pitcher['PO']), int(pitcher['BF']), pitcher['Pit'], pitcher['Str'], pitcher['StL'], pitcher['StS'], pitcher['GB/FB'], pitcher['LD'], pitcher['PU'], pitcher['WHIP'], pitcher['BAbip'], pitcher['SO9'], pitcher['SO/W'], pitcher['mlbID']))
    print('added ', last_name)

conn.commit()
cursor.close()
conn.close()

print("Data loaded into the PostgreSQL database.")
