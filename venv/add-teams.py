import psycopg2
import pybaseball

# PostgreSQL connection details
db_params = {
    "host": "localhost",
    "database": "mlb",
    "user": "mlb_user",
    "password": "baseball",
}
print('um hi')

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)

# Cursor
cursor = conn.cursor()

teams_2023 = pybaseball.team_batting(2023)


# Create a table to store the data
create_table_query = """
CREATE TABLE IF NOT EXISTS teams (
    team_id SERIAL PRIMARY KEY,
    team VARCHAR(10),
    age VARCHAR(10), 
    AB INT,
    PA INT,
    H INT,
    oneB INT,
    twoB INT,
    threeB INT,
    HR INT,
    R INT,
    RBI INT,
    BB INT,
    IBB INT,
    SO INT,
    HBP INT,
    SF INT,
    SH INT,
    GDP INT,
    SB INT,
    CS INT,
    AVG FLOAT,
    BBpct FLOAT,
    Kpct FLOAT,
    OBP FLOAT,
    SLG FLOAT,
    OPS FLOAT
);
"""
cursor.execute(create_table_query)

# Insert data into the table
for _, team in teams_2023.iterrows():
    insert_query = """
    INSERT INTO teams (team, age, AB, PA, H, oneB, twoB, threeB, HR, R, RBI, BB, IBB, SO, HBP, SF, SH, GDP, SB, CS, AVG, BBpct, Kpct, OBP, SLG, OPS)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_query, (team['Team'], team['Age'], team['AB'], team['PA'], team['H'], team['1B'], team['1B'], team['3B'], team['HR'], team['R'], team['RBI'], team['BB'], team['IBB'], team['SO'], team['HBP'], team['SF'], team['SH'], team['GDP'], team['SB'], team['CS'], team['AVG'], team['BB%'], team['K%'], team['OBP'], team['SLG'], team['OPS']))
# Commit the changes and close the connection
conn.commit()
cursor.close()
conn.close()

print("Data loaded into the PostgreSQL database.")
