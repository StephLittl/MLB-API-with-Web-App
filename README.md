Basic idea: Python was used to load data from Pybaseball and the MLB Stats API into a PostgreSQL database. This was used to supply data for my own API, which provides player, 
team, game, and pitch-by-pitch information. Only data from 2023 was used.

Used for this project:

->Python (virtual environment)
  
->Pybaseball, MLB Stats API

->Node

->Express

->Vue

To run the API: in the project directory, run the script:

        node server.js
  
Then go to http://localhost:8080 in a browser
  
To run the web app: first deploy the API using node server.js, then in the baseball-app directory run the script:
  
        npm run serve

Then go to http://localhost:8081 in a browser

http://localhost:8080/help
  decribes in more detail the endpoints and field options

API examples:

/teams?team=CLE

->returns the Cleveland's team stats

/teams?attr=hr

->returns the every teams stats, sort by number of home runs

/batters?team=Cleveland&attr=hr

->returns all of Cleveland's hitters, sorted by number of home runs

/batters?last_name=Kwan

->returns the stats of every hitter with the last name Kwan

/batters?obp=.35&min_pas=100&attr=obp

->returns all batters with OBP greater than 0.350 who have had at least 100 at bats, sorted by OBP

/pitchers?last_name=Bieber

->returns the pitching stats for all pitchers with last name Bieber

/pitchers?team=Cleveland&attr=w

->returns all the Cleveland pitchers, sorted by their number of wins

/pitchers?era=3&min_innings=40

->returns all pitchers who have ERA less than 3 and who have pitched at least 40 innings

/games?team=CLE&home=true

->returns all of Cleveland's home games

/games?startDate=2023-06-01&endDate2023-06-30

->returns all games in June

/game_pitches?pitcher=Bieber&hitter=Devers

->returns the details of every pitch Bieber has thrown to Devers

/game_pitches?team=CLE&startDate=2023-04-05&endDate=2023-04-05

->returns the details of all of the pitches in Cleveland's game on 2023-04-05
