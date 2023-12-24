/*
const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");

const app = express();

var corsOptions = {
  origin: "http://localhost:8081"
};

app.use(cors(corsOptions));

// parse requests of content-type - application/json
app.use(bodyParser.json());

// parse requests of content-type - application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: true }));

const db = require("./app/models");
db.sequelize.sync({ force: true }).then(() => {
    console.log("Drop and re-sync db.");
  });

// simple route
app.get("/", (req, res) => {
  res.json({ message: "Welcome to Steph's Fabuluous App" });
});

// set port, listen for requests
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}.`);
});
*/
const express = require('express')
const { Pool } = require('pg');
const cors = require("cors");
const app = express()
const port = 8080

const pool = new Pool({
  user: 'mlb_user',
  host: 'localhost',
  database: 'mlb',
  password: 'baseball',
  port: 5432,
});

app.get('/', (req, res) => {
  res.send('Welcome to my fabulous app!')
})

app.use(express.json());
app.use(cors());


app.get('/teams', async (req, res) => {
  try {
    let { name, attr }= req.query;
    let query = 'SELECT * FROM teams WHERE 1 = 1';

    const parameters = [];

    if (name !== undefined && name !== null && name !== '') {
      query += ' AND team = $' + (parameters.length + 1);
      parameters.push(name);
    }
    
    let possible_values = ['hr', 'r', 'bb', 'age']
    if (possible_values.includes(attr)) {
      const validAttributes = ['hr', 'r', 'bb', 'age'];
      if (validAttributes.includes(attr)) {
        query += ' ORDER BY ' + attr + ' DESC'; 
        //parameters.push(attr);
      } else {
        res.status(400).json({ error: 'Invalid attribute for sorting' });
        return;
      }
    }
    
    const { rows } = await pool.query(query, parameters);
    res.status(200).json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('failed');
  }
});

app.get('/batters', async (req, res) => {
  try {
    let { team, last_name, obp, min_pas, attr } = req.query;
    
    let query = 'SELECT * FROM batters WHERE 1=1';
    
    const parameters = [];

    if (team) {
      query += ' AND team like $' + (parameters.length + 1);
      parameters.push(team);
    }    
    if (last_name) {
      query += ' AND last_name = $' + (parameters.length + 1);
      parameters.push(last_name);
    }
    if (obp) {
      query += ' AND obp < $' + (parameters.length + 1) + '::float';
      parameters.push(obp);
    }
    if (min_pas) {
      query += ' AND pa > $' + (parameters.length + 1) + '::int';
      parameters.push(min_pas);
    }
    let possible_values = ['ba', 'obp', 'slg', 'hr', 'bb']
      if (possible_values.includes(attr)) {
      const validAttributes = ['ba', 'obp', 'slg', 'hr', 'bb'];
      if (validAttributes.includes(attr) && attr != "") {
          query += ' ORDER BY ' + attr + ' DESC'; 
      } else {
        res.status(400).json({ error: 'Invalid attribute for sorting' });
        return;
      }
    }
    const { rows } = await pool.query(query, parameters);
    res.status(200).json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Failed');
  }
});


app.get('/pitchers', async (req, res) => {
  try {
    let { team, last_name, era, min_innings, attr } = req.query;
    
    let query = 'SELECT * FROM pitchers WHERE 1=1';
    
    const parameters = [];

    if (team) {
      query += ' AND team like $' + (parameters.length + 1);
      parameters.push(team);
    }    
    if (last_name) {
      query += ' AND last_name = $' + (parameters.length + 1);
      parameters.push(last_name);
    }
    if (era) {
      query += ' AND era < $' + (parameters.length + 1) + '::float';
      parameters.push(era);
    }
    if (min_innings) {
      query += ' AND ip > $' + (parameters.length + 1) + '::float';
      parameters.push(min_innings);
    }
    possible_values = ['w', 'g', 'ip', 'whip', 'era'];
    if (possible_values.includes(attr)) {
      const validAttributes = ['w', 'g', 'ip', 'whip', 'era'];
      if (validAttributes.includes(attr)) {
        // Use string interpolation for the column name in the ORDER BY clause
        if (attr == 'w' || attr == 'g' || attr == 'ip'){
          query += ' ORDER BY ' + attr + ' DESC'; // or ASC, depending on your needs
        }else{
          query += ' ORDER BY ' + attr + ' ASC';
        }
      } else {
        // Handle invalid 'attr' values
        res.status(400).json({ error: 'Invalid attribute for sorting' });
        return;
      }
    }
    const { rows } = await pool.query(query, parameters);
    res.status(200).json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Failed');
  }
});

// get all games for a team
app.get('/games', async (req, res) => {
  try {
    let { team, home, startDate, endDate } = req.query;
    
    
    let query = 'SELECT * FROM games WHERE 1=1';
    
    const parameters = [];

    if (team) {
      query += ' AND team = $' + (parameters.length + 1);
      parameters.push(team);
    }    
    if (home) {
      const homeOrAway = '$' + (parameters.length + 1) + '::boolean';
      query += ' AND home = ' + homeOrAway;
      parameters.push(home);
    }
    
    if (startDate && endDate) {
      query += ' AND g_date BETWEEN $' + (parameters.length + 1) + ' AND $' + (parameters.length + 2);
      parameters.push(startDate, endDate);
    } else if (startDate) {
      query += ' AND g_date >= $' + (parameters.length + 1);
      parameters.push(startDate);
    } else if (endDate) {
      query += ' AND g_date <= $' + (parameters.length + 1);
      parameters.push(endDate);
    }
    const { rows } = await pool.query(query, parameters);
    res.status(200).json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Failed');
  }
});

app.get('/game_pitches', async (req, res) => {
  try {
    let { pitcher, batter, team, startDate, endDate } = req.query;
    
    let query = 'SELECT * FROM game_pitches WHERE 1=1';
    
    const parameters = [];

    if (pitcher) {
      query += ' AND pitcher = (SELECT mlbID FROM pitchers WHERE last_name = $' + (parameters.length + 1) + ')';
      parameters.push(pitcher);
    }
    
    if (batter) {
      query += ' AND batter = (SELECT mlbID FROM batters WHERE last_name = $' + (parameters.length + 1) + ')';
      parameters.push(batter);
    }
    
    if (team) {
      query += ' AND (home_team = $' + (parameters.length + 1) + ' OR away_team = $' + (parameters.length + 2) + ')';
      parameters.push(team, team);
    }
    
    if (startDate && endDate) {
      query += ' AND g_date BETWEEN $' + (parameters.length + 1) + ' AND $' + (parameters.length + 2);
      parameters.push(startDate, endDate);
    } else if (startDate) {
      query += ' AND g_date >= $' + (parameters.length + 1);
      parameters.push(startDate);
    } else if (endDate) {
      query += ' AND g_date <= $' + (parameters.length + 1);
      parameters.push(endDate);
    }
    
    const { rows } = await pool.query(query, parameters);
    res.status(200).json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Failed');
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});