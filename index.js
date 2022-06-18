// Import libraries
const express = require('express');
const bodyParser = require('body-parser');
const mysql = require('mysql');
const mustacheExpress = require('mustache-express');

// Initialise objects and declare constants
const app = express();
const webPort = 8088;

const db = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "root",
    database: "region_home_school"
});

app.engine('html', mustacheExpress());
app.set('view engine', 'html');
app.set('views', './');
app.use(bodyParser.urlencoded({ extended: true }));

app.use(express.static('public'))

app.get('/', function (req, res) {
    try {
        db.connect((err) => { if (err) throw err; console.log("Connected to database"); });
        db.query(
            "SELECT * FROM Planets",
            (error, results, _) => {
                if (error) throw error;
                res.render("search", { data: results });
            });
    }
    finally {
        db.end()
    }
});

app.listen(webPort, () => console.log('EMO app listening on port ' + webPort)); // success callback
