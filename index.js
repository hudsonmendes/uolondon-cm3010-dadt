// Import libraries
const express = require('express');
const bodyParser = require('body-parser');
const mysql = require('mysql');
const mustacheExpress = require('mustache-express');
const fs = require('fs');

// Initialise objects and declare constants
const app = express();
const webPort = 8088;

const dataset = {
    folder: "/Users/hudsonmendes/Datasets/properties/property-prices"
}

const db = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "root",
    database: "Astronomy"
});

app.engine('html', mustacheExpress());
app.set('view engine', 'html');
app.set('views', './templates');
app.use(bodyParser.urlencoded({ extended: true }));

app.post('/etl', function (req, res) {
    const filenames = fs.readdirSync(dataset.folder);
    filenames.forEach(console.log);
});

app.get('/search', (req, res) => {
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
})

app.get('/', function (req, res) {
    res.render("index")
});

app.listen(webPort, () => console.log('EMO app listening on port ' + webPort)); // success callback
