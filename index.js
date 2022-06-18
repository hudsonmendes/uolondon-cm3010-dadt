// Import libraries
const express = require('express');
const bodyParser = require('body-parser');
const mysql = require('mysql');
const mustacheExpress = require('mustache-express');
const fs = require('fs');
const tdqm = require(`tqdm`);
const csv = require("csv-parse");
const tqdm = require('tqdm');

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

app.use(express.static('public'))

app.get('/', function (req, res) {
    res.render("index")
});

app.get('/etl', function (req, res) {
    res.render("etl")
});

app.post('/etl', function (req, res) {
    // https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#using-or-publishing-our-price-paid-data
    // check for "Address Data"
    const csvHeader = {
        price: 1, dateOfTransfer: 2,
        postcode: 3, propertyType: 4,
        oldOrNew: 5, leaseType: 6,
        propertyNumberOrName: 7, buildingOrBlock: 8, streetName: 9,
        locality: 10, townOrCity: 11, district: 12, county: 13,
        ppdCategoryType: 14 }

    const csvPropertyType = {
        D: 'Detached',
        S: 'Semi-detached',
        T: 'Terraced',
        F: 'Flats/Maisonettes',
        O: 'Other'
    }

    const csvPropertyDuration = {
        F: 'Freehold',
        L: 'Leasehold',
    }

    const filenames = fs.readdirSync(dataset.folder);
    for (let filename of tqdm(filenames)) {
        filepath = `${dataset.folder}/${filename}`
        fs.createReadStream(filepath)
            .pipe(csv.parse())
            .on("data", console.log)
    }
    
    return res.redirect("etl")
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

app.listen(webPort, () => console.log('EMO app listening on port ' + webPort)); // success callback
