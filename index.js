// Import libraries
const fs = require('fs');
const util = require('util');
const exec = util.promisify(require('child_process').exec);
const express = require('express');
const mustacheExpress = require('mustache-express');
const tqdm = require('tqdm');
const bodyParser = require('body-parser');
const mysql = require('mysql');
const csv = require("csv-parse");

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
    database: "region_home_school"
});
db.connect((err) => { if (err) throw err; console.log("Connected to database"); });
util.promisify(db.query).bind(db)

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

app.post('/etl', async function (req, res) {
    // https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#using-or-publishing-our-price-paid-data
    // check for "Address Data"
    const csvHeader = {
        price: 1, dateOfTransfer: 2,
        postcode: 3, propertyType: 4,
        oldOrNew: 5, tenureType: 6,
        propertyNumberOrName: 7, buildingOrBlock: 8, streetName: 9,
        locality: 10, townOrCity: 11, district: 12, county: 13,
        ppdCategoryType: 14
    }

    const csvPropertyType = {
        D: 'Detached',
        S: 'Semi-detached',
        T: 'Terraced',
        F: 'Flats/Maisonettes',
        O: 'Other'
    }

    const csvTenureType = {
        F: 'Freehold',
        L: 'Leasehold',
    }

    const filenames = fs.readdirSync(dataset.folder);
    for (let filename of tqdm(filenames)) {
        const filepath = `${dataset.folder}/${filename}`
        const filelen  = parseInt(await exec(`cat ${filepath} | wc -l`)).stdout.trim()
        const filehandler = fs.createReadStream(filepath)
        const fileprogress = tqdm(filehandler, {total: filelen})
        const csvparser = csv.parse()
        for await (const line of fileprogress) {
            const values = csvparser(line)
            const row = {
                price: values[csvHeader.price],
                dateOfTransfer: values[csvHeader.dateOfTransfer],
                postcode: values[csvHeader.postcode],
                propertyType: csvPropertyType[values[csvHeader.propertyType]],
                oldOrNew: values[csvHeader.oldOrNew],
                tenureType: csvTenureType[values[csvHeader.tenureType]],
                propertyNumberOrName: values[csvHeader.propertyNumberOrName],
                buildingOrBlock: values[csvHeader.buildingOrBlock],
                streetName: values[csvHeader.streetName],
                locality: values[csvHeader.locality],
                townOrCity: values[csvHeader.townOrCity],
                district: values[csvHeader.district],
                county: values[csvHeader.county],
                ppdCategoryType: values[csvHeader.ppdCategoryType]
            };
            console.log(row);
        }
        res.redirect("etl");
    }
});

app.get('/search', async (req, res) => {
    const { data } = await db.query("SELECT * FROM Planets");
    return res.render("search", { data });
})

app.listen(webPort, () => console.log('EMO app listening on port ' + webPort)); // success callback
