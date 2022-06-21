const fs = require('fs')

// Import libraries
const express = require('express');
const bodyParser = require('body-parser');
const mysql = require('mysql');
const ini = require('ini')
const mustacheExpress = require('mustache-express');

// Loading config
const configPath = './config.ini'
if (!fs.existsSync(configPath)) {
    console.warn("Running from `./web` folder, copying config")
    fs.copyFileSync("../config.ini", configPath) // failsafe
}
var config = ini.parse(fs.readFileSync(configPath, 'utf-8'))
console.log(`Config loaded from "${configPath}"`)
const basePath = !process.cwd().endsWith("web") ? './web' : './'

// Initialise objects and declare constants
const app = express();
const webPort = config.web.port;
const db = mysql.createConnection(config.mysql);

// serve static files
app.use(express.static(`${basePath}/public`))

// template engine
app.engine('html', mustacheExpress());
app.set('view engine', 'html');
app.set('views', `${basePath}/templates`);
app.use(bodyParser.urlencoded({ extended: true }));

app.get('/', (req, res) => {
    db.query(
        "SELECT id, name FROM postgroups ORDER BY name",
        (_, results) => {
            const postgroups = results.map(r => ({ value: r.id, text: r.name }))
            const priceRanges = [...Array(10).keys()].map(i => 100000 + (i * 50000)).map(v => ({ value: v, text: v.toLocaleString('en-US') }))
            const data = {postgroups, priceRanges}

            if (req.query.mode == "price-range-and-postcode") {
                db.query(`
                    SELECT pg.name "postgroup", AVERAGE(pt.price), AVERAGE(sr.rating)
                    FROM places_postgroups ppg
                        INNER JOIN postgroups pg ON ppg.postgroup_id = pg.postgroup_id
                        INNER JOIN postcodes pc ON pg.id = pc.postgroup_id
                        INNER JOIN properties pr ON pc.id = pr.postcode_id
                        INNER JOIN property_transactions pt ON pr.id = pt.property_id
                        INNER JOIN schools s ON pc.id = s.postcode_id
                        INNER JOIN school_ratings sr ON s.id = sr.school_id
                    WHERE ppg.postgroup_id in (?)
                    GROUP BY pg.name
                    HAVING AVERAGE(pt.price) BETWEEN ? AND ?`
                    [ req.query.postgroupIds, req.query.priceRangeStart, req.query.priceRangeEnd ],
                    (_, results) => {
                        data['results'] = results
                        return res.render("index", data)
                    })
            }
            else if (req.query.mode == "place-name") {
                db.query(`
                    SELECT pg.name "postgroup", AVERAGE(pt.price), AVERAGE(sr.rating)
                    FROM places p
                        INNER JOIN places_postgroups ppg ON p.id = ppg.place_id
                        INNER JOIN postgroups pg ON ppg.postgroup_id = pg.postgroup_id
                        INNER JOIN postcodes pc ON pg.id = pc.postgroup_id
                        INNER JOIN properties pr ON pc.id = pr.postcode_id
                        INNER JOIN property_transactions pt ON pr.id = pt.property_id
                        INNER JOIN schools s ON pc.id = s.postcode_id
                        INNER JOIN school_ratings sr ON s.id = sr.school_id
                    WHERE p.name LIKE (?)
                    GROUP BY pg.name`
                    [ `${req.query.placeName}%` ],
                    (_, results) => {
                        data['results'] = results
                        return res.render("index", data)
                    })
            }
            else
                return res.render("index", data)
        })
});

app.listen(webPort, () => console.log('App Started, listening http://127.0.0.1:' + webPort)); // success callback
