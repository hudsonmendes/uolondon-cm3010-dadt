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

// result mapping
const resultMap = r => ({
    locality: r.locality,
    postcode: r.postgroup,
    avgHomePrice: `£ ${r.average_price.toLocaleString('en-US')}`,
    avgOfstedRating: r.average_rating ? r.average_rating.toFixed(1) : "N/A",
    avgOfstedOutstanding: r.average_rating && r.average_rating >= 4,
    avgOfstedVeryGood: r.average_rating && r.average_rating >= 3.5 && r.average_rating < 4,
    avgOfstedGood: r.average_rating && r.average_rating >= 2.5 && r.average_rating < 3.5,
    avgOfstedNeedsImprovement: r.average_rating && r.average_rating >= 1.5 && r.average_rating < 2.5,
    avgOfstedInadequate: r.average_rating && r.average_rating < 1.5,
})

// routes
app.get('/', (req, res) => {
    db.query(
        "SELECT id, name FROM postgroups ORDER BY name",
        (_, results) => {
            const postgroups = results.map(r => ({
                value: r.id,
                text: r.name,
                selected: (req.query.postgroupIds && (
                    req.query.postgroupIds == r.id.toString() ||
                    (Array.isArray(req.query.postgroupIds.map) && req.query.postgroupIds.indexOf(r.id.toString()) >= 0)))
            }))

            const priceRangesStart = [...Array(20).keys()].map(i => 100000 + (i * 50000)).map(v => ({
                value: v,
                text: `£ ${v.toLocaleString('en-US')}`,
                selected: v.toString() == req.query.priceRangeStart
            }))

            const priceRangesEnd = [...Array(20).keys()].map(i => 100000 + (i * 50000)).map(v => ({
                value: v,
                text: `£ ${v.toLocaleString('en-US')}`,
                selected: v.toString() == req.query.priceRangeEnd
            }))

            const data = {
                query: req.query,
                postgroups,
                priceRangesStart,
                priceRangesEnd
            }

            if (req.query.mode == "price-range" && req.query.postgroupIds && req.query.priceRangeStart && req.query.priceRangeEnd) {
                const sql = `
                    SELECT
                        pg.name "postgroup",
                        l.name "locality",
                        AVG(pt.price) "average_price",
                        (SELECT AVG(sr.rating)
                            FROM postcodes pc2
                            INNER JOIN schools s ON s.postcode_id = pc2.id
                            INNER JOIN school_ratings sr ON sr.school_id = s.id
                            WHERE pc2.postgroup_id = pg.id) "average_rating"
                    FROM localities l
                        INNER JOIN localities_postcodes lpc ON lpc.locality_id = l.id
                        INNER JOIN postcodes pc1 ON pc1.id = lpc.postcode_id
                        INNER JOIN postgroups pg ON pg.id = pc1.postgroup_id
                        INNER JOIN properties p ON p.postcode_id = pc1.id
                        INNER JOIN property_transactions pt ON pt.property_id = p.id
                    WHERE (pg.id = -1 OR pg.id in (?))
                    GROUP BY pg.name, l.name
                    HAVING AVG(pt.price) BETWEEN ? AND ?
                    ORDER BY pg.name, l.name`.trim()
                const params = [
                    req.query.postgroupIds
                        ? (req.query.postgroupIds.map ? req.query.postgroupIds.map(id => parseInt(id)) : parseInt(req.query.postgroupIds))
                        : null,
                    parseInt(req.query.priceRangeStart), 
                    parseInt(req.query.priceRangeEnd)]
                db.query(sql, params, (_, results) => {
                    data['results'] = results.map(resultMap)
                    return res.render("index", data)
                })
            }
            else if (req.query.mode == "place-name" && req.query.placeName) {
                const sql = `
                    SELECT
                        pg.name "postgroup",
                        l.name "locality",
                        AVG(pt.price) "average_price",
                        (SELECT AVG(sr.rating)
                            FROM postcodes pc2
                            INNER JOIN schools s ON s.postcode_id = pc2.id
                            INNER JOIN school_ratings sr ON sr.school_id = s.id
                            WHERE pc2.postgroup_id = pg.id) "average_rating"
                    FROM localities l
                        INNER JOIN localities_postcodes lpc ON lpc.locality_id = l.id
                        INNER JOIN postcodes pc1 ON pc1.id = lpc.postcode_id
                        INNER JOIN postgroups pg ON pg.id = pc1.postgroup_id
                        INNER JOIN properties p ON p.postcode_id = pc1.id
                        INNER JOIN property_transactions pt ON pt.property_id = p.id
                    WHERE l.name LIKE ?
                    GROUP BY pg.name, l.name
                    ORDER BY pg.name, l.name`.trim()
                const params = [`${req.query.placeName}%`]
                db.query(sql, params, (_, results) => {
                    data['results'] = results.map(resultMap)
                    return res.render("index", data)
                })
            }
            else
                return res.render("index", data)
        })
});

app.listen(webPort, () => console.log('App Started, listening http://127.0.0.1:' + webPort)); // success callback
