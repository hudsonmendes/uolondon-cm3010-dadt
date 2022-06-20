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

app.get('/', (_, res) => {
    db.query(
        "SELECT id, name FROM postgroups ORDER BY name",
        (_, results) => {
            const postgroups = results.map(r => ({ value: r.id, text: r.name }))
            const priceRanges = [...Array(10).keys()].map(i => 100000 + (i * 50000)).map(v => ({ value: v, text: v.toLocaleString('en-US') }))
            const data = {postgroups, priceRanges}
            return res.render("index", data)
        })
});

app.listen(webPort, () => console.log('App Started, listening http://127.0.0.1:' + webPort)); // success callback
