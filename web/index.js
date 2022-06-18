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

// Initialise objects and declare constants
const app = express();
const webPort = config.web.port;
const db = mysql.createConnection(config.mysql);

app.engine('html', mustacheExpress());
app.set('view engine', 'html');
app.set('views', './web');
app.use(bodyParser.urlencoded({ extended: true }));

app.get('/', (_, res) => res.render("index"));

app.listen(webPort, () => console.log('App Started, listening http://127.0.0.1:' + webPort)); // success callback
