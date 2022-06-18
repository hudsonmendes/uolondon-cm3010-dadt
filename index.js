// Import libraries
const fs = require('fs');
const express = require('express');
const mustacheExpress = require('mustache-express');
const bodyParser = require('body-parser');

// load controllers
const rest = {
    home: require('./rest/home'),
    etl: require('./rest/home'),
    search: require('./rest/search')
}

// Initialise objects and declare constants
const app = express();
const webPort = 8088;

// Register Static & View configs
app.engine('html', mustacheExpress());
app.set('view engine', 'html');
app.set('views', './templates');
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static('public'))

// Register Routes
app.get('/', rest.home.get);
app.get('/search', rest.search.get);
app.get('/etl', rest.etl.get);
app.post('/etl', rest.etl.post);

// Listen to traffic
app.listen(webPort, () => console.log('EMO app listening on port ' + webPort)); // success callback
