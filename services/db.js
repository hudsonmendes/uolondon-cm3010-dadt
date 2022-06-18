const util = require('util');
const mysql = require('mysql');

const connSettings = { host: "localhost", user: "root", password: "root", database: "region_home_school" }
const db = mysql.createConnection(connSettings);
util.promisify(db.query).bind(db)
db.connect((err) => { if (err) throw err; console.log("Connected to database"); });

modules.export = db