const mysql = require('mysql');

const connSettings = { host: "localhost", user: "root", password: "root", database: "region_home_school" }
const db = mysql.createConnection(connSettings);
db.connect((err) => { if (err) throw err; console.log("Connected to database"); });

db.tenure = require('./tenure')(db)

module.exports = db