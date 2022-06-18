const fs = require('fs');
const db = require('../domain/infra/db')
const service = require('../domain/services/etl')
const dataPath = "/Users/hudsonmendes/Datasets/properties/property-prices";

const get = (_, res) => {
    return res.render("etl");
}

const post = async (_, res) => {
    try {
        const filenames = fs.readdirSync(dataPath);
        for (let filename of filenames) {
            const filepath = `${dataPath}/${filename}`
            const filecsv = service.extract(filepath)
            for await (const csvline of filecsv) {
                const record = service.transform(csvline)
                await service.load(db, record)
            }
            res.redirect("etl");
        }
    }
    catch (e) {
        console.error(e)
        res.write(e.toString())
        res.end()
    }
};

module.exports = { get, post }