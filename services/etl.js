const fs = require('fs');
const csv = require('csv-parse')

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

function extract(filepath) {
    return fs.createReadStream(filepath)
        .pipe(csv.parse())
}

function transform(values) {
    return {
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
    }
}

async function load(record, db) {
    const rows = db.query("SELECT id FROM tenures WHERE name =?", [record.tenureType])
    if (!rows)
        await db.query("INSERT INTO tenures (name) VALUES (?)", [record.tenureType])
}

module.exports = { extract, transform, load }