function tenureFactory(db) {
    return {
        findByName: function (name) {
            return new Promise((_, resolve) => {
                db.query(
                    "SELECT id FROM tenures BY name = ?",
                    [name],
                    (_, results) => resolve(results))
            })
        }
    }
}

module.exports = tenureFactory