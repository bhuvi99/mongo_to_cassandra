const cassandra = require("./cassandra");
const mongo = require("./mongo");
function startdumping(mongoDb, cassandraClient, template ) {

    let cassandraDb = cassandra(cassandraClient, template);
    let mongoDb = mongo(mongoDb);
    return dumpDataFromMongoToCassandra(mongoDb, cassandraDb, template);


}

async function dumpDataFromMongoToCassandra(mongo, cassandra) {

    let data = await mongo.getDocuments(1000);
    if (data.length === 0)
        return true;
    try {
        let result = await Promise.all(
            mongo.updateDocStatus(data),
            cassandra.insertDocs(data)
        );
    }
    catch (err) {
        
    }
    setTimeout(function repeat() {
        dumpDataFromMongoToCassandra
    }, 0);

}

module.exports = {
    startdumping
}