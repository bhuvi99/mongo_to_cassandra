const cassandra = function (cassandraClient, table, template) {

    var client = cassandraClient;

    return {
        insertDocs: function insertDocs(data, allowNull = false) {
            let preparedData = prepareData(template, data);
            return insertDocsHelper(client, table, preparedData);
        }
    };
}


const prepareData = function prepareData(template, data) {

    let preparedData = [];

    data.forEach(document => {
        let doc = extractData(template, document );
        if( doc ){
            prepareData.push( doc );
        } 
    });
    return preparedData;
}


const extractData = function extractData(template, document) {

    let doc = {};
    let templateKeys = Object.keys(template);
    
    templateKeys.forEach(key => {
        doc[key] = document[template[key]];
    });
    return doc;
}


const getValues = function getValues( doc, columnNames){

    let values = [];
    columnNames.forEach( column => {

            values.push( doc[ column ]);
    });
        
    return values;
}


const getColumnNames = function getColumnNames( doc ){
    return Object.keys();
}


const createBatch = function createBatch( table, data ){

    let dataBatch = 'BEGIN BATCH';
    data.forEach( doc => {
            let columnNames = getColumnNames( doc ).toString();
            let values = getValues( doc, columnNames).toString();
            dataBatch = dataBatch + 'INSERT INTO ' + table + '(' +
                        columnNames + ') VALUES (' +
                        values + ')';

    })
    dataBatch = dataBatch + 'APPLY BATCH';
}


const execute = function execute(client, query) {

    return client.execute(query);
}

const insertDocsHelper = function insertDocsHelper(client, table, data) {

    let batchInsert = createBatch( table, data );
    return execute( client, batchInsert);
}

module.exports = cassandra;