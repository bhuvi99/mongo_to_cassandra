const async = require("async");
const DOC_STATUS = "dump_status__123asf";
const DOC_STATUS_VALUE = null;
const getDocuments = function getDocuments( db, collection,  numberOfDocs){

    let query = {
        [DOC_STATUS] : [DOC_STATUS_VALUE]
    }
    return db.collection(collection).find(query).limit(numberOfDocs);
}

const $updateDocStatus = function $updateDocStatus(db, collection,doc,callback){

   let query = {
       _id: doc._id
   };
   let update = {
      $set: {
        [DOC_STATUS] : 1
      } 
   }

  return  db.collection( collection ).findOneAndUpdate(query, update);

}
const updateDocStatus = function updateDocStatus(db, collection, data ){
    
    return async.each( data, function( doc, asynccallback){
        $updateDocStatus(db, collection, doc)
        .catch( err => {
            throw new Error( err );
        }).finally( ()=>{
             asynccallback();
         })
    });

}


const mongo = function mongoDb( db, collection ){
    var db = db;
    var helpers = {

        getDocuments :  ( numberOfDocs) => {
           return  getDocuments( db, collection, numberOfDocs );
        },
        updateDocStatus : ( data )=>{
            return  updateDocStatus(db, collection, data );
        }

    }

    return helpers;
}

module.exports = mongo;