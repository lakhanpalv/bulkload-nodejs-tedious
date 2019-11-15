const Connection = require('tedious').Connection;
const Request = require('tedious').Request;
const TYPES = require('tedious').TYPES;
const events = require('events');
const eventEmitter = new events.EventEmitter();
const fs = require('fs').promises;
var dbConnection,bulkLoadTbl;

var dbConConfig = {
  userName: 'username',
  password: 'password',
  server: 'XXXXX',

  options: {
    port: 49175,
    database: 'databasename',
    instancename: 'SQLEXPRESS',
    rowCollectionOnRequestCompletion: true
  }
};

const arrDataTypes = [];
arrDataTypes['bit'] = TYPES.Bit;
arrDataTypes['char'] = TYPES.Char;
arrDataTypes['varchar'] = TYPES.VarChar;
arrDataTypes['datetime'] = TYPES.DateTime;
arrDataTypes['bigint'] = TYPES.BigInt;
arrDataTypes['smallint'] = TYPES.SmallInt;
arrDataTypes['int'] = TYPES.Int;
arrDataTypes['decimal'] = TYPES.Decimal;

function getDataType(type) {
  return arrDataTypes[type];
}

function cpDataToTable(fileData){
  let rows = fileData.split('\r\n');
  let colArr = [];
  let skipFirstRow = true;
  
  for(let row of rows){
    if(skipFirstRow){ //skip first row if it contains Column Names
      skipFirstRow = false;
      continue;
    }
    colArr = row.split(',');
    if(colArr.length > 1){
      bulkLoadTbl.addRow(colArr);
    }
  }
}

async function getTableMetaInfo(){
  const sql = "SELECT COL.COLUMN_NAME, COL.DATA_TYPE, COL.CHARACTER_MAXIMUM_LENGTH, COL.IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS COL WHERE COL.TABLE_NAME = 'myTable'";

  const request = new Request(sql, async (err, rowCount, rs) => {
    if (err) {
      console.log("Error in myTable table schema select SQL query " + err);
      process.exit(1);
    } else {
      let nullableVal;
      rs.forEach(function (metadata) {
        if (metadata[3].value == 'NO') {
          nullableVal = false;
        } else {
          nullableVal = true;
        }
        bulkLoadTbl.addColumn(metadata[0].value, getDataType(metadata[1].value), {
          length: metadata[2].value,
          nullable: nullableVal
        });
      });
      try{
        let fileData = await fs.readFile('./data/data.csv', 'utf-8');
        await cpDataToTable(fileData);
      }catch(e){
        console.log(e)
        process.exit(1);
      }
      
      try{
        console.log("execute bulk upload myTable ");
        dbConnection.execBulkLoad(bulkLoadTbl);
      }catch(e){
        console.log(e);
        process.exit(1);
      }
    }
  });
  dbConnection.execSql(request);
}

function clearTable(){
  const sql = 'truncate table dbo.myTable';
  const request = new Request(sql, function (err) {
    if (err) {
      console.log("Error in myTable table clear SQL query " + err);
      process.exit(1);
    } else {
      getTableMetaInfo();
    }
  });
  console.log("clear myTable table ");
  dbConnection.execSql(request);
}

function initBulkUpload(){
  bulkLoadTbl = dbConnection.newBulkLoad('myTable', function (error, rowCount) {
    if (error) {
      console.log('Error table_name: ' + error);
      process.exit(1);
    }
    console.log(rowCount + " : Number of rows moved ");
    eventEmitter.emit('DBUpdateDone'); //emit event to subscriber
  });
  bulkLoadTbl.setTimeout(10000);
  clearTable();
}

module.exports = function(){
  this.StartDBProcess = () =>{
    dbConnection = new Connection(dbConConfig);

    dbConnection.on('connect', (err) => {
      if (err) {
        console.log('db connection error info: ', err);
        process.exit(1);
      } else {
        console.log("DB Connection successful");
        initBulkUpload();
      }
    });
    dbConnection.on('end', function (err) {
      console.log('All DB task completed, closing db connection');
      if (err) {
        console.log('connection closing error: ', err);
        process.exit(1);
      } else {
        console.log("Database Connection closed successful");
        eventEmitter.emit('DBUpdateDone');
      }
    });
  };
  
  this.on = eventEmitter.on.bind(eventEmitter);
}