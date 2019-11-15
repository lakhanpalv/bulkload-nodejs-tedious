const processData = require('./process-data');

function dbUpdateDone(){
  console.log('info', '/*********END DB Update********/');
  process.exit(0);
}

const dbProcessor = new processData();
dbProcessor.on('DBUpdateDone', dbUpdateDone);
dbProcessor.StartDBProcess();

//global function to catch uncaught exceptions 
process.on('uncaughtException', function (err) {
  console.log('uncaught exception error: ', err);
  process.exit(1);
});