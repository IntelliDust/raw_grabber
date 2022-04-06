config = require('./config.json')

const mqtt = require('mqtt')
var mysql      = require('mysql2');



const clientId = `raw_grabber_${Math.random().toString(16).slice(3)}`
console.log('Starting')



const connectUrl = `mqtt://${config['mqtt']['host']}:${config['mqtt']['port']}`
console.log(connectUrl);
const mqtt_client = mqtt.connect(connectUrl, config.mqtt.params)
console.log('Connecting MQTT')


var db_connection = mysql.createConnection(config.mysql)

db_connection.connect();




const topic = '/miner/#'
mqtt_client.on("error",function(error){ console.log("Can't connect"+error);})

mqtt_client.on('connect', () => {
  console.log('Connected')
  mqtt_client.subscribe([topic], () => {
    console.log(`Subscribe to topic '${topic}'`)
  })
})
mqtt_client.on('message', (topic, payload) => {
  console.log('Received Message:', topic)
  pl=JSON.parse(payload.toString())
  console.table(pl)   // /miner/miner-d57d17eb61359699ad31/Wireless-MBus/water/2146083
  const path=topic.split("/",5);
  if (path[1]=='miner'){
     var query = db_connection.query('INSERT IGNORE INTO `raw_intake` (`path`, `raw_data`, `ts_produced`, `processed`) VALUES (?, ?, ?, 0)',[topic,payload.toString(),pl.timestamp], function (error, results, fields) {
        if (error){
            console.log(query.sql);
            throw error;
	}
        // Neat!
     });
//     console.log(query.sql);
  }else{
    console.log('Unknown root [',path[1],'] message: ',payload.toString())
  }
})