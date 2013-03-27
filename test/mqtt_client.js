/**
 * New node file
 */

 var mqtt = require('mqtt');
 
 var client = mqtt.createClient('10808', '128.128.4.160', {clientId: 'MCXXX00', username: 'MessageCentre', password: '000000'});
 var patt_mid_init_req = /_MC_\/_GET_\/_MSG_ID_PARA_/;
 var topic_mid_init = "_BROKER_\/_INIT_\/_MSG_ID_PARA_";
 var patt_clientInfo_init = "_BROKER_\/_INIT_\/_CLIENT_INFO_";
 
 var payload_mid_init = '<payload><recycled></recycled><available>1</available></payload>';
 
 client.on('message', function(topic, message, packet) {
    console.log(topic + ' ' + message);
    if(patt_mid_init_req.test(topic)){
    	client.publish(topic_mid_init, payload_mid_init, {qos: 2});
    }
  });
  
  client.on('connect', function(){
  	console.log('connected');
  	
  
  });