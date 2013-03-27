/**
 * New node file
 */
var config_reader = require('../lib/config_reader.js');
var util = require('util');

var config = new config_reader.Config();

console.log('final info: ' + util.inspect(config.getResult(), false, null));

var config_info = config.getResult();

console.log('port: ' + config_info.port + ' client_id: ' + config_info.message_centre.client_id);