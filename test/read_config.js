/**
 * New node file
 */

 var fs = require('fs'),
 	 util = require('util'),
 	 xml2js = require('xml2js');
 
 var parser = new xml2js.Parser({explicitRoot: false, explicitArray: true});
 
 fs.readFile('./payload.xml', function(err, data){
 	parser.parseString(data, function(err, result){
 		console.log(util.inspect(result, false, null));
 		console.log(result.available[0] + ' type: ' + (typeof result.available[0]));
 		console.log(result.recycled[0].number);
 	});
 });