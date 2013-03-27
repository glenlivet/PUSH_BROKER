/**
 * New node file
 */

 var fs = require('fs'),
 	 util = require('util'),
 	 Step = require('step'),
 	 xml2js = require('xml2js');
 	 
 	 
 var Config = module.exports.Config = 
 function(path){
 
 	this.result = null;
 	this.path	= path||'../config/Config.xml';
 	
 	var self = this;
 	var parser = new xml2js.Parser({explicitRoot: false, explicitArray: false});
 	
 	var data = fs.readFileSync(self.path);
 	
 	Step(
 		function parse(){
 			parser.parseString(data, this);
 		},
 		
 		function setResult(err, result){
 			if(err) throw err;
 			self.result = result;
 			return result;
 		}
 	);
 	
 };
 
 Config.prototype.getResult = 
 function(){
 	return this.result;
 	
 };
 