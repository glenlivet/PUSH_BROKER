/**
 * New node file
 */

 var xml2js 		= require('xml2js'),
 	 Step			= require('step');
 	 
 var parser = new xml2js.Parser({explicitRoot: false});
 	 
 var parse = module.exports.parse = 
 function parse(xml){
 	var _result = null;
 	Step(
 		function parse(){
 			parser.parseString(xml, this);
 		},
 		function setResult(err, result){
 			if(err) throw err;
 			_result = result;
 		}
 	);
 	return _result;
 };
 