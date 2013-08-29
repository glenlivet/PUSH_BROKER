/**
 * New node file
 */
var util = require('util');
var object = require('./object.js');

var Extended = module.exports.extendedObject = 
function Extended(){
	object.myObject.call(this);
}

util.inherits(Extended, object.myObject);

Extended.prototype.changeProperty = 
function(_property){
	this.property = _property*_property;
}