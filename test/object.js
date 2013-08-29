/**
 * New node file
 */
var MyObject = module.exports.myObject = 
function MyObject(){
	this.property = 100;
	
}

MyObject.prototype.changeProperty = 
function(_property){
	this.property = _property;
}

MyObject.prototype.invoke = 
function(){
	console.log(this.property);
}

