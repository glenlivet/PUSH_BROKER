/**
 * New node file
 */

 
 var Retained_msg = exports.retainedMsg = 
 function(payload, qos, dbMid){
 	this.payload = payload;
 	this.qos	 = qos;
 	this.dbMid	 = dbMid;
 }
 
 Retained_msg.prototype.arrangeQos = 
 function(_qos){
 	if(this.qos > _qos){
 		return _qos;
 	}
 	return this.qos;
 }
 
 