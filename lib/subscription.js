/**
 * New node file
 */

 
 var Subscription = module.exports =
 function Subscription(obj){
 	if(typeof obj === 'object'){
 		//this.topic = obj.topic;
 		this.qos   = obj.qos||0;
 		//this.abs  = obj.abs;
 		this.topicDesc = obj.topicDesc;
 		this.parseTopic();
 	}
 }
 
 Subscription.prototype.parseTopic = 
 function(){
 	var _topic = this.topicDesc;
 	if(_topic.indexOf('+')>=0||_topic.indexOf('#')>=0){
 		var _reg = new RegExp('^' + _topic.replace('+', '[^\/]+').replace('/#', '(/.+)?$'));
 		this.topic = _reg;
 		this.abs = false;
 		
 	}else{
 		this.topic = this.topicDesc;
 		this.abs = true;
 	}
 }
 
 Subscription.prototype.modifyQos = 
 function(qos){
 	this.qos = qos;
 }