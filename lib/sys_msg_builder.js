/**
 * New node file
 */
 
 var xmlHead = '<?xml version="1.0" encoding="UTF-8"?>';
 
 var MC_GET_CLIENT_INFO = module.exports.MC_GET_CLIENT_INFO = '_MC_/_GET_/_CLIENT_INFO_';
 var BROKER_INIT_CLIENT_INFO = module.exports.BROKER_INIT_CLIENT_INFO = '_BROKER_/_INIT_/_CLIENT_INFO_';
 var MC_GET_CACHED_MSG = module.exports.MC_GET_CACHED_MSG = '_MC_/_GET_/_CACHED_MSG_';
 var BROKER_INTI_CACHED_MSG = module.exports.BROKER_INTI_CACHED_MSG = '_BROKER_/_INIT_/_CACHED_MSG_';
 var SYS_MSG = module.exports.SYS_MSG = '_SYSTEM_MSG_';
 
 var mcPostRetainedStr = '_MC_/_POST_/_RETAIN_MSG_';
 var mcPostPublishListStr = '_MC_/_POST_/_PUBLISH_LIST_';
 var mcPostMsgIdUpdateStr	= '_MC_/_POST_/_MSG_ID_PARA_';
 var mcPostCachedStr = '_MC_/_POST_/_CACHED_MSG_';
 var mcNotifyPubrelAckStr = '_MC_/_NOTIFY_/_PUBREL_ACK_';
 var mcNotifyPublishAckStr = '_MC_/_NOTIFY_/_PUBLISH_ACK_';
 
 var isSysMsg = module.exports.isSysMsg = 
 function isSysMsg(obj){
 	if(obj.type[0] == SYS_MSG)
 		return true;
 	else
 		return false;
 };
 
 var McNotifyPublishAck = module.exports.McNotifyPublishAck = 
 function McNotifiyPublishAck(obj){
 	this.dbMsgId	= obj.dbMsgId;
 	this.recipient	= obj.recipient;
 	
 	this.sysTopic	= mcNotifyPublishAckStr;
 };
 
 McNotifyPublishAck.prototype.getXml = 
 function(){
 	var rtn = xmlHead + 
 				'<payload>' +
 					'<type>' +  
 						SYS_MSG +
 					'</type>' + 
 					'<db_msg_id>' + 
 						this.dbMsgId + 
 					'</db_msg_id>' + 
 					'<recipient>' + 
 						this.recipient + 
 					'</recipient>' + 
 				'</payload>';
 	return rtn; 
 };
 
 McNotifyPublishAck.prototype.publish = 
 function(_broker){
 	_broker.msgCntr.publish({
 		topic			: this.sysTopic,
 		payload			: this.getXml(),
 		qos				: 0
 	});
 };
 
 var McNotifyPubrelAck = module.exports.McNotifyPubrelAck = 
 function McNotifyPubrelAck(obj){
 	this.dbMsgId	= obj.dbMsgId;
 	this.recipient	= obj.clientId;
 
 	this.sysTopic	= mcNotifyPubrelAckStr;
 }; 
 
 McNotifyPubrelAck.prototype.getXml = 
 function(){
 	var rtn = xmlHead + 
 			  '<payload>' + 
 			  	'<type>' + 
 			  		SYS_MSG + 
 			  	'</type>' + 
 			  	'<db_msg_id>' + 
 			  		this.dbMsgId + 
 			  	'</db_msg_id>' +
 			  	'<recipient>' + 
 			  		this.recipient + 
 			  	'</recipient>' + 
 			  '</payload>';
 	return rtn;
 };
 McNotifyPubrelAck.prototype.publish = 
 function(_broker){
 	_broker.msgCntr.publish({
 		topic			: this.sysTopic,
 		payload			: this.getXml(),
 		qos				: 0
 	});
 };
 
 
 var McPostRetained = module.exports.McPostRetained = 
 function McPostRetained(obj){
 	this.type 		= obj.type;
 	this.dbMsgId 	= obj.dbMsgId;
 	this.topic 		= obj.topic;
 	this.payload 	= obj.payload||'';
 	this.qos 		= obj.qos||'';
 	
 	this.sysTopic   = mcPostRetainedStr;
 	
 };
 
 McPostRetained.prototype.getXml = 
 function(){
 	var rtn = xmlHead + 
 			 '<payload>' + 
 				'<type>' + 
 					this.type + 
 				'</type>' + 
 				'<db_msg_id>' + 
 					this.dbMsgId + 
 				'</db_msg_id>' + 
 				'<topic>' + 
 					this.topic + 
 				'</topic>' + 
 				'<msg_payload>' + 
 					this.payload + 
 				'</msg_payload>' + 
 				'<qos>' + 
 					this.qos + 
 				'</qos>' + 
 			'</payload>';
 	return rtn;
 };
 
 McPostRetained.prototype.publish = 
 function(_broker){
 	_broker.msgCntr.publish({
 		topic : this.sysTopic,
 		payload : this.getXml(),
 		qos : 2,
 		messageId : _broker.midFac.createMid()
 	});
 };
 
 var McPostPublishList = module.exports.McPostPublishList = 
 function McPostPublishList(obj){
 	this.dbMsgId 	= obj.dbMsgId;
 	this.recipients = obj.recipients;
 	
 	this.sysTopic	= mcPostPublishListStr;
 };
 
 McPostPublishList.prototype.getXml = 
 function(){
 	var rtn = xmlHead + 
 			  '<payload>' + 
 			  	'<type>' + 
 			  		SYS_MSG + 
 			  	'</type>' + 
 			  	'<db_msg_id>' + 
 			  		this.dbMsgId + 
 			  	'</db_msg_id>';
 	if(this.recipients.length == 0){
 		rtn += '<recipient>' + 
 			   '</recipient>';
 	}
 	for(var i=0;i<this.recipients.length;i++){
 		rtn += '<recipient>' + 
 					this.recipients[i] + 
 			   '</recipient>';
 	}
 	rtn += '</payload>';
 	return rtn;
 };
 
 McPostPublishList.prototype.publish = 
 function(_broker){
 	_broker.msgCntr.publish({
 		topic : this.sysTopic,
 		payload : this.getXml(),
 		qos : 0
 	});
 };
 

 
 var McPostCached = module.exports.McPostCached = 
 function McPostCached(_msg, type){
 	this.type = type;
 	this.msg  = _msg;
 	
 	this.done = false;
 	this.sysTopic = mcPostCachedStr;  
 };
 
 McPostCached.prototype.publish = 
 function(_broker){
 	if(this.done && this.type == '_ADD_') return;
 	_broker.msgCntr.publish({
 		topic		: this.sysTopic,
 		payload		: this.getXml(),
 		qos			: 2,
 		messageId	: _broker.midFac.createMid()
 	});
 };
 
 McPostCached.prototype.getXml = 
 function(){
 	var rtn = xmlHead + 
 			  '<payload>' + 
 			  	'<type>' +
 			  		this.type + 
 			 	'</type>' + 
 			 	'<db_msg_id>' + 
 			 		this.msg.dbMid + 
 			 	'</db_msg_id>';
 	if(null == this.msg.initMid){
 		rtn += '<broker_msg_id>' +
 			   '</broker_msg_id>';
 	}else{
 		rtn += '<broker_msg_id>' +
 					this.msg.initMid
 			   '</broker_msg_id>';
 	}
 	rtn += '<broker_msg_id>' + 
 				this.msg.curMid + 
 			'</broker_msg_id>';
 			//如果
 	if(isEmptyObject(this.msg.publishRecipients)&& this.msg.pubrelRecipients.length == 0){
 		this.done = true;
 	}
 	
 	rtn += '<publish_recipients>';
 	for(var clientId in this.msg.publishRecipients){
 		rtn += '<publish_recipient>' + 
 					'<client_id>' + 
 						clientId + 
 					'</client_id>' + 
 					'<qos>' + 
 						this.msg.publishRecipients[clientId] + 
 					'</qos>' + 
 				'</publish_recipient>';
 	}
 	rtn += '</publish_recipients>';
 	
 	rtn += '<pubrel_recipients>';
 	for(var i=0; i<this.msg.pubrelRecipients.length; i++){
 		rtn += '<pubrel_recipient>' + 
 					'<client_id>' + 
 						this.msg.pubrelRecipients[i] + 
 					'</client_id>' + 
 					'<qos>' +
 						2 +
 					'</qos>' + 
 				'</pubrel_recipient>';
 	}
 	rtn += '</pubrel_recipients>';
 	rtn += '</payload>';
 	
 	return rtn;		  
 };
 






 var McPostMsgIdUpdate = module.exports.McPostMsgIdUpdate = 
 function McPostMsgIdUpdate(obj){
 	//TODO this.
 };
 
 
 //判断是否为空对象
 function isEmptyObject(obj){
 	for(var k in obj){
 		return false;
 	}
 	return true;
 }
 
 
 
 
 
 
 
 
 
 