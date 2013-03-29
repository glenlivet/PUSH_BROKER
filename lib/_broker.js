/**
 * New node file
 */

 var mqtt 			= require('mqtt'),
 	 NodeLog 		= require('./node_log.js'),
 	 Subscription	= require('./subscription.js'), 
 	 MidFac			= require('./mid_factory.js'),
 	 CachedMsg 		= require('./msg_factory.js'),
 	 RetainedMsgFac = require('./retained_msg.js'),
 	 fs				= require('fs'),
 	 xml2js 		= require('xml2js'),
 	 config_reader 	= require('./config_reader.js'),
 	 Client			= require('./client.js'),
 	 events 		= require('events'),
 	 Step 			= require('step'),
 	 SysMsgBuilder  = require('./sys_msg_builder.js'),
 	 util			= require('util');
 	 
 var _nodeLog = new NodeLog(true);
 var parser = new xml2js.Parser({explicitRoot: false});

 var Broker = module.exports = 
 function Broker(){
 	//存储client的集合
 	this.clients		= {};
 	//存储暂存msg的数组
 	this.cachedMsgs 	= [];
 	//存储保留msg的集合
 	this.retainedMsgs	= {};
 	//
 	this.midFac			= new MidFac();
 	//
 	this.port			= 1883;
 	//
 	this.hasInitialized = false;
 	//mid初始化flag
 	this.midInitialized = false;
 	
 	this.msgCntr		= null;
 	
 	this.server			= null;
 	
 	this.sysIds			= [];
 	
 	this.sysIdPrefix	= 'BROKER' + NodeLog.getCurDate();
 	
 	this.sysIdNum		= 1;
 	
 	this.initialMsgs		= {};
 	
 	//读取配置表
 	var config = new config_reader.Config();
 	config_info = config.getResult();
 	
 	this.port = parseInt(config_info.port);
 	var _mc_clientId = config_info.message_centre.client_id;
 	var _mc_username = config_info.message_centre.username;
 	var _mc_password = config_info.message_centre.password;
 	this.msgCntr	= new Client(_mc_clientId, _mc_username, _mc_password);
 	
 	events.EventEmitter.call(this);
 };
 
 util.inherits(Broker, events.EventEmitter);
 
 /**
  * 确认连接客户是否正确 
  */
 Broker.prototype.clientAuthorized = 
 function(packet){
 	var _clientId = packet.clientId;
 	var _client = this.clients[_clientId];
 	if(typeof packet.username == 'undefined' || typeof packet.password == 'undefined')
 		return false;
 	if(typeof _client == 'undefined'){
 		return false;
 	}else{
 		//每个客户都必须要有username和password
 		if(_client.checkUsernameAndPassword(packet.username, packet.password))
 			return true;
 		else
 			return false;
 	}
 };
 //init
 //发送请求msg_fac的数据
 Broker.prototype.init = 
 function(){
 	var _broker = this;
 	this.on('initDone', function(){
 		_broker.sendCachedMsgTo(_broker.msgCntr.id);
 	});
 	this.msgCntr.publish({topic: '_MC_/_GET_/_MSG_ID_PARA_', qos: 2, messageId: this.midFac.getReserved()});
 };
 
 Broker.prototype.start = 
 function(){
 	var _broker = this;
 	this.server = mqtt.createServer(function(conn){
 		//当有客户端连接上来
 		conn.on('connect', function(packet){
 			_nodeLog.info('Client[clientId=' + packet.clientId + '] has sent a connect');
 			//是否完成了初始化
 			if(!_broker.hasInitialized){
 				//判断是否为mc连接上来
 				if(_broker.isMc(packet)){
 					//_nodeLog.debug('isMc');
 					//配置msgCntr
 					conn.id = packet.clientId;
 					_broker.msgCntr.setConn(conn);
 					
 					this.connack({returnCode: 0});
 					_nodeLog.info('Client[clientId=' + packet.clientId + '] accepted');
 					//做数据初始化
 					_broker.init();
 					return;
 				}else{
 					//未完成初始化之前拒绝客户端访问
 					this.connack({returnCode: 3});
 					_nodeLog.info('Client[clientId=' + packet.clientId + '] rejected');
 					return;
 				}
 			}else{
 				//normal process
 				if(!_broker.clientAuthorized(packet)){
 					this.connack({returnCode: 4});
 					_nodeLog.info('Client[clientId=' + packet.clientId + '] rejected');
 					return;
 				}
 				else{
 					if(_broker.isMc(packet)){
 						_broker.msgCntr.setConn(conn);
 					}
 					//will handling
 					conn.id = packet.clientId;
 					_broker.clients[packet.clientId].setConn(conn);
 					this.connack({returnCode: 0});
 					_nodeLog.info('Client[clientId=' + packet.clientId + '] accepted');
 					//发送指令给msgCntr告知客户端连接
 					
 					//发送缓存msg
 					_broker.sendCachedMsgTo(packet.clientId);
 				
 				}
 				
 			}
 			
 		});
 		//
 		conn.on('subscribe', function(packet){
 			
 		});
 		
 		conn.on('unsubscribe', function(packet){
 		
 		});
 		
 		conn.on('publish', function(packet){
 			_nodeLog.debug('in publish: ' + util.inspect(packet, false, null));
 			//处理初始化
 			if(!_broker.hasInitialized){
 				switch(packet.qos){
 					case 0:
 						_broker.handleInitialCommand(packet.topic, packet.payload);
 						break;
 					case 1:
 						_broker.handleInitialCommand(packet.topic, packet.payload);
 						conn.puback({messageId: packet.messageId});
 						break;
 					case 2:
 						_broker.initialMsgs[packet.messageId] = {topic: packet.topic, payload: packet.payload};
 						conn.pubrec({messageId: packet.messageId});
 						break;
 				
 				}
 				return;
 			}
 			var _topic = packet.topic;
 			//解析payload的xml来获取db_msg_id， created_time和真正的内容
 			var payloadObj = _broker.payloadParse(packet.payload); 
 			//处理retained msg
 			if(packet.retain == true){
 				var _dbMsgId = payloadObj.bd_msg_id[0];
 				var _content = payloadObj.content[0];
 				
 				_nodeLog.debug('Received a RETAIN MSG on [TOPIC:' + _topic + '].');
 				//handle delete retain msg (which got a null payload)
 				if('undefined' == typeof _content || null == _content || '' == _content.replace(/^\s+|\s+$/g, "")){
 					_nodeLog.info('A DEL-RETAIN-COMMAND MSG on [TOPIC: ' + _topic + '] has received!');
 					for(var __topic in _broker.retainedMsgs){
 						if(_topic === __topic){
 							delete retainMsgs[__topic];
 							//do notify the msgCntr
 							var _mcPostRetained = new SysMsgBuilder.McPostRetained({
 								type		: '_DEL',
 								dbMsgId		: _dbMsgId,
 								topic		: _topic
 							});
 							_mcPostRetained.publish(_broker);
 							
 							_nodeLog.info('Retain Msg on [TOPIC:' + _topic + '] has been deleted!');
 							break;
 						}
 					}
 				}
 				//otherwise add/renew the retain msg
 				var _retainedMsg = new RetainedMsgFac.retainedMsg(_content, packet.qos, _dbMsgId);
 				_broker.retainedMsgs[_topic] = _retainedMsg;
 				_nodeLog.info('[RETAIN MSG] [TOPIC: ' + _topic + '] [PAYLOAD: ' + _content + '] [QOS: ' + packet.qos +
  						'] has been added/renewed in retainMsgs collection');
 				
 			}
 			//获取发送列表
 			//如果qos>0 将发送列表发送给mc _MC_/_POST_/_PUBLISH_LIST_/[sysId]
 			var targets = {};
 			if(!_broker.isSysMsg(_topic)){
 				targets = getSubscribedClients(_topic);
 				if(packet.qos>0){
 					var _subedClients = [];
 					for(var _cid in targets){
 						_subedClients.push(_cid);
 					}
 					var _mcPostPublishList = new SysMsgBuilder.McPostPublishList({
 						dbMsgId		: payloadObj.bd_msg_id[0],
 						recipients	: _subedClients
 					});
 					_mcPostPublishList.publish(_broker);
 				}
 			}
 			var noTarget = false;
 			if(isEmptyObject(targets)) {
  				noTarget = true;
  				//系统指令
  				_nodeLog.info('PUBLISH: ' + util.inspect(packet, true, null) + ' got no subscriber!');
  			}
 			//处理发送，按qos不同
 			switch(packet.qos){
 				case 0:
 					//直接发送
 					for(var clientId in targets){
 						var _toClient = _broker.clients[clientId];
 						_toClient.publish({
 							topic 		: packet.topic,
 							payload		: payloadObj.content[0],
 							qos			: 0
 						}); 
 						//log
  						_nodeLog.info('message: ' + '[topic] ' +  packet.topic + ' [payload]' + payloadObj.content[0] + ' [qos]0 ' + 
  								' has just been sent to Client[clientId=' + clientId + ']');
 					}
 					break;
 				case 1:
 					//qos 1: generate new mid for further publishing; store the msg; publish it; send PUBACK back;
 					if(!noTarget){
 						//更改发送目标的qos
 						for(var clientId in targets){
 							targets[clientId] = targets[clientId] > 1 ? 1 : targets[clientId];
 						}
 						//store msg
 						var _msg = new CachedMsg({
 							dbMid				: payloadObj.db_msg_id[0],
 							curMid				: _broker.midFac.createMid(),
 							topic				: packet.topic,
 							payload				: payloadObj.content[0],
 							publishRecipients	: targets,
 							sender				: conn.id,
 							initMid				: packet.messageId,
 							createdTime			: payloadObj.created_time[0]
 						});
 						//系统指令 _MC_/_POST_/_MSG_ID_PARA_
 						
 						_broker.addCachedMsg(_msg);
 						//publish
 						for(var clientId in targets){
 							var _toClient = _broker.clients[clientId];
 							var _pubQos   = targets[clientId];
 							_toClient.publish({
 								topic		: _msg.topic,
 								payload		: _msg.payload,
 								qos			: _pubQos,
 								messageId	: _msg.curMid
 							});
 							//log
  							_nodeLog.info('message: ' + '[topic] '  +  _msg.topic + 
  								   ' [payload] '  +  _msg.payload + 
  								   ' [qos] '      +  _pubQos + 
  								   ' [messageId] ' +  _msg.curMid + 
  								' has just been sent to Client[clientId=' + clientId + ']');
 						}
 						//if(_pubQos == 0) 将发送者从发送列表中删除 
 						if(_pubQos == 0){
 							_msg.removePublishRecipient(clientId);
 							//log
  							_nodeLog.info('msg[msgId=' + _msg.curMid + '] has removed recipient[ClientId=' + clientId + 
  									'] for QOS\' sake');
 						}
 						
 						_msg.initMid = null;
 						
 						//发送系统指令 _MC_/_POST_/_CACHED_MSG_
 						var  _mcPostCached = new SysMsgBuilder.McPostCached(_msg, '_ADD_');
 						_mcPostCached.publish(_broker);
 						
 						
 						
 					}
 					//PUBACK 
  					_broker.clients[conn.id].puback({messageId: packet.messageId});
  					//log
  					_nodeLog.info('msg[msgId=' + packet.messageId + '] has been sent a PUBACK back to Client[clientId=' + client.id + ']');
  					break;
  				case 2:
  					var hasCached = false;
  					var _client = _broker.clients[conn.id];
  					//handle dup
  					if(packet.dup == true){
  						//loop cachedMsgs see if the dup one can be found
  						for(var m=0; m<_broker.cachedMsgs.length;m++){
  							//find if has cached b4
  							if(_client.id === _broker.cachedMsgs[m].sender && packet.messageId === _broker.cachedMsgs[m].initMid){
  								_nodeLog.warn('a dup publish has been caught. ' + 
  								'probably caused by previous disconnection with Client[clientId=' + 
  								 _client.id + ']. The msg: ' + packet);
  								 hasCached = true;
  								 break;
  							}
  						}
  					}
  					if(!hasCached){
  						//store the msg
  						var _msg = new CachedMsg({
  							dbMid				: payloadObj.db_msg_id[0],
 							curMid				: _broker.midFac.createMid(),
 							topic				: packet.topic,
 							payload				: payloadObj.content[0],
 							publishRecipients	: targets,
 							sender				: conn.id,
 							initMid				: packet.messageId,
 							createdTime			: payloadObj.created_time[0]
  						});
  						//log
  						_nodeLog.info('msg: ' + 
  							util.inspect(_msg, true, null) + 'has been cached.');
  						
  						_broker.addCachedMsg(_msg);  						
  						//发送系统指令 _MC_/_POST_/_CACHED_MSG_
  						var  _mcPostCached = new SysMsgBuilder.McPostCached(_msg, '_ADD_');
 						_mcPostCached.publish(_broker);
  					}
  					
  					_client.pubrec({messageId: packet.messageId});
  					//log
  					_nodeLog.info('PUBREC on msg[msgId=' + packet.messageId + 
  							'] has been sent back to Client[clientId='+ client.id+']');
 					
 					break;
 				default:
 					break;	
 			}
 			
 		});
 		
 		conn.on('pubrel', function(packet){
 			var _client = _broker.clients[conn.id];
 			_nodeLog.info('in pubrel' + util.inspect(packet, false, null));
 			if(!_broker.hasInitialized){
 				var _msg = _broker.initialMsgs[packet.messageId];
 				//_nodeLog.debug('topic: ' + _msg.topic + ' payload: ' + _msg.payload);
 				_broker.handleInitialCommand(_msg.topic, _msg.payload);
 				return;
 			}
 			//找到缓存的msg
 			var __initMid = packet.messageId;
 			var __sender  = conn.id;
 			var _msg = null;
 			//循环缓存msg找到初始msgId一样的msg
 			for(var i = 0; i < _broker.cachedMsgs.length; i++){
 				if(__initMid === _broker.cachedMsgs[i].initMid && __sender === _broker.cachedMsgs[i].sender){
 					_msg = _broker.cachedMsgs[i];
 					break;
 				}
 			}
 			//没找到匹配的缓存msg //这里保证了程序只处理一次publish
 			if(_msg == null) {
 				//log
  				_nodeLog.info('Unknown msg id detected! May caused by DUP! ');
  				if(packet.dup == true){
  					this.pubcomp({messageId: packet.messageId, dup: 1});
  					return;
  				}else{
  					this.emit('error', new Error('Unknown message id'));
  				}
  				
 			}
 			
 			//publish给订阅者（判定是否有目标）
 			//pubcomp
 		});
 		
 		conn.on('puback', function(packet){
 			//找到缓存msg，将接收人从接收人列表中删除
 			//发送给mc _MC_/_NOTIFY_/_PUBLISH_ACK_/[sysId]
 		});
 		
 		conn.on('pubrec', function(packet){
 			_nodeLog.info('in pubrec' + util.inspect(packet, false, null));
 			if(!_broker.hasInitialized){
 				conn.pubrel(packet);
 			}
 			//找到缓存的msg
 			//缓存msg将该用户从publish转为pubrel
 			//pubrel
 		});
 		
 		conn.on('pubcomp', function(packet){
 		_nodeLog.info('in pubcomp' + util.inspect(packet, false, null));
 			if(!_broker.hasInitialized){
 				_broker.midFac.releaseReserve();
 			}
 			//找到缓存的msg
 			//将接收人删除
 			//发送给mc _MC_/_NOTIFY_/_PUBLISH_ACK_/[sysId]
 		});
 		
 		conn.on('pingreq', function(packet){
 			conn.pingresp();
 		});
 		
 		conn.on('disconnect', function(packet){
 			conn.stream.end();
 		});
 		
 		conn.on('close', function(packet){
 			
 		});
 		
 		conn.on('error', function(e){
 			conn.stream.end();
 			
 		});
 		
 	});
 	this.server.listen(this.port);
 };
 
 Broker.prototype.isSysMsg = 
 function(_topic){
 	var patt_mid_init = /_BROKER_\/_INIT_\/_MSG_ID_PARA_/;
 	var patt = /^_BROKER_\/_/; 
 	return patt.test(_topic);
 };
 
 Broker.prototype.isMc = 
 function(packet){
 	//_nodeLog.debug(util.inspect(packet, false, null));
 	//_nodeLog.debug(util.inspect(this.msgCntr, false, null));
 	if(this.msgCntr.id == packet.clientId 
 	 && this.msgCntr.username == packet.username  
 	 && this.msgCntr.password == packet.password
 		)
 		return true;
 	else
 		return false;
 };
 /**
 * addCachedMsg 向缓存消息数组中添加一个msg
 *
 * @param msg <CachedMsg> 待添加的msg
 *
 * @see CachedMsg
 */
 Broker.prototype.addCachedMsg = 
 function(_msg){
 	var _broker = this;
 	//监听_msg.emit('done') ，把msg从列表中删除，回收这个curMid
 	_msg.once('done', function(){
 		var __mid = this.curMid;
 		//把msg从列表中删除
 		for(var i=0;i<_broker.cachedMsgs.length;i++){
 			if(__mid === _broker.cachedMsgs[i].curMid){
 				_broker.cachedMsgs.splice(i,1);
 				break;
 			}
 		}
 		//系统指令 _MC_/_POST_/_CACHED_MSG_
 		
 		//recycle the curMid
 		_broker.midFac.recycle(__mid);
 		//系统指令 _MC_/_POST_/_MSG_ID_PARA_
 		
 		//log
  		_nodeLog.info('msg[msgId=' + __mid + '] has done, mid has been recycled');
 	});
 	
 	_broker.cachedMsgs.push(_msg);
 };
 
 /**
  * sendCachedMsgTo 注册过的客户端重新连接后，将属于其的msg发送给他，包括(publish和pubrel)
  * 
  * @param clientId <String> 
  */
 Broker.prototype.sendCachedMsgTo = 
 function(clientId){
 	var _self = this;
 	//loop the cached msg array
 	for(var i=0;i<_self.cachedMsgs.length;i++){
 		var _msg = _self.cachedMsg[i];
 		//clientId in publishRecipients 
 		if(clientId in _msg.publishRecipients){
 			//publish
 			_self.clients[clientId].publish({
 				topic		: _msg.topic,
 				payload		: _msg.payload,
 				messageId	: _msg.curMid,
 				qos			: _msg.publishRecipients[clientId],
 				dup			: true
 			});
 			//log
  			_nodeLog.info('a msg has been republish to Client[clientId=' + clientId + '] on qos' + _msg.publishRecipients[clientId]);
  			continue;
 		}
 		//clientId in pubrelRecipients
 		var _pubrelRecipients = _msg.pubrelRecipients;
  		//在该msg的pubrel收件人中寻找clientId,如果找到， 发送pubrel，并跳出循环
  		for(var j=0;j<_pubrelRecipients.length;j++){
  			if(clientId === _pubrelRecipients[j]){
  				clients[clientId].pubrel({messageId : _msg.curMid, dup: true});
  				break;
  			}
  		}
 	}
 	
 };
 
 Broker.prototype.handleInitialCommand = 
 function(topic, payload){
 	var patt_mid_init = /_BROKER_\/_INIT_\/_MSG_ID_PARA_/;
 	var patt_clientInfo_init = /_BROKER_\/_INIT_\/_CLIENT_INFO_/;
 	var patt_cachedMsg_init	= /_BROKER_\/_INIT_\/_CACHED_MSG_/;
 	var patt_retain_init = /_BROKER_\/_INIT_\/_RETAIN_MSG_/;
 	if(patt_mid_init.test(topic)){
 		this.handleMidInit(payload);
 		this.msgCntr.publish({topic: '_MC_/_GET_/_CLIENT_INFO_', qos: 2, messageId: this.midFac.getReserved()});
 	}
 	if(patt_clientInfo_init.test(topic)){
 		this.handleClientInfoInit(payload);
 		this.msgCntr.publish({topic: '_MC_/_GET_/_CACHED_MSG_', qos: 2, messageId: this.midFac.getReserved()});
 	}
 	if(patt_cachedMsg_init.test(topic)){
 		this.handleCachedMsgInit(payload);
 		this.msgCntr.publish({topic: '_MC_/_GET_/_RETAIN_MSG_', qos: 2, messageId: this.midFac.getReserved()});
 	}
 	if(patt_retain_init.test(topic)){
 		this.handleRetainedInit(payload);
 		_nodeLog.info('Initialization Done!');
 		this.hasInitialized = true;
 		this.emit('initDone');
 	}
 	
 };
 
 Broker.prototype.payloadParse = 
 function(payload){
 	var _result = null;
 	Step(
 		function parse(){
 			parser.parseString(payload, this);
 		},
 		function setResult(err, result){
 			if(err) throw err;
 			_result = result;
 		}
 	);
 	return _result;
 };
 
 Broker.prototype.handleMidInit = 
 function(payload){
 	_nodeLog.info('handle mid start' + payload);
 	var recycled = [];
 	var available;
 	Step(
 		function parse(){
 			parser.parseString(payload, this);
 		},
 		function setResult(err, result){
 			if(err) throw err;
 			available = Number(result.available[0]);
 			var _nums = result.recycled[0].number;
 			_nodeLog.debug(typeof _nums);
 			//如果返回的xml中没有回收项
 			if((typeof _nums) != 'undefined'){
 				for(var i=0; i< _nums.length; i++){
 					var _num = Number(_nums[i]);
 					recycled.push(_num);
 				}
 			}
 		}
 	);
 	this.midFac = new MidFac(recycled, available);
 	this.midInitialized = true;
 	//MID FAC INITIALIZATION DONE
 	_nodeLog.debug('MID FAC INITIALIZATION DONE!');
 	
 };
 
 Broker.prototype.handleClientInfoInit = 
 function(payload){
 	var _self = this;
 	_nodeLog.info('Init Client Info started!' + payload);
 	Step(
 		function parse(){
 			parser.parseString(payload, this);
 		},
 		function setResult(err, result){
 			if(err) throw err;
 			var _clients = result.client_info;
 			if((typeof _clients) != 'undefined'){
 				for(var i=0; i<_clients.length; i++){
 					var _clientId = _clients[i].client_id[0];
 					/////////处理没有username和password的情况
 					var _username = _clients[i].username[0];
 					var _password = _clients[i].password[0];
 					var _client = new Client(_clientId, _username, _password);
 					//处理没有订阅的情况
 					var _subs = _clients[i].subscriptions[0].subscription;
 					if((typeof _subs) != 'undefined'){
 						for(var j=0; j<_subs.length; j++){
 							var _sub = new Subscription({qos: _subs[j].qos[0], topicDesc: _subs[j].topic[0]});
 							_client.addSubscription(_sub); 
 						}
 					}
 					_self.clients[_clientId] = _client;
 					_nodeLog.debug(util.inspect(_client, true, null));
 				}
 			}
 		}
 	);
 	//更新msgCntr在clients中的内容
 	_self.clients[_self.msgCntr.id] = _self.msgCntr;
 	_nodeLog.info('Init Client Info done!');
 }
 
 Broker.prototype.handleCachedMsgInit = 
 function(payload){
 	var _self = this;
 	_nodeLog.info('init cached msg start' + payload);
 	Step(
 		function parse(){
 			_nodeLog.err('in cachedmsg step1');
 			parser.parseString(payload, this);
 		},
 		function setResult(err, result){
 			if(err) throw err;
 			_nodeLog.err('in cachedmsg step2');
 			var _msgs = result.cached_msg;
 			_nodeLog.debug(_msgs.length);
 			if((typeof _msgs) != 'undefined'){
 				for(var i=0; i<_msgs.length; i++){
 					var _dbMid 		= _msgs[i].db_msg_id[0];
 					var _curMid 	= _msgs[i].broker_msg_id[0];
 					var _initMid 	= _msgs[i].sender_msg_id[0];
 					var _sender 	= _msgs[i].sender[0];
 					var _createTime = _msgs[i].created_time[0];
 					var _topic 		= _msgs[i].topic[0];
 					var _payload 	= _msgs[i].msg_payload[0];
 					var _publishRecipients = {};
 					var _publishRecs = _msgs[i].publish_recipients[0].publish_recipient;
 					if((typeof _publishRecs) != 'undefined'){
 						for(var j=0;j<_publishRecs.length;j++){
 							var _clientId = _publishRecs[j].client_id[0];
 							var _qos = _publishRecs[j].qos[0];
 							_publishRecipients[_clientId] = _qos;
 						}
 					}
 					var _pubrelRecipients = [];
 					var _pubrelRecs = _msgs[i].pubrel_recipients[0].pubrel_recipient;
 					if((typeof _pubrelRecs) != 'undefined'){
 						for(var j=0;j<_pubrelRecs.length;j++){
 							var _clientId = _pubrelRecs[j].client_id[0];
 							_pubrelRecipients.push(_clientId);
 						}
 					}
 
 					var _readMsg = new CachedMsg({
 						dbMid				: _dbMid,
 						curMid				: _curMid,
 						initMid				: _initMid, 
 						sender				: _sender,
 						createdTime			: _createTime,
 						topic				: _topic,
 						payload				: _payload,
 						publishRecipients	: _publishRecipients,
 						pubrelRecipients	: _pubrelRecipients
 					});
 					_self.cachedMsgs.push(_readMsg);
 					_nodeLog.debug(util.inspect(_readMsg, false, null));
 				}
 			}
 		}
 	);
 	_nodeLog.info('init cached msg done');
 };
 
 Broker.prototype.handleRetainedInit = 
 function(payload){
 	var _self = this;
 	_nodeLog.info('init retained start' + payload);
 	Step(
 		function parse(){
 			parser.parseString(payload, this);
 		},
 		function setResult(err, result){
 			if(err) throw err;
 			var _msgs = result.retain_msg;
 			if((typeof _msgs) != 'undefined'){
 				for(var i=0;i<_msgs.length;i++){
 					var _dbMid = _msgs[i].db_msg_id[0];
 					var _topic = _msgs[i].topic[0];
 					var _payload = _msgs[i].msg_payload[0];
 					var _qos = _msgs[i].qos[0];
 					_retMsg = new RetainedMsgFac.retainedMsg(_payload, _qos, _dbMid);
 					_self.retainedMsgs[_topic] = _retMsg;
 					_nodeLog.debug(util.inspect(_retMsg, false, null));
 				}
 			}
 		}
 	);
 	_nodeLog.info('init retained done');
 };
 
 Broker.prototype.getSubscribedClients = 
 function(_topic){
 	//return obj {clientId : subQos}
 	var rtn = {};
 	//每一个客户端
 	var _clients = this.clients;	
 	for(var clientId in _clients){
 		var c = _clients[clientId];
 		//每一个客户端的每一个订阅和现在待发送的主题进行比对
 		for(var i = 0; i < c.subscriptions.length; i++){
      		var _sub = c.subscriptions[i];
      		if(_sub.abs === true && _sub.topic === _topic){
      			//rtn.push({toClient: c, subQos: _sub.qos});
      			rtn[clientId] = _sub.qos;
      			break;
      		}else
      		if(_sub.abs === false && _sub.topic.test(_topic)){
      			//rtn.push({toClient: c, subQos: _sub.qos});
      			rtn[clientId] = _sub.qos;
      			break;
      		}
      	} 	
 	}
 	return rtn;
 };
 
 function pad(num, size){
 	var s = '00000000' + num;
 	return s.substr(s.length - size); 
 }
 
 //判断是否为空对象
 function isEmptyObject(obj){
 	for(var k in obj){
 		return false;
 	}
 	return true;
 }
 
 