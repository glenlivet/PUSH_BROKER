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
 //init
 //发送请求msg_fac的数据
 Broker.prototype.init = 
 function(){
 	
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
 			
 			//处理retained msg
 			//获取发送列表
 			//如果qos>0 将发送列表发送给mc _MC_/_POST_/_PUBLISH_LIST_/[sysId]
 			//处理发送，按qos不同
 		});
 		
 		conn.on('pubrel', function(packet){
 			_nodeLog.info('in pubrel' + util.inspect(packet, false, null));
 			if(!_broker.hasInitialized){
 				var _msg = _broker.initialMsgs[packet.messageId];
 				//_nodeLog.debug('topic: ' + _msg.topic + ' payload: ' + _msg.payload);
 				_broker.handleInitialCommand(_msg.topic, _msg.payload);
 			}
 			//找到缓存的msg
 			//判断是否为系统初始化信息
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
 	}
 	
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
 }
 
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
 	
 }
 
 function pad(num, size){
 	var s = '00000000' + num;
 	return s.substr(s.length - size); 
 }
 
 