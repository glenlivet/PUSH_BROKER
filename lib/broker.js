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
 	 xmlParser		= require('./xml_parser.js'),
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
 	//TESTING
 	this.hasInitialized = false;
 	
 	this.msgCntr		= null;
 	
 	this.server			= null;

 	this.initialMsgs	= {};
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
 	/*
 	var _broker = this;
 	this.on('initDone', function(){
 		_broker.sendCachedMsgTo(_broker.msgCntr.id);
 	});
 	*/
 	this.msgCntr.publish({topic: SysMsgBuilder.MC_GET_CLIENT_INFO, qos: 0});
 }; 
 
 Broker.prototype.start = 
 function(){
 	var _broker = this;
 	this.server = mqtt.createServer(function(conn){
 		//当有客户端连接上来
 		conn.on('connect', function(packet){
 			_nodeLog.info('Client[clientId=' + packet.clientId + '] has sent a CONNECT');
 			//是否完成了初始化
 			if(!_broker.hasInitialized){
 				//判断是否为mc连接上来
 				if(_broker.isMc(packet)){
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
 					_nodeLog.info('Client[clientId=' + packet.clientId + '] rejected' + 
 									' because system initiating!');
 					
 					return;
 				}
 			}else{
 				//normal process
 				if(!_broker.clientAuthorized(packet)){
 					this.connack({returnCode: 4});
 					_nodeLog.info('Client[clientId=' + packet.clientId + '] rejected' + 
 									' because client unauthorized!');
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
 			_nodeLog.debug('Client[clientId=' + conn.id + '] has sent a PUBLISH' + util.inspect(packet, false, null));
 			//处理初始化
 			if(!_broker.hasInitialized){
 				if(packet.qos > 0){
 					_broker.msgCntr.puback({messageId : packet.messageId});
 				}
 				_broker.handleInitialCommand(packet.topic, packet.payload);
 				return;
 			}
 			//解析payload的xml来获取db_msg_id， created_time和真正的内容
 			var payloadObj = xmlParser.parse(packet.payload);
 			//处理retainedMsg
 			
 			//是否为系统信息 如果是处理系统信息
 			if(SysMsgBuilder.isSysMsg(payloadObj)){
 				//
 				return;
 			}
 			//获取发送列表
 			var targets = {};
 			var noTarget = false;
 			targets = _broker.getSubscribedClients(packet.topic);
 			//没有接收者
 			if(isEmptyObject(targets)) {
 				noTarget = true;
 				_nodeLog.info('PUBLISH on TOPIC: ' + packet.topic + ' got no subscriber!');
 				//考虑是否需要发送一个系统指令给mc
 				return;
 			}
 			//订阅qos>0的发送名单
 			var _publishList = [];
 			//qos>0的发送目标纪录
 			var shouldRecordTargets = targets;
 			for(var _cid in targets){
 				if(targets[_cid] > 0){
 					_publishList.push(_cid);
 				}else{
 					delete shouldRecordTargets[_cid];
 				}
 			}
 			//缓存msg
 			var _msg = null;
 			if(!isEmptyObject(shouldRecordTargets)){
 				_msg = new CachedMsg({
 					dbMid				: payloadObj.db_msg_id[0],
 					curMid				: _broker.midFac.createMid(),
 					topic				: packet.topic,
 					payload				: packet.payload,
 					publishRecipients	: shouldRecordTargets,
 					sender				: conn.id,
 					createdTime			: payloadObj.created_time[0]
 				});
 				_broker.addCachedMsg(_msg);
 			}
 			
 			
 			//将发送名单发送给mc
 			var _mcPostPublishList = new SysMsgBuilder.McPostPublishList({
 				dbMsgId		: payloadObj.db_msg_id[0],
 				recipients	: _publishList
 			});
 			_mcPostPublishList.publish(_broker);
 			_nodeLog.debug("PUBLISH LIST has been sent to MC!" + _mcPostPublishList.getXml());
 			
 			//发送消息
 			for(var _cid in targets){
 				var _client = _broker.clients[_cid];
 				//qos>0 需要messageId
 				if(targets[_cid]>0){
 					_client.publish({
 						topic		: _msg.topic,
 						payload		: _msg.payload,
 						qos			: targets[_cid],
 					 	messageId	: _msg.curMid
 					});
 				}
 				if(targets[_cid] == 0){
 					_client.publish({
 						topic		: packet.topic,
 						payload		: packet.payload,
 						qos			: 0
 					});
 				}
 			}
 			
 		});
 		
 		conn.on('pubrel', function(packet){
 			//TODO 暂时用不到 发送者只有mc 且qos=0
 		});
 		
 		conn.on('puback', function(packet){
 			//TODO
 			_nodeLog.debug('Client[clientId=' + conn.id + '] has sent a PUBACK');
 			//找到缓存msg，将接收人从接收人列表中删除
 			var _msgId = packet.messageId;
 			var _msg = null;
 			for(var i=0; i< _broker.cachedMsgs.length; i++){
 				if(_msgId === _broker.cachedMsgs[i].curMid){
 					_msg = _broker.cachedMsgs[i];
 					break;
 				}
 			}
 			if(_msg == null) {
  				this.emit('error', new Error('Unknown message id'));
  			}
  			var _clientId = conn.id;
  			var _client = _broker.clients[_clientId];
  			_msg.removePublishRecipient(_clientId);
  			_nodeLog.info('Client[clientId=' + _clientId + 
  							'] has been removed from PUBLISH LIST on cachedmsg[msgId=' 
  							+ _msgId + ']');
 			//发送给mc _MC_/_NOTIFY_/_PUBLISH_ACK_/[sysId]
 			var _mcNotifiyPublishAck = new SysMsgBuilder.McNotifyPublishAck({
  				dbMsgId				: _msg.dbMid,
  				recipient			: _client.id
  			});
  			_mcNotifiyPublishAck.publish(_broker);
 		});
 		
 		conn.on('pubrec', function(packet){
 			_nodeLog.debug('Client[clientId=' + conn.id + '] has sent a PUBREC');
 			//找到缓存的msg
 			var _msgId = packet.messageId;
 			var _msg = null;
 			for(var i=0; i< _broker.cachedMsgs.length; i++){
 				if(_msgId === _broker.cachedMsgs[i].curMid){
 					_msg = _broker.cachedMsgs[i];
 					break;
 				}
 			}
 			if(_msg == null) {
  				this.emit('error', new Error('Unknown message id'));
  			}
 			//缓存msg将该用户从publish转为pubrel
 			var _clientId = conn.id;
 			var _client = _broker.clients[_clientId];
 			_msg.publishToPubrel(_clientId);
 			//log
  			_nodeLog.info('Client[clientId=' + _clientId + 
  							'] has been moved from PUBLISH LIST to PUBREL LIST on cachedmsg[msgId=' 
  							+ _msgId + ']');
  			//系统指令 _MC_/_NOTIFY_/_PUBREL_ACK_
  			var _mcNotifyPubrelAck = new SysMsgBuilder.McNotifyPubrelAck({
  				dbMsgId				: _msg.db_msg_id,
  				clientId			: _clientId 
  			});
  			_mcNotifyPubrelAck.publish(_broker);
  			//do pubrel
  			_client.pubrel(packet);
  			//log
  			_nodeLog.info('PUBREL on msg[msgId=' + _msgId + '] has been sent back to Client[clientId=' + _client.id + ']');
 			//pubrel
 		});
 		
 		conn.on('pubcomp', function(packet){
 			var _client = _broker.clients[conn.id];
 			//log
  			_nodeLog.info('Client[clientId=' + conn.id + '] has sent a pubcomp');
  			//找到cachedMsg
  			var _msgId = packet.messageId;
  			var _msg = null;
  			for(var i=0;i<_broker.cachedMsgs.length;i++){
  				if(_msgId === _broker.cachedMsgs[i].curMid){
  					_msg = _broker.cachedMsgs[i];
  					break;
  				}
  			}
  			if(_msg == null) this.emit('error', new Error('Unknown message id'));
  			//发送系统指令_MC_/_NOTIFY_/_PUBLISH_ACK_
  			var _mcNotifiyPublishAck = new SysMsgBuilder.McNotifyPublishAck({
  				dbMsgId				: _msg.dbMid,
  				recipient			: _client.id
  			});
  			_mcNotifiyPublishAck.publish(_broker);
  			//delete the client from pubrelrecipients
  			_msg.removePubrelRecipient(_client.id);
  			//log
  			_nodeLog.info('Client[clientId=' + _client.id + '] has been removed from msg[msgId=' + _msgId + '] \'s PUBREL LIST');
  			
 		});
 		
 		conn.on('pingreq', function(packet){
 			conn.pingresp();
 		});
 		
 		conn.on('disconnect', function(packet){
 			//log
  			_nodeLog.warn('Client[clientId=' + conn.id + '] has disconnected');
  			conn.stream.end();
 		});
 		
 		conn.on('close', function(packet) {
 			//log
  			_nodeLog.info('Client[clientId=' + client.id + '] has closed the connection');
 		});
 		
 		conn.on('error', function(e) {
    		conn.stream.end();
    		_nodeLog.err(e);
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
 		var _msg = _self.cachedMsgs[i];
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
  				_self.clients[clientId].pubrel({messageId : _msg.curMid, dup: true});
  				break;
  			}
  		}
 	}
 	
 };
 
 Broker.prototype.handleInitialCommand = 
 function(topic, payload){
 	var patt_clientInfo_init = /_BROKER_\/_INIT_\/_CLIENT_INFO_/;
 	var patt_cachedMsg_init	= /_BROKER_\/_INIT_\/_CACHED_MSG_/;
 	var patt_retain_init = /_BROKER_\/_INIT_\/_RETAIN_MSG_/;
 	
 	if(patt_clientInfo_init.test(topic)){
 		this.handleClientInfoInit(payload);
 		this.msgCntr.publish({topic: '_MC_/_GET_/_CACHED_MSG_', qos: 0});
 	}
 	if(patt_cachedMsg_init.test(topic)){
 		this.handleCachedMsgInit(payload);
 		_nodeLog.info('Initialization Done!');
 		this.hasInitialized = true;
 		this.emit('initDone');
 		//this.msgCntr.publish({topic: '_MC_/_GET_/_RETAIN_MSG_', qos: 2, messageId: this.midFac.getReserved()});
 	}
 	if(patt_retain_init.test(topic)){
 		this.handleRetainedInit(payload);
 		_nodeLog.info('Initialization Done!');
 		this.hasInitialized = true;
 		this.emit('initDone');
 	}
 	
 };
 //TODO  去掉step
 Broker.prototype.handleClientInfoInit = 
 function(payload){
 	var _self = this;
 	_nodeLog.info('Init Client Info started!' + payload);
 	var _result = xmlParser.parse(payload);
 	var _clients = _result.client_info;
 	if(typeof _clients != 'undefined'){
 		for(var i=0; i<_clients.length; i++){
 			var _clientId = _clients[i].client_id[0];
 			/////////处理没有username和password的情况
 			
 			var _username = _clients[i].username[0];
 			var _password = _clients[i].password[0];
 			var _client = new Client(_clientId, _username, _password);
 			//处理没有订阅的情况
 			
 			var _subs = _clients[i].subscription;
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
 	//更新msgCntr在clients中的内容
 	_self.clients[_self.msgCntr.id] = _self.msgCntr;
 	_nodeLog.info('Init Client Info done!');
 };
 
 Broker.prototype.handleCachedMsgInit = 
 function(payload){
 	var _self = this;
 	_nodeLog.info('init cached msg start' + payload);
 	var _result = xmlParser.parse(payload);
 	var _msgs = _result.cached_msg;
 	_nodeLog.debug('initing cachedMsg length: ' + _msgs.length);
 	if((typeof _msgs) != 'undefined'){
 		for(var i=0; i<_msgs.length; i++){
 			var _dbMid				= _msgs[i].db_msg_id[0];
 			var _curMid				= _self.midFac.createMid();
 			var _createdTime		= _msgs[i].created_time[0];
 			var _topic				= _msgs[i].topic[0];
 			var _payload			= _msgs[i].content[0];
 			var _publishRecipients = {};
 			var _publishRecs = _msgs[i].publish_recipient;
 			if((typeof _publishRecs) != 'undefined'){
 				for(var j=0;j<_publishRecs.length;j++){
 					var _clientId = _publishRecs[j].client_id[0];
 					var _qos = _publishRecs[j].qos[0];
 					_publishRecipients[_clientId] = _qos;
 				}
 			}
 			//系统重启后 将没完成的任务重新发送，因为是新申请的mid之前发过pubrel
 			//没收到pubcomp的mid没法跟踪到。
 			var _pubrelRecipients = [];
 			var _pubrelRecs = _msgs[i].pubrel_recipient;
 			if((typeof _pubrelRecs) != 'undefined'){
 				for(var j=0;j<_pubrelRecs.length;j++){
 					var _clientId = _pubrelRecs[j].client_id[0];
 					_publishRecipients[_clientId] = 2;
 				}
 			}
 			var _readMsg = new CachedMsg({
 				dbMid				: _dbMid,
 				curMid				: _curMid,
 				createdTiem			: _createdTime,
 				topic				: _topic,
 				payload				: _payload,
 				publishRecipients	: _publishRecipients,
 				pubrelRecipients	: _pubrelRecipients
 			});
 			_self.cachedMsgs.push(_readMsg);
 			_nodeLog.debug(util.inspect(_readMsg, false, null));
 		}
 	}
 	
 	_nodeLog.info('init cached msg done');
 };
 
 Broker.prototype.handleRetainedInit = 
 function(payload){
 	var _self = this;
 	_nodeLog.info('init retained start' + payload);
 	
 	//TODO ~~~~~~~
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
 		//recycle the curMid
 		_broker.midFac.recycle(__mid);
 		//log
  		_nodeLog.info('msg[msgId=' + __mid + '] has done, mid has been recycled');
 	});
 	
 	_broker.cachedMsgs.push(_msg);
 };
 
  //判断是否为空对象
 function isEmptyObject(obj){
 	for(var k in obj){
 		return false;
 	}
 	return true;
 }
 
  
 function pad(num, size){
 	var s = '00000000' + num;
 	return s.substr(s.length - size); 
 }
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 