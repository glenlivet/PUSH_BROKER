/**
 * New node file
 */
 
 
var Client = module.exports = 
function(id, username, password){
	this.id = id;
	this.username = username||null;
	this.password = password||null;
	this.conn	  = null;
	this.subscriptions = [];
};

Client.prototype.changeUsernameAndPassword = 
function(username, password){
	this.username = username;
	this.password = password;
};

Client.prototype.checkUsernameAndPassword = 
function(username, password){
	if(this.username == username && this.password == password)
		return true;
	else
		return false;
};

Client.prototype.changePassword = 
function(password){
	this.password = password;
};

Client.prototype.addSubscription = 
function(sub){
	this.subscriptions.push(sub);
};

Client.prototype.removeSubscription = 
function(_topic){
	var _subs = this.subscriptions;
	for(var j=0;j<_subs.length;j++){
  		if(_subs[j].topicDesc == _topic){
  			_subs.splice(j,1);
  			break;
  		}
  	}
  	this.subscriptions = _subs;
};

Client.prototype.setConn = 
function(conn){
	this.conn = conn;
};

Client.prototype.publish = 
function(packet){
	var _conn = this.conn;
	process.nextTick(function(){
		_conn.publish(packet);
	});
};

Client.prototype.pubrec = 
function(packet){
	var _conn = this.conn;
	process.nextTick(function(){
		_conn.pubrec(packet);
	});
};

Client.prototype.pubrel = 
function(packet){
	var _conn = this.conn;
	process.nextTick(function(){
		_conn.pubrel(packet);
	});
};

Client.prototype.pubcomp = 
function(packet){
	var _conn = this.conn;
	process.nextTick(function(){
		_conn.pubcomp(packet);
	});
};
