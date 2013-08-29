/**
 * 该方法有一个问题，异步的时候，需要同步方法！！！
 */
 var util = require("util");
 var events = require("events");
 //messageID最大值
 var _max = 65534;
 
 /**
  * MidFactory Constructor
  * Message ID 工厂
  *
  * @property <Array> recycled 回收可再利用message ID
  * @property <Number> available 可用的messageID
  */
var MidFactory = module.exports= 
function MidFactory(recycled, available){
	
	this.recycled = recycled||[];
	this.available = available||1;
	this.canReserve = true;
	events.EventEmitter.call(this);
};
//继承EventEmitter
util.inherits(MidFactory, events.EventEmitter);
/**
 * createdMid 创建一个可用messageID
 * 先用回收的。
 * 当用到65536时，溢出，抛出溢出事件
 */
MidFactory.prototype.createMid = 
function(){
	if(this.recycled.length>0)
		return this.recycled.shift();
	if(this.available < _max)
		return this.available++;
	this.emit('mid_overflow');
	return -1;
};

MidFactory.prototype.getReserved = 
function(){
	if(this.canReserve){
		this.canReserve = false;
		return 65535;
	}else{
		this.emit('VITAL_ERROR');
		console.log('MESSAGE-ID -1!!!!!!');
		return -1;
	}
	
};

MidFactory.prototype.releaseReserve = 
function(){
	this.canReserve = true;
};

/**
 * recycle 回收完成推送的message id
 */
MidFactory.prototype.recycle =
function(mid){
	this.recycled.push(mid);
};

//重置工厂 
MidFactory.prototype.reset = 
function(){
	this.recycled = [];
	this.available = 1;
};


