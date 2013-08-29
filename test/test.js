/**
 * New node file
 */
var extend = require('./extended_object.js');
var Obj	   =  require('./object.js');
var extObj = new extend.extendedObject();
var _obj   = new Obj.myObject();
extObj.changeProperty(300);
_obj.changeProperty(300);

extObj.invoke();
_obj.invoke();