<?php

namespace Pipa\Data\Source\MongoDB;
use MongoCode;
use Pipa\Data\Criteria;
use Pipa\Data\Aggregate;

class MongoDBAggregateBuilder {
	
	function build(Aggregate $aggregate) {
		
		switch($aggregate->operation) {
			case Aggregate::OPERATION_SUM:
				$mapfn = new MongoCode( 
<<<MAP
function() {
	var value = this.{$aggregate->field->name};
	if (value instanceof Date) {
		value = value.getTime();
	} else if (value instanceof Object) {
		var sum = 0;
		for(var i in value)
			sum += parseFloat(value[i]);
		value = sum;
	} else {
		value = parseFloat(value);
	}
	emit("{$aggregate->field->name}", value);
}
MAP
				);
				$reducefn = new MongoCode(
<<<REDUCE
function(k, values) {
	var r = 0;
	values.forEach(function(v){ r += v });
	return r;
}
REDUCE
				);
				break;

			case Aggregate::OPERATION_AVG:
				$mapfn = new MongoCode( 
<<<MAP
function() {
	var value = this.{$aggregate->field->name};
	if (value instanceof Date) {
		value = value.getTime();
	} else if (value instanceof Object) {
		var sum = 0;
		for(var i in value)
			sum += parseFloat(value[i]);
		value = sum;
	} else {
		value = parseFloat(value);
	}
	emit("{$aggregate->field->name}", value);
}
MAP
				);
				$reducefn = new MongoCode(
<<<REDUCE
function(k, values) {
	var r = 0;
	values.forEach(function(v){ r += v });
	return r / (values.length || 1);
}
REDUCE
				);
				break;

			case Aggregate::OPERATION_MAX:
				$mapfn = new MongoCode( 
<<<MAP
function() {
	var value = this.{$aggregate->field->name};
	if (value instanceof Date) {
		value = value.getTime();
	} else if (value instanceof Object) {
		var sum = 0;
		for(var i in value)
			sum += parseFloat(value[i]);
		value = sum;
	} else {
		value = parseFloat(value);
	}
	emit("{$aggregate->field->name}", value);
}
MAP
				);
				$reducefn = new MongoCode(
<<<REDUCE
function(k, values) {
	return Math.max.apply(Math, values);
}
REDUCE
				);
				break;

			case Aggregate::OPERATION_MIN:
				$mapfn = new MongoCode( 
<<<MAP
function() {
	var value = this.{$aggregate->field->name};
	if (value instanceof Date) {
		value = value.getTime();
	} else if (value instanceof Object) {
		var sum = 0;
		for(var i in value)
			sum += parseFloat(value[i]);
		value = sum;
	} else {
		value = parseFloat(value);
	}
	emit("{$aggregate->field->name}", value);
}
MAP
				);
				$reducefn = new MongoCode(
<<<REDUCE
function(k, values) {
	return Math.min.apply(Math, values);
}
REDUCE
				);
				break;
		}
		
		return array($mapfn, $reducefn);
	}
}
