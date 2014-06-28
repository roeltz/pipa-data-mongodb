<?php

namespace Pipa\Data\Source\MongoDB;
use stdClass;
use MongoId;
use MongoRegex;
use Pipa\Data\Criteria;
use Pipa\Data\HasPrimaryKey;
use Pipa\Data\Restrictions;
use Pipa\Data\Expression;
use Pipa\Data\Expression\ComparissionExpression;
use Pipa\Data\Expression\JunctionExpression;
use Pipa\Data\Expression\ListExpression;
use Pipa\Data\Expression\NegationExpression;
use Pipa\Data\Expression\RangeExpression;
use Pipa\Data\Order;

class MongoDBQueryBuilder {
	
	private $dataSource;
	
	function __construct(MongoDBDataSource $dataSource) {
		$this->dataSource = $dataSource;
	}

	function build(Criteria $criteria) {
		if (count($criteria->expressions)) {
			$query = $this->renderExpression(Restrictions::_and($criteria->expressions));		
			return $this->deprivatizeValues($query);
		} else
			return array();
	}
	
	function buildSort(Criteria $criteria) {
		$orders = array();
		foreach($criteria->order as $order)
			$orders[$order->field->name] = $order->type == Order::TYPE_ASC ? 1 : -1;
		return $orders;
	}
	
	function deprivatizeValues(&$values) {
		\Pipa\object_walk_recursive($values, function(&$value){
			if (is_object($value)
				&& !($value instanceof DateTime)) {
					
				if ($value instanceof HasPrimaryKey) {
					$value = $value->getPrimaryKeyValue();
				} elseif (!($value instanceof MongoId)) {
					$value2 = new stdClass;
					foreach($value as $k=>$v)
						$value2->$k = $v;
					$value = $value2;
				}
			}
		});
		return $values;
	}
	
	function resolveExpressionOperator($field, $value, $operator) {

		if ($field->name == "_id" && strlen($value) == 24 && ctype_xdigit($value))
			$value = new MongoId($value);
		else
			$value = $this->dataSource->escape($value);
		
		switch($operator) {
			case '=':
				if ($value === null)
					return array($field->name=>array('$type'=>10));
				else
					return array($field->name=>$this->dataSource->escape($value));
			case 'like':
				$value = preg_replace('/[()[\]\\.*?|]/', '\\\\$1', $value);
				$value = str_replace('%', '.*', $value);
				$value = str_replace('_', '.', $value);
				$value = "/$value/m";
			case 'regex':
				return array($field->name=>array('$regex'=>new MongoRegex($value)));
		}
		
		$equiv = array('='=>'$eq', '<>'=>'$ne',
						'<'=>'$lt', '>'=>'$gt', '<='=>'$lte', '>='=>'$gte');

		return array($field->name=>array($equiv[$operator]=>$value));
	}
	
	function renderComparissionExpression(ComparissionExpression $expression) {
		return $this->resolveExpressionOperator($expression->a, $expression->b, $expression->operator);
	}
	
	function renderRangeExpression(RangeExpression $expression) {
		return array($expression->field->name => array('$gte'=>$this->dataSource->escape($expression->min), '$lte'=>$this->dataSource->escape($expression->max)));
	}

	function renderListExpression(ListExpression $expression) {
		$equiv = array(ListExpression::OPERATOR_IN=>'$in', ListExpression::OPERATOR_NOT_IN=>'$nin');
		$values = array();
		foreach($expression->values as $v)
			$values[] = $this->dataSource->escape($v);
		return array($expression->field->name => array($equiv[$expression->operator]=>$values));
	}
	
	function renderNegationExpression(NegationExpression $expression) {
		$criteria = $this->renderExpression($expression);
		return array('$not'=>$criteria);		
	}
	
	function renderJunctionExpression(JunctionExpression $expression) {
		$criteria = array();
		if ($expression->operator == JunctionExpression::OPERATOR_CONJUNCTION) {
			foreach($expression->expressions as $e) {
				$criteria = array_merge_recursive($criteria, $this->renderExpression($e));
			}
		} else {
			$criteria['$or'] = array();
			foreach($expression->expressions as $e) {
				$criteria['$or'][] = $this->renderExpression($e);
			}
		}
		return $criteria;
	}
	
	function renderExpression(Expression $expression) {
		if ($expression instanceof ComparissionExpression) {
			return $this->renderComparissionExpression($expression);
		} elseif ($expression instanceof RangeExpression) {
			return $this->renderRangeExpression($expression);
		} elseif ($expression instanceof ListExpression) {
			return $this->renderListExpression($expression);
		} elseif ($expression instanceof NegationExpression) {
			return $this->renderNegationExpression($expression);
		} elseif ($expression instanceof JunctionExpression) {
			return $this->renderJunctionExpression($expression);
		}
	}

}
