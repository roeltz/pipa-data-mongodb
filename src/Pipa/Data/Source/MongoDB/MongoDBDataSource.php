<?php

namespace Pipa\Data\Source\MongoDB;
use DateTime;
use Mongo;
use MongoCode;
use MongoConnectionException;
use MongoDate;
use MongoDBRef;
use MongoId;
use Pipa\Data\Aggregate;
use Pipa\Data\Collection;
use Pipa\Data\Criteria;
use Pipa\Data\DataSource;
use Pipa\Data\DocumentDataSource;
use Pipa\Data\Exception\ConnectionException;
use Pipa\Data\Exception\QueryException;
use Psr\Log\LoggerInterface;

class MongoDBDataSource implements DataSource, DocumentDataSource {
	
	private $db;
	private $connection;
	private $queryBuilder;
	private $aggregateBuilder;
	private $logger;
	
	function __construct($db, $host, $user = null, $password = null, array $options = array()) {
		try {
			$conn = array();
			if ($user) {
				$conn[] = $user;
				if ($password) {
					$conn[] = ":";
					$conn[] = $password;
				}
				$conn[] = "@";
			}
			$conn[] = $host;
			$conn = join("", $conn);
			$this->connection = new Mongo("mongodb://$conn", $options);
			$this->queryBuilder = new MongoDBQueryBuilder($this);
			$this->aggregateBuilder = new MongoDBAggregateBuilder($this);
			$this->db = $db;
		} catch(MongoConnectionException $e) {
			throw new ConnectionException($e->getMessage());
		}
	}
	
	function setLogger(LoggerInterface $logger) {
		$this->logger = $logger;
	}

	function getCollection($name) {
		return new Collection($name);
	}
	
	function getConnection() {
		return $this->connection;
	}
	
	function getCriteria() {
		return new MongoDBCriteria(new Criteria($this));
	}
	
	function find(Criteria $criteria) {
		$query = $this->queryBuilder->build($criteria);
		$fields = array();
		
		foreach($criteria->fields as $field) {
			$fields[$field->name] = true;
		}
		
		if ($criteria->distinct) {
			$key = $criteria->fields[0]->name;
			$items = $this->connection->{$this->db}->{$criteria->collection->name}->distinct($key, $query);
			foreach($items as &$item) {
				$item = array($key=>$item);
			}
		} else {
			$cursor = $this->connection->{$this->db}->{$criteria->collection->name}->find($query, $fields);
		
			if ($criteria->order) {
				$cursor->sort($this->queryBuilder->buildSort($criteria));
			}
			
			if ($criteria->limit) {
				if ($criteria->limit->offset) {
					$cursor->skip($criteria->limit->offset);
				}
				$cursor->limit($criteria->limit->length);
			}
			
			$items = array_values(iterator_to_array($cursor));
		}
		
		foreach($items as &$item) {
			$this->processItem($item);
		}
		
		return $items;
	}
	
	function count(Criteria $criteria) {
		$query = $this->queryBuilder->build($criteria);
		$count = $this->connection->{$this->db}->{$criteria->collection->name}->count($query);
		return $count;
	}
	
	function aggregate(Aggregate $aggregate, Criteria $criteria) {
		$query = $this->queryBuilder->build($criteria);
		list($mapfn, $reducefn) = $this->aggregateBuilder->build($aggregate);

		$command = array(
			'mapreduce'=>$criteria->collection->name,
			'query'=>count($query) ? $query : null,
			'map'=>$mapfn,
			'reduce'=>$reducefn,
			'out'=> array('inline'=>true)
		);

		$result = $this->connection->{$this->db}->command($command);
		
		if ($result['ok']) {
			return @$result['results'][0]['value'];
		} else {
			throw new QueryException($result['errmsg'] . (@$result['assertion'] ? ": " . $result['assertion'] : ""));
		}
	}
	
	function save(array $values, Collection $collection, $sequence = null) {
		$this->queryBuilder->deprivatizeValues($values);
		$this->recursiveEscape($values);
		$this->connection->{$this->db}->{$collection->name}->insert($values, array('safe'=>true));
		$id = $values["_id"] instanceof MongoId ? (string) $values["_id"] : $values["_id"];
		return $id;
		
	}
	
	function update(array $values, Criteria $criteria) {
		unset($values['_id']);
		$this->recursiveEscape($values);
		$query = $this->queryBuilder->build($criteria);
		$this->connection->{$this->db}->{$criteria->collection->name}->update($query, array('$set'=>$values), array('safe'=>true));
	}
	
	function delete(Criteria $criteria) {
		$query = $this->queryBuilder->build($criteria);
		$this->connection->{$this->db}->{$criteria->collection->name}->remove($query, array('safe'=>true));		
	}
	
	function mapReduce(Criteria $criteria, $map, $reduce, $finalize = null, array $scope = null) {
		$command = array(
			'mapReduce'=>$criteria->collection->name,
			'map'=>new MongoCode($map),
			'reduce'=>new MongoCode($reduce),
			'out'=>array('inline'=>1)
		);
		
		if ($finalize) {
			$command['finalize'] = new MongoCode($finalize);
		}
		
		if ($criteria->expressions) {
			$command['query'] = $this->queryBuilder->build($criteria);
		}
		
		if ($criteria->order) {
			$command['sort'] = $this->queryBuilder->buildSort($criteria);
		}
		
		if ($scope) {
			$command['scope'] = $scope;
		}
		
		if ($this->logger) {
			$this->logger->debug("MapReduce: Collection: {$command['mapReduce']}, Query: ".json_encode(@$command['query']));
		}

		$result = $this->connection->{$this->db}->command($command);

		if ($result['ok']) {
			return $result['results'];
		} else {
			$assertion = isset($result['assertion']) ? ": {$result['assertion']}" : "";
			throw new QueryException("{$result['errmsg']}{$assertion}", $result['code']);
		}		
	}
	
	function processItem(&$value) {
		$self = $this;
		$db = $this->connection->{$this->db};
		\Pipa\object_walk_recursive($value, function(&$value) use($self, $db) {
			if ($value instanceof MongoId) {
				$value = (string) $value;
				return false;
			} elseif ($value instanceof MongoDate) {
				$value = new DateTime(date("Y-m-d H:i:s", $value->sec));
				return false;
			} elseif (MongoDBRef::isRef($value)) {
				$value = $db->getDBRef($value);
			}
		});
		return $value;
	}
	
	function escape($value) {
		switch (gettype($value)) {
			case "object":
				if ($value instanceof DateTime) {
					return new MongoDate($value->getTimestamp());
				}
			default:
				return $value;
		}
	}
	
	function recursiveEscape(&$values) {
		$self = $this;
		\Pipa\object_walk_recursive($values, function(&$value) use($self){
			$value = $self->escape($value);
		});
	}
}
