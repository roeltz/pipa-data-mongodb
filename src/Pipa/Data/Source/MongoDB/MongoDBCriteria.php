<?php

namespace Pipa\Data\Source\MongoDB;
use Pipa\Data\CriteriaDecorator;

class MongoDBCriteria extends CriteriaDecorator {
	
	function mapReduce($map, $reduce, $finalize, array $scope = null) {
		$result = $this->criteria->dataSource->mapReduce($this->criteria, $map, $reduce, $finalize, $scope);
		if ($this->index)
			$result = $this->indexResult($this->index, $result);
		return $result;
	}
}
