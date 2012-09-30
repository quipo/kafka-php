<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_ResponseTest extends PHPUnit_Framework_TestCase
{
	/**
	 * @expectedException Kafka_Exception_OffsetOutOfRange
	 */
	public function testErrorCodeValidationOffsetOutOfRange() {
		Kafka_Response::validateErrorCode(1);
		$this->fail('the line above should throw an exception');
	}

	/**
	 * @expectedException Kafka_Exception_InvalidMessage
	 */
	public function testErrorCodeValidationInvalidMessage() {
		Kafka_Response::validateErrorCode(2);
		$this->fail('the line above should throw an exception');
	}

	/**
	 * @expectedException Kafka_Exception_WrongPartition
	 */
	public function testErrorCodeValidationWrongPartition() {
		Kafka_Response::validateErrorCode(3);
		$this->fail('the line above should throw an exception');
	}

	/**
	 * @expectedException Kafka_Exception_InvalidFetchSize
	 */
	public function testErrorCodeValidationInvalidFetchSize() {
		Kafka_Response::validateErrorCode(4);
		$this->fail('the line above should throw an exception');
	}

	/**
	 * @expectedException Kafka_Exception
	 */
	public function testErrorCodeValidationUnknown() {
		Kafka_Response::validateErrorCode(20);
		$this->fail('the line above should throw an exception');
	}
}
