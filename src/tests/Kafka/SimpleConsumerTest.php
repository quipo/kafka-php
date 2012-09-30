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
 * Override connect() method of base class
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_ConsumerMock extends Kafka_SimpleConsumer {
	public function connect() {
		if (null === $this->socket) {
			$this->socket = Kafka_Socket::createFromStream(fopen('php://temp', 'w+b'));
		}
	}

	public function writeInt4($n) {
		$this->socket->write(pack('N', $n));
	}

	public function writeInt2($n) {
		$this->socket->write(pack('n', $n));
	}
	
	public function rewind() {
		$this->socket->rewind();
	}

	public function getResponseSize() {
		return parent::getResponseSize();
	}

	public function getResponseCode() {
		return parent::getResponseCode();
	}
}

/**
 * Description of ProducerTest
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_SimpleConsumerTest extends PHPUnit_Framework_TestCase
{
	/**
	 * @var Kafka_Producer
	 */
	private $consumer;
	
	public function setUp() {
		$this->consumer = new Kafka_ConsumerMock('localhost', 1234, 10, 100000);
	}
	
	public function tearDown() {
		$this->consumer->close();
		unset($this->consumer);
	}

	/**
	 * @expectedException Kafka_Exception_OutOfRange
	 */
	public function testInvalidMessageSize() {
		$this->consumer->connect();
		$this->consumer->writeInt4(0);
		$this->consumer->rewind();
		$this->consumer->getResponseSize();
		$this->fail('The above call should throw an exception');
	}

	public function testMessageSize() {
		$this->consumer->connect();
		$this->consumer->writeInt4(10);
		$this->consumer->rewind();
		$this->assertEquals(10, $this->consumer->getResponseSize());
	}

	public function testMessageCode() {
		$this->consumer->connect();
		$this->consumer->writeInt2(1);
		$this->consumer->rewind();
		$this->assertEquals(1, $this->consumer->getResponseCode());
	}

	/**
	 * @expectedException Kafka_Exception_Socket_EOF
	 */
	public function testMessageSizeFailure() {
		$this->consumer->close();
		$this->consumer->getResponseSize();
		$this->fail('The above call should throw an exception');
	}

	/**
	 * @expectedException Kafka_Exception_Socket
	 */
	public function testConnectFailure() {
		$consumer = new Kafka_SimpleConsumer('invalid-host-name', 1234567890, 10, 1000000);
		$consumer->connect();
		$this->fail('The above call should throw an exception');
	}
}
