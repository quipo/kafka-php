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
class Kafka_ProducerMock extends Kafka_Producer {
	public function connect() {
		if (null === $this->socket) {
			$this->socket = Kafka_Socket::createFromStream(fopen('php://temp', 'w+b'));
		}
	}
	
	public function getData() {
		$this->connect();
		$this->socket->rewind();
		return $this->socket->read(10000000);
	}

	public function getHost() {
		return $this->host;
	}
	
	public function getPort() {
		return $this->port;
	}

	public function getCompression() {
		return $this->compression;
	}
}

/**
 * Description of ProducerTest
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_ProducerTest extends PHPUnit_Framework_TestCase
{
	/**
	 * @var Kafka_Producer
	 */
	private $producer;
	
	public function setUp() {
		$this->producer = new Kafka_ProducerMock('localhost', 1234, Kafka_Encoder::COMPRESSION_NONE);
	}
	
	public function tearDown() {
		$this->producer->close();
		unset($this->producer);
	}

	public function testProducer() {
		$messages = array(
			'test 1',
			'test 2 abc',
		);
		$topic = 'a topic';
		$partition = 3;
		$this->producer->send($messages, $topic, $partition);
		$sent = $this->producer->getData();
		$this->assertContains($topic, $sent);
		$this->assertContains($partition, $sent);
		foreach ($messages as $msg) {
			$this->assertContains($msg, $sent);
		}
	}

	/**
	 * @expectedException Kafka_Exception_Socket
	 */
	public function testConnectFailure() {
		$producer = new Kafka_Producer('invalid-host-name', 1234567890, Kafka_Encoder::COMPRESSION_NONE);
		$producer->connect();
		$this->fail('The above call should throw an exception');
	}

	public function testSerialize() {
		$producer = new Kafka_ProducerMock('host', 1234, Kafka_Encoder::COMPRESSION_SNAPPY);
		$serialized = serialize($producer);
		$unserialized = unserialize($serialized);
		$this->assertEquals('host', $unserialized->getHost());
		$this->assertEquals(1234, $unserialized->getPort());
		$this->assertEquals(Kafka_Encoder::COMPRESSION_SNAPPY, $unserialized->getCompression());
	}
}
