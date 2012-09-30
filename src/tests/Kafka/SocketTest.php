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
 * Description of FetchRequestTest
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_SocketTest extends PHPUnit_Framework_TestCase
{
	/**
	 * @expectedException Kafka_Exception_Socket_Connection
	 */
	public function testConnectNoHost() {
		$socket = new Kafka_Socket(null, 80);
		$socket->connect();
		$this->fail('The above connect() call should fail on a null host');
	}

	/**
	 * @expectedException Kafka_Exception_Socket_Connection
	 */
	public function testConnectInvalidPort() {
		$socket = new Kafka_Socket('localhost', 80);
		$socket->connect();
		$this->fail('The above connect() call should fail on an invalid port');
	}

	/**
	 * @expectedException Kafka_Exception_Socket_Connection
	 */
	public function testConnectInvalidHost() {
		$socket = new Kafka_Socket('invalid-host', 80);
		$socket->connect();
		$this->fail('The above connect() call should fail on an invalid host');
	}

	/**
	 * @expectedException Kafka_Exception_Socket
	 */
	public function testWriteNoStream() {
		$socket = Kafka_Socket::createFromStream(null);
		$socket->write('test');
		//$socket->rewind();
		//var_dump($socket->read(4));
		$this->fail('The above write() call should fail on a null socket');
	}

	/**
	 * @expectedException Kafka_Exception_Socket
	 */
	public function testWriteReadOnlySocket() {
		$roStream = fopen('php://temp', 'r');
		$socket = Kafka_Socket::createFromStream($roStream);
		$socket->write('test');
		//$socket->rewind();
		//var_dump($socket->read(4));
		$this->fail('The above write() call should fail on a read-only socket');
	}

	/**
	 * @expectedException Kafka_Exception_Socket_Timeout
	 */
	public function testWriteTimeout() {
		$this->markTestSkipped('find a better way of testing socket timeouts');
		$stream = fopen('php://temp', 'w+b');
		$socket = new Kafka_Socket('localhost', 0, 0, 0, -1, -1);
		$socket->setStream($stream);
		$socket->write('short timeout');
		//$socket->rewind();
		//var_dump($socket->read(4));
		$this->fail('The above write() call should fail on a socket with timeout = -1');
	}

	public function testWrite() {
		$socket = Kafka_Socket::createFromStream(fopen('php://temp', 'w+b'));
		$written = $socket->write('test');
		$this->assertEquals(4, $written);
	}

	public function testWriteAndRead() {
		$socket = Kafka_Socket::createFromStream(fopen('php://temp', 'w+b'));
		$written = $socket->write('test');
		$socket->rewind();
		$this->assertEquals('test', $socket->read(4));
	}

	public function testRead() {
		$stream = fopen('php://temp', 'w+b');
		fwrite($stream, 'test');
		fseek($stream, 0, SEEK_SET);
		$socket = Kafka_Socket::createFromStream($stream);
		$this->assertEquals('test', $socket->read(4));
	}

	public function testReadFewerBytes() {
		$stream = fopen('php://temp', 'w+b');
		fwrite($stream, 'tes');
		fseek($stream, 0, SEEK_SET);
		$socket = Kafka_Socket::createFromStream($stream);
		$this->assertEquals('tes', $socket->read(4));
	}

	/**
	 *
	 * @expectedException Kafka_Exception_Socket_EOF
	 */
	public function testReadFewerBytesVerifyLength() {
		$stream = fopen('php://temp', 'w+b');
		fwrite($stream, 'tes');
		fseek($stream, 0, SEEK_SET);
		$socket = Kafka_Socket::createFromStream($stream);
		$this->assertEquals('tes', $socket->read(4, true));
		$this->fail('The above call shoud throw an exception because the socket had fewer bytes than requested');
	}

	public function testReadMultiple() {
		$stream = fopen('php://temp', 'w+b');
		fwrite($stream, 'test1test2');
		fseek($stream, 0, SEEK_SET);
		$socket = Kafka_Socket::createFromStream($stream);
		$this->assertEquals('test1', $socket->read(5));
		$this->assertEquals('test2', $socket->read(5));
	}

	/**
	 * @expectedException Kafka_Exception_Socket
	 */
	public function testReadAfterClose() {
		$stream = fopen('php://temp', 'w+b');
		fwrite($stream, 'test');
		fseek($stream, 0, SEEK_SET);
		$socket = Kafka_Socket::createFromStream($stream);
		$socket->close();
		$socket->read(4);
		$this->fail('The above read() call should fail on a closed socket');
	}

	/**
	 * @expectedException Kafka_Exception_Socket
	 */
	public function testWriteAfterClose() {
		$stream = fopen('php://temp', 'w+b');
		$socket = Kafka_Socket::createFromStream($stream);
		$socket->close();
		$socket->write('test');
		$this->fail('The above write() call should fail on a closed socket');
	}
}
