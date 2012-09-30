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
class Kafka_MessageSetTest extends PHPUnit_Framework_TestCase
{
	private function getMessageSetBuffer(array $messages) {
		$message_set = '';
		foreach ($messages as $message) {
			$encoded = Kafka_Encoder::encode_message($message, Kafka_Encoder::COMPRESSION_NONE);
			// encode messages as <LEN: int><MESSAGE_BYTES>
			$message_set .= pack('N', strlen($encoded)) . $encoded;
		}
		return $message_set;
	}

	private function writeDummyMessageSet($stream, array $messages) {
		return fwrite($stream, $this->getMessageSetBuffer($messages));
	}

	private function writeDummyCompressedMessageSet($stream, array $messages, $compression) {
		$encoded = Kafka_Encoder::encode_message($this->getMessageSetBuffer($messages), $compression);
		return fwrite($stream, pack('N', strlen($encoded)) . $encoded);
	}

	public function testIterator() {
		$stream = fopen('php://temp', 'w+b');
		$messages = array('message #1', 'message #2', 'message #3');
		$this->writeDummyMessageSet($stream, $messages);
		rewind($stream);
		$socket = Kafka_Socket::createFromStream($stream);
		$set = new Kafka_MessageSet($socket, 0, 0);
		$idx = 0;
		foreach ($set as $offset => $msg) {
			$this->assertEquals($messages[$idx++], $msg->payload());
		}
		$this->assertEquals(count($messages), $idx);

		// test new offset
		$readBytes = $set->validBytes();
		$this->assertEquals(60, $readBytes);
		$readBytes = $set->sizeInBytes();
		$this->assertEquals(60, $readBytes);

		// no more data
		$set = new Kafka_MessageSet($socket, $readBytes, 0);
		$cnt = 0;
		foreach ($set as $offset => $msg) {
			$cnt++;
		}
		$this->assertEquals(0, $cnt);

		fclose($stream);
	}

	public function testIteratorInvalidLastMessage() {
		$stream1 = fopen('php://temp', 'w+b');
		$messages = array('message #1', 'message #2', 'message #3');
		$size = $this->writeDummyMessageSet($stream1, $messages);
		rewind($stream1);
		$stream = fopen('php://temp', 'w+b');
		fwrite($stream, fread($stream1, $size - 2)); // copy partial stream buffer
		rewind($stream);

		$socket = Kafka_Socket::createFromStream($stream);
		$set = new Kafka_MessageSet($socket, 0, 0);
		$idx = 0;
		foreach ($set as $offset => $msg) {
			$this->assertEquals($messages[$idx++], $msg->payload());
		}
		$this->assertEquals(count($messages) - 1, $idx); // the last message should NOT be returned
		fclose($stream);

		// test new offset
		$readBytes = $set->validBytes();
		$this->assertEquals(40, $readBytes);
	}

	public function testOffset() {
		$stream = fopen('php://temp', 'w+b');
		$messages = array('message #1', 'message #2', 'message #3');
		$this->writeDummyMessageSet($stream, $messages);
		$offsetOfSecondMessage = 20; // manually calculated
		fseek($stream, $offsetOfSecondMessage, SEEK_SET);

		$socket = Kafka_Socket::createFromStream($stream);
		$set = new Kafka_MessageSet($socket, $offsetOfSecondMessage, 0);

		$cnt = 0;
		$idx = 1;
		foreach ($set as $offset => $msg) {
			$cnt++;
			$this->assertEquals($messages[$idx++], $msg->payload());
		}
		$this->assertEquals(2, $cnt);
		fclose($stream);

		// test new offset
		$readBytes = $set->validBytes();
		$this->assertEquals(40, $readBytes);
	}

	public function testOffset2() {
		$stream = fopen('php://temp', 'w+b');
		$messages = array('message #1', 'message #2', 'message #3');
		$this->writeDummyMessageSet($stream, $messages);
		$offsetOfThirdMessage = 40; // manually calculated

		fseek($stream, $offsetOfThirdMessage, SEEK_SET);
		$socket = Kafka_Socket::createFromStream($stream);
		$set = new Kafka_MessageSet($socket, $offsetOfThirdMessage, 0);

		$cnt = 0;
		foreach ($set as $offset => $msg) {
			$cnt++;
			$this->assertEquals($messages[2], $msg->payload());
		}
		$this->assertEquals(1, $cnt);
		fclose($stream);

		// test new offset
		$readBytes = $set->validBytes();
		$this->assertEquals(20, $readBytes);
	}

	public function testCompressedMessages() {
		$stream = fopen('php://temp', 'w+b');
		$messages = array('message #1', 'message #2', 'message #3');
		$this->writeDummyCompressedMessageSet($stream, $messages, Kafka_Encoder::COMPRESSION_GZIP);
		rewind($stream);
		$socket = Kafka_Socket::createFromStream($stream);
		$set = new Kafka_MessageSet($socket, 0, 0);
		$idx = 0;
		foreach ($set as $offset => $msg) {
			$this->assertEquals($messages[$idx++], $msg->payload());
		}
		$this->assertEquals(count($messages), $idx);

		// test new offset
		$readBytes = $set->validBytes();
		$this->assertEquals(69, $readBytes);

		// no more data
		$set = new Kafka_MessageSet($socket, $readBytes, 0);
		$cnt = 0;
		foreach ($set as $offset => $msg) {
			$cnt++;
		}
		$this->assertEquals(0, $cnt);

		fclose($stream);
	}

	public function testMixedMessages() {
		$stream = fopen('php://temp', 'w+b');
		$messages = array('message #1', 'message #2', 'message #3');
		$this->writeDummyCompressedMessageSet($stream, $messages, Kafka_Encoder::COMPRESSION_GZIP);
		$messages2 = array('message #4', 'message #5', 'message #6');
		$this->writeDummyMessageSet($stream, $messages2, Kafka_Encoder::COMPRESSION_NONE);
		$this->writeDummyCompressedMessageSet($stream, $messages, Kafka_Encoder::COMPRESSION_GZIP);
		rewind($stream);

		$allMessages = $messages;
		foreach ($messages2 as $msg) {
			$allMessages[] = $msg;
		}
		foreach ($messages as $msg) {
			$allMessages[] = $msg;
		}

		$socket = Kafka_Socket::createFromStream($stream);
		$set = new Kafka_MessageSet($socket, 0, 0);
		$idx = 0;
		foreach ($set as $offset => $msg) {
			$this->assertEquals($allMessages[$idx++], $msg->payload());
		}
		$this->assertEquals(count($allMessages), $idx);

		// test new offset
		$readBytes = $set->validBytes();
		$this->assertEquals(198, $readBytes);

		// no more data
		$set = new Kafka_MessageSet($socket, $readBytes, 0);
		$cnt = 0;
		foreach ($set as $offset => $msg) {
			$cnt++;
		}
		$this->assertEquals(0, $cnt);

		fclose($stream);
	}
}
