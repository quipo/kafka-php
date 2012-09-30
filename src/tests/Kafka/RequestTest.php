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
 * Description of RequestTest
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_RequestTest extends PHPUnit_Framework_TestCase
{
	public function testEncodeDecode64bitShortUnsigned() {
		$short = 3;
		$encoded = Kafka_Request::packLong64bigendian($short);
		$this->assertEquals($short, Kafka_Request::unpackLong64bigendian($encoded));
	}

	public function testEncodeDecode64bitShortSigned() {
		$short = -3;
		$encoded = Kafka_Request::packLong64bigendian($short);
		$this->assertEquals($short, Kafka_Request::unpackLong64bigendian($encoded));
	}

	public function testEncodeDecode64bitIntUnsigned() {
		$int = 32767;
		$encoded = Kafka_Request::packLong64bigendian($int);
		$this->assertEquals($int, Kafka_Request::unpackLong64bigendian($encoded));

		$int = 32768;
		$encoded = Kafka_Request::packLong64bigendian($int);
		$this->assertEquals($int, Kafka_Request::unpackLong64bigendian($encoded));
	}

	public function testEncodeDecode64bitIntSigned() {
		$int = -32768;
		$encoded = Kafka_Request::packLong64bigendian($int);
		$this->assertEquals($int, Kafka_Request::unpackLong64bigendian($encoded));

		$int = -32769;
		$encoded = Kafka_Request::packLong64bigendian($int);
		$this->assertEquals($int, Kafka_Request::unpackLong64bigendian($encoded));
	}

	public function testEncodeDecode64bitLongUnsigned() {
		$long = 2147483647;
		$encoded = Kafka_Request::packLong64bigendian($long);
		$this->assertEquals($long, Kafka_Request::unpackLong64bigendian($encoded));

		$long = 4294967295;
		$encoded = Kafka_Request::packLong64bigendian($long);
		$this->assertEquals($long, Kafka_Request::unpackLong64bigendian($encoded));
	}

	public function testEncodeDecode64bitLongSigned() {
		$long = -2147483648;
		$encoded = Kafka_Request::packLong64bigendian($long);
		$this->assertEquals($long, Kafka_Request::unpackLong64bigendian($encoded));

		$long = -2147483649;
		$encoded = Kafka_Request::packLong64bigendian($long);
		$this->assertEquals($long, Kafka_Request::unpackLong64bigendian($encoded));
	}
}
