<?php
#!/usr/bin/php
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

// Sample code showing how to use the Zookeeper-based Kafka consumer

set_include_path(
	implode(PATH_SEPARATOR, array(
		realpath(__DIR__ . '/../lib'),
		get_include_path(),
	))
);
require 'autoloader.php';

// zookeeper address (one or more, separated by commas)
$zkaddress = 'localhost:8121';

// kafka topic to consume from
$topic = 'testtopic';

// kafka consumer group
$group = 'testgroup';

// socket buffer size: must be greater than the largest message in the queue
$socketBufferSize = 10485760; //10 MB

// approximate max number of bytes to get in a batch
$maxBatchSize = 20971520; //20 MB

$zookeeper = new Zookeeper($zkaddress);

$zkconsumer = new Kafka_ZookeeperConsumer(
	new Kafka_Registry_Topic($zookeeper),
	new Kafka_Registry_Broker($zookeeper),
	new Kafka_Registry_Offset($zookeeper, $group),
	$topic,
	$socketBufferSize
);

$messages = array();
try {
	foreach ($zkconsumer as $message) {
		// either process each message one by one, or collect them and process them in batches
		$messages[] = $message;
		if ($zkconsumer->getReadBytes() >= $maxBatchSize) {
			break;
		}
	}
} catch (Kafka_Exception_OffsetOutOfRange $exception) {
	// if we haven't received any messages, resync the offsets for the next time, then bomb out
	if ($zkconsumer->getReadBytes() == 0) {
		$zkconsumer->resyncOffsets();
		die($exception->getMessage());
	}
	// if we did receive some messages before the exception, carry on.
} catch (Kafka_Exception_Socket_Connection $exception) {
	// deal with it below
} catch (Kafka_Exception $exception) {
	// deal with it below
}

if (null !== $exception) {
	// if we haven't received any messages, bomb out
	if ($zkconsumer->getReadBytes() == 0) {
		die($exception->getMessage());
	}
	// otherwise log the error, commit the offsets for the messages read so far and return the data
}

// process the data in batches, wait for ACK

$success = doSomethingWithTheMessages($messages);

// Once the data is processed successfully, commit the byte offsets.
if ($success) {
	$zkconsumer->commitOffsets();
}

// get an approximate figure on the size of the queue
try {
	echo "\nRemaining bytes in queue: " . $consumer->getRemainingSize();
} catch (Kafka_Exception_Socket_Connection $exception) {
	die($exception->getMessage());
} catch (Kafka_Exception $exception) {
	die($exception->getMessage());
}

