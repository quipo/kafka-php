<?php
/**
 * Kafka Client
 *
 * @category  Libraries
 * @package   Kafka
 * @author    Lorenzo Alberton <l.alberton@quipo.it>
 * @copyright 2012 Lorenzo Alberton
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @version   $Revision: $
 * @link      http://sna-projects.com/kafka/
 */

/**
 * Encode messages and messages sets into the kafka protocol
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_Encoder
{
	/**
	 * 1 byte "magic" identifier to allow format changes
	 *
	 * @const integer
	 */
	const CURRENT_MAGIC_VALUE = 1;

	const COMPRESSION_NONE   = 0;
	const COMPRESSION_GZIP   = 1;
	const COMPRESSION_SNAPPY = 2;

	/**
	 * Encode a message. The format of an N byte message is the following:
	 *  - 1 byte: "magic" identifier to allow format changes
	 *  - 1 byte:  "compression-attributes" for compression alogrithm
	 *  - 4 bytes: CRC32 of the payload
	 *  - (N - 6) bytes: payload
	 *
	 * @param string $msg Message to encode
	 *
	 * @return string
	 * @throws Kafka_Exception
	 */
	static public function encode_message($msg, $compression = self::COMPRESSION_NONE) {
		$compressed = self::compress($msg, $compression);
		// <MAGIC_BYTE: 1 byte> <COMPRESSION: 1 byte> <CRC32: 4 bytes bigendian> <PAYLOAD: N bytes>
		return pack('CCN', self::CURRENT_MAGIC_VALUE, $compression, crc32($compressed))
			 . $compressed;
	}

	/**
	 * Compress a message
	 *
	 * @param string  $msg         Message
	 * @param integer $compression 0=none, 1=gzip, 2=snappy
	 *
	 * @return string
	 * @throws Kafka_Exception
	 */
	static public function compress($msg, $compression) {
		switch ($compression) {
			case self::COMPRESSION_NONE:
				return $msg;
			case self::COMPRESSION_GZIP:
				return gzencode($msg);
			case self::COMPRESSION_SNAPPY:
				throw new Kafka_Exception_NotSupported('SNAPPY compression not yet implemented');
			default:
				throw new Kafka_Exception_NotSupported('Unknown compression flag: ' . $compression);
		}
	}

	/**
	 * Decompress a message
	 *
	 * @param string  $msg         Message
	 * @param integer $compression 0=none, 1=gzip, 2=snappy
	 *
	 * @return string
	 * @throws Kafka_Exception
	 */
	static public function decompress($msg, $compression) {
		switch ($compression) {
			case self::COMPRESSION_NONE:
				return $msg;
			case self::COMPRESSION_GZIP:
				// NB: this is really a MessageSet, not just a single message
				// although I'm not sure this is the best way to handle the inner offsets,
				// as the symmetry with the outer collection iteration is broken.
				// @see https://issues.apache.org/jira/browse/KAFKA-406
				$stream = fopen('php://temp', 'w+b');
				fwrite($stream, gzinflate(substr($msg, 10)));
				rewind($stream);
				$socket = Kafka_Socket::createFromStream($stream);
				return new Kafka_MessageSetInternalIterator($socket, 0, 0);
			case self::COMPRESSION_SNAPPY:
				throw new Kafka_Exception_NotSupported('SNAPPY decompression not yet implemented');
			default:
				throw new Kafka_Exception_NotSupported('Unknown compression flag: ' . $compression);
		}
	}

	/**
	 * Encode a complete request
	 *
	 * @param string  $topic       Topic
	 * @param integer $partition   Partition number
	 * @param array   $messages    Array of messages to send
	 * @param integer $compression flag for type of compression
	 *
	 * @return string
	 * @throws Kafka_Exception
	 */
	static public function encode_produce_request($topic, $partition, array $messages, $compression = self::COMPRESSION_NONE) {
		// not sure I agree this is the best design for compressed messages
		// @see https://issues.apache.org/jira/browse/KAFKA-406
		$compress = ($compression !== self::COMPRESSION_NONE);
		$message_set = '';
		foreach ($messages as $message) {
			$encoded = self::encode_message($message, self::COMPRESSION_NONE);
			// encode messages as <LEN: int><MESSAGE_BYTES>
			$message_set .= pack('N', strlen($encoded)) . $encoded;
		}
		if ($compress) {
			$encoded = self::encode_message($message_set, $compression);
			$message_set = pack('N', strlen($encoded)) . $encoded;
		}

		// create the request as <REQUEST_SIZE: int> <REQUEST_ID: short> <TOPIC: bytes> <PARTITION: int> <BUFFER_SIZE: int> <BUFFER: bytes>
		$data = pack('n', Kafka_RequestKeys::PRODUCE) .
			pack('n', strlen($topic)) . $topic .
			pack('N', $partition) .
			pack('N', strlen($message_set)) . $message_set;
		return pack('N', strlen($data)) . $data;
	}
}
