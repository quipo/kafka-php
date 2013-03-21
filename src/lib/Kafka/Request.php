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
 * Abstract Request class
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
abstract class Kafka_Request
{
	/**
	 * @var integer
	 */
	public $id;

	/**
	 * @var string
	 */
	protected $topic;

	/**
	 * @var integer
	 */
	protected $partition;

	/**
	 * Write the request to the output stream
	 *
	 * @param Kafka_Socket $socket Output stream
	 *
	 * @return void
	 */
	abstract public function writeTo(Kafka_Socket $socket);

	/**
	 * Get request size in bytes
	 *
	 * @return integer
	 */
	abstract public function sizeInBytes();

	/**
	 * Write the Request Header
	 * <req_len> + <req_type> + <topic_len> + <topic> + <partition>
	 *
	 * @param Kafka_Socket $socket Socket
	 *
	 * @return void
	 */
	protected function writeRequestHeader(Kafka_Socket $socket) {
		// REQUEST_LENGTH (int) + REQUEST_TYPE (short)
		$socket->write(pack('N', $this->sizeInBytes() + 2));
		$socket->write(pack('n', $this->id));

		// TOPIC_SIZE (short) + TOPIC (bytes)
		$socket->write(pack('n', strlen($this->topic)) . $this->topic);
		// PARTITION (int)
		$socket->write(pack('N', $this->partition));
	}

	/**
	 * Pack a 64bit integer as big endian long
	 *
	 * @param integer $big Big int
	 *
	 * @return bytes
	 */
	static public function packLong64bigendian($big) {
		$left  = 0xffffffff00000000;
		$right = 0x00000000ffffffff;

		$l = ($big & $left) >> 32;
		$r = $big & $right;

		return pack('NN', $l, $r);
	}

	/**
	 * Pack a 64bit integer as big endian long
	 *
	 * @param integer $big Big int
	 *
	 * @return integer
	 */
	static public function unpackLong64bigendian($bytes) {
		$set = unpack('N2', $bytes);
		return $original = ($set[1] & 0xFFFFFFFF) << 32 | ($set[2] & 0xFFFFFFFF);
	}
}
