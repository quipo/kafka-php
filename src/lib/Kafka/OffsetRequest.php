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
 * Represents a request object
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_OffsetRequest extends Kafka_Request
{
	/**
	 * @var integer
	 */
	private $time;
	
	/**
	 * @var integer
	 */
	private $maxSize;
	
	/**
	 * @param string  $topic         Topic
	 * @param integer $partition     Partition
	 * @param integer $time          Time in millisecs 
	 *                               (-1, from the latest offset available, 
	 *                                -2 from the smallest offset available)
	 * @param integer $maxNumOffsets Max number of offsets to return
	 */
	public function __construct($topic, $partition, $time, $maxNumOffsets) {
		$this->id            = Kafka_RequestKeys::OFFSETS;
		$this->topic         = $topic;
		$this->partition     = $partition;
		$this->time          = $time;
		$this->maxNumOffsets = $maxNumOffsets;
	}
	
	/**
	 * Write the request to the output stream
	 * 
	 * @param Kafka_Socket $socket Output stream
	 * 
	 * @return void
	 */
	public function writeTo(Kafka_Socket $socket) {
		$this->writeRequestHeader($socket);
		
		// TIMESTAMP (long)
		$socket->write(self::packLong64bigendian($this->time));
		// N_OFFSETS (int)
		$socket->write(pack('N', $this->maxNumOffsets));
	}
	
	/**
	 * Get request size in bytes
	 * 
	 * @return integer
	 */
	public function sizeInBytes() {
		return 2 + strlen($this->topic) + 4 + 8 + 4;
	}
	
	/**
	 * Get time
	 *
	 * @return integer
	 */
	public function getTime() {
		return $this->time;
	}
	
	/**
	 * Get maxSize
	 * 
	 * @return integer
	 */
	public function getMaxSize() {
		return $this->maxSize;
	}
	
	/**
	 * Get partition
	 * 
	 * @return integer
	 */
	public function getPartition() {
		return $this->partition;
	}

	/**
	 * Parse the response and return the array of offsets
	 *
	 * @param Kafka_Socket $socket Socket handle
	 *
	 * @return array
	 */
	static public function deserializeOffsetArray(Kafka_Socket $socket) {
		$nOffsets = array_shift(unpack('N', $socket->read(4)));
		if ($nOffsets < 0) {
			throw new Kafka_Exception_OutOfRange($nOffsets . ' is not a valid number of offsets');
		}
		$offsets = array();
		for ($i=0; $i < $nOffsets; ++$i) {
			$offsets[] = self::unpackLong64bigendian($socket->read(8));
		}
		return $offsets;
	}

	/**
	 * String representation of the Fetch Request
	 * 
	 * @return string
	 */
	public function __toString() {
		return 'topic:' . $this->topic . ', part:' . $this->partition . ' offset:' . $this->offset . ' maxSize:' . $this->maxSize;
	}
}
