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
 * A message. The format of an N byte message is the following:
 * 1 byte "magic" identifier to allow format changes
 * 1 byte compression-attribute (missing if magic=0)
 * 4 byte CRC32 of the payload
 * N - 6 byte payload (N-5 if magic=0)
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_Message
{
	/**
	 * Wire format (0=without compression attribute, 1=with)
	 * @var integer
	 */
	private $magic = Kafka_Encoder::CURRENT_MAGIC_VALUE;

	/**
	 * @var string
	 */
	private $payload = null;
	
	/**
	 * @var integer
	 */
	private $size = 0;
	
	/**
	 * @var integer
	 */
	private $compression = Kafka_Encoder::COMPRESSION_NONE;
	
	/**
	 * @var string
	 */
	private $crc = false;
	
	/**
	 * Constructor
	 * 
	 * @param string $data Message payload
	 */
	public function __construct($data) {
		$this->magic = array_shift(unpack('C', substr($data, 0, 1)));
		if ($this->magic == 0) {
			$this->crc         = array_shift(unpack('N', substr($data, 1, 4)));
			$this->payload     = substr($data, 5);
		} else {
			$this->compression = array_shift(unpack('C', substr($data, 1, 1)));
			$this->crc         = array_shift(unpack('N', substr($data, 2, 4)));
			$this->payload     = substr($data, 6);
		}
		$this->size  = strlen($this->payload);
	}

	
	/**
	 * Return the compression flag
	 * 
	 * @return integer
	 */
	public function compression() {
		return $this->compression;
	}

	
	/**
	 * Encode a message
	 * 
	 * @return string
	 */
	public function encode() {
		return Kafka_Encoder::encode_message($this->payload);
	}
	
	/**
	 * Get the message size
	 * 
	 * @return integer
	 */
	public function size() {
		return $this->size;
	}
  
	/**
	 * Get the magic value
	 * 
	 * @return integer
	 */
	public function magic() {
		return $this->magic;
	}
	
	/**
	 * Get the message checksum
	 * 
	 * @return integer
	 */
	public function checksum() {
		return $this->crc;
	}
	
	/**
	 * Get the message payload
	 * 
	 * @return string|Kafka_MessageSetInternalIterator
	 */
	public function payload() {
		return Kafka_Encoder::decompress($this->payload, $this->compression);
	}
	
	/**
	 * Verify the message against the checksum
	 * 
	 * @return boolean
	 */
	public function isValid() {
		return ($this->crc === crc32($this->payload));
	}
  
	/**
	 * Debug message
	 * 
	 * @return string
	 */
	public function __toString() {
		try {
			$payload = $this->payload();
		} catch (Exception $e) {
			$payload = 'ERROR decoding payload: ' . $e->getMessage();
		}
		if (!is_string($payload)) {
			$payload = 'COMPRESSED-CONTENT';
		}
		return 'message('
			. 'magic = ' . $this->magic 
			. ', compression = ' . $this->compression 
			. ', size = ' . $this->size() 
			. ', crc = ' . $this->crc 
			. ', valid = ' . ($this->isValid() ? 'true' : 'false') 
			. ', payload = ' . $payload 
			. ')';
	}
}
