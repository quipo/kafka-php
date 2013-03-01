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
 * A sequence of messages stored in a byte buffer
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_MessageSet implements Iterator
{
	/**
	 * @var Kafka_Socket
	 */
	protected $socket = null;

	/**
	 * @var integer
	 */
	protected $initialOffset = 0;

	/**
	 * @var integer
	 */
	protected $validByteCount = 0;
	
	/**
	 * @var boolean
	 */
	private $valid = false;
	
	/**
	 * @var Kafka_Message
	 */
	private $msg;

	/**
	 * @var Kafka_MessageSetInternalIterator
	 */
	private $internalIterator = null;
	
	/**
	 * Constructor
	 * 
	 * @param Kafka_Socket $socket        Stream resource
	 * @param integer      $initialOffset Initial offset
	 */
	public function __construct(Kafka_Socket $socket, $initialOffset = 0) {
		$this->socket        = $socket;
		$this->initialOffset = $initialOffset;
	}

	/**
	 * Read the size of the next message (4 bytes)
	 *
	 * @return integer Size of the response buffer in bytes
	 * @throws Kafka_Exception when size is <=0 or >= $maxSize
	 */
	protected function getMessageSize() {
		$data = $this->socket->read(4, true);
		$unpack = unpack('N', $data);
		$size = array_shift($unpack);
		if ($size <= 0) {
			throw new Kafka_Exception_OutOfRange($size . ' is not a valid message size');
		}
		// TODO check if $size is too large
		return $size;
	}

	/**
	 * Read the next message 
	 *
	 * @return string Message (raw)
	 * @throws Kafka_Exception when the message cannot be read from the stream buffer
	 */
	protected function getMessage() {
		try {
			$size = $this->getMessageSize();
			$msg = $this->socket->read($size, true);
		} catch (Kafka_Exception_Socket_EOF $e) {
			$size = isset($size) ? $size : 'enough';
			$logMsg = 'Cannot read ' . $size . ' bytes, the message is likely bigger than the buffer - original exception: ' . $e->getMessage();
			throw new Kafka_Exception_OutOfRange($logMsg);
		}
		$this->validByteCount += 4 + $size;
		return $msg;
	}
	
	/**
	 * Get message set size in bytes
	 * 
	 * @return integer
	 */
	public function validBytes() {
		return $this->validByteCount;
	}
	
	/**
	 * Get message set size in bytes
	 * 
	 * @return integer
	 */
	public function sizeInBytes() {
		return $this->validBytes();
	}
	
	/**
	 * next
	 * 
	 * @return void
	 */
	public function next() {
		if (null !== $this->internalIterator) {
			$this->internalIterator->next();
		 	if ($this->internalIterator->valid()) {
		 		return;
		 	}
		}
		$this->internalIterator = null;
		$this->preloadNextMessage();
	}
	
	/**
	 * valid
	 * 
	 * @return boolean
	 */
	public function valid() {
		return $this->valid;
	}
	
	/**
	 * key
	 * 
	 * @return integer
	 */
	public function key() {
		return $this->validByteCount; 
	}
	
	/**
	 * current
	 * 
	 * @return Kafka_Message 
	 */
	public function current() {
		if (null !== $this->internalIterator && $this->internalIterator->valid()) {
			return $this->internalIterator->current();
		}
		return $this->msg;
	}
	
	/**
	 * rewind - Cannot use fseek()
	 * 
	 * @return void
	 */
	public function rewind() {
		$this->internalIterator = null;
		$this->validByteCount = 0;
		$this->preloadNextMessage();
	}

	/**
	 * Preload the next message
	 * 
	 * @return void
	 */
	private function preloadNextMessage() {
		try {
			$this->msg = new Kafka_Message($this->getMessage());
			if ($this->msg->compression() != Kafka_Encoder::COMPRESSION_NONE) {
				$this->internalIterator = $this->msg->payload();
				$this->internalIterator->rewind();
				$this->msg = null;
			} else {
				$this->internalIterator = null;
			}
			$this->valid = TRUE;
		} catch (Kafka_Exception_OutOfRange $e) {
			$this->valid = FALSE;
		}
	}
}
