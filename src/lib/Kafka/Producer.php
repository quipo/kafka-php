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
 * Simple Kafka Producer
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_Producer
{
	/**
	 * @var integer
	 */
	protected $request_key;

	/**
	 * @var Kafka_Socket
	 */
	protected $socket;
	
	/**
	 * @var string
	 */
	protected $host;
	
	/**
	 * @var integer
	 */
	protected $port;

	/**
	 * Compression: 0=none; 1=gzip; 2=snappy
	 *
	 * @var integer
	 */
	protected $compression;

	/**
	 * Send timeout in seconds.
	 *
	 * Combined with sendTimeoutUsec this is used for send timeouts.
	 *
	 * @var int
	 */
	private $sendTimeoutSec = 0;

	/**
	 * Send timeout in microseconds.
	 *
	 * Combined with sendTimeoutSec this is used for send timeouts.
	 *
	 * @var int
	 */
	private $sendTimeoutUsec = 100000;

	/**
	 * Recv timeout in seconds	
	 *
	 * Combined with recvTimeoutUsec this is used for recv timeouts.
	 *
	 * @var int
	 */
	private $recvTimeoutSec = 0;

	/**
	 * Recv timeout in microseconds
	 *
	 * Combined with recvTimeoutSec this is used for recv timeouts.
	 *
	 * @var int
	*/
	private $recvTimeoutUsec = 750000;

	/**
	 * Constructor
	 * 
	 * @param integer $host Host 
	 * @param integer $port Port
	 */
	public function __construct($host, $port, $compression = Kafka_Encoder::COMPRESSION_GZIP, $recvTimeoutSec = 0, $recvTimeoutUsec = 750000, $sendTimeoutSec = 0, $sendTimeoutUsec = 100000) {
		$this->request_key = Kafka_RequestKeys::PRODUCE;
		$this->host        = $host;
		$this->port        = $port;
		$this->compression = $compression;
		$this->recvTimeoutSec  = $recvTimeoutSec;
		$this->recvTimeoutUsec = $recvTimeoutUsec;
		$this->sendTimeoutSec  = $sendTimeoutSec;
		$this->sendTimeoutUsec = $sendTimeoutUsec;
	}
	
	/**
	 * Connect to Kafka via a socket
	 * 
	 * @return void
	 * @throws Kafka_Exception
	 */
	public function connect() {
		if (null === $this->socket) {
			$this->socket = new Kafka_Socket($this->host, $this->port, $this->recvTimeoutSec, $this->recvTimeoutUsec, $this->sendTimeoutSec, $this->sendTimeoutUsec);
		}
		$this->socket->connect();
	}

	/**
	 * Close the socket
	 * 
	 * @return void
	 */
	public function close() {
		if (null !== $this->socket) {
			$this->socket->close();
		}
	}

	/**
	 * Send messages to Kafka
	 * 
	 * @param array   $messages  Messages to send
	 * @param string  $topic     Topic
	 * @param integer $partition Partition
	 *
	 * @return boolean
	 */
	public function send(array $messages, $topic, $partition = 0xFFFFFFFF) {
		$this->connect();
		return $this->socket->write(Kafka_Encoder::encode_produce_request($topic, $partition, $messages, $this->compression));
	}

	/**
	 * When serializing, close the socket and save the connection parameters
	 * so it can connect again
	 * 
	 * @return array Properties to save
	 */
	public function __sleep() {
		$this->close();
		return array('request_key', 'host', 'port', 'compression');
	}

	/**
	 * Restore parameters on unserialize
	 * 
	 * @return void
	 */
	public function __wakeup() {
		
	}
}
