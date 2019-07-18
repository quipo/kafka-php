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
 * Class to read/write to a socket
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_Socket
{
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
	 * Stream resource
	 *
	 * @var resource
	 */
	private $stream = null;

	/**
	 * Socket host
	 *
	 * @var string
	 */
	private $host = null;

	/**
	 * Socket port
	 *
	 * @var integer
	 */
	private $port = -1;

	/**
	 * Constructor
	 *
	 * @param string  $host            Host
	 * @param integer $port            Port
	 * @param integer $recvTimeoutSec  Recv timeout in seconds
	 * @param integer $recvTimeoutUsec Recv timeout in microseconds
	 * @param integer $sendTimeoutSec  Send timeout in seconds
	 * @param integer $sendTimeoutUsec Send timeout in microseconds
	 */
	public function __construct($host, $port, $recvTimeoutSec = 0, $recvTimeoutUsec = 750000, $sendTimeoutSec = 0, $sendTimeoutUsec = 100000) {
		$this->host            = $host;
		$this->port            = (int)$port;
		$this->recvTimeoutSec  = $recvTimeoutSec;
		$this->recvTimeoutUsec = $recvTimeoutUsec;
		$this->sendTimeoutSec  = $sendTimeoutSec;
		$this->sendTimeoutUsec = $sendTimeoutUsec;
	}

	/**
	 * Optional method to set the internal stream handle
	 *
	 * @param resource $stream File handle
	 *
	 * @return void
	 */
	static public function createFromStream($stream) {
		$socket = new self('localhost', 0);
		$socket->setStream($stream);
		return $socket;
	}

	/**
	 * Optional method to set the internal stream handle
	 *
	 * @param resource $stream File handle
	 *
	 * @return void
	 */
	public function setStream($stream) {
		$this->stream = $stream;
	}

	/**
	 * Connects the socket
	 *
	 * @return void
	 * @throws Kafka_Exception_Socket_Connection
	 */
	public function connect() {
		if (is_resource($this->stream)) {
			return true;
		}

		if (empty($this->host)) {
			throw new Kafka_Exception_Socket_Connection('Cannot open null host');
		}
		if ($this->port <= 0) {
			throw new Kafka_Exception_Socket_Connection('Cannot open without port');
		}

		$this->stream = @fsockopen(
			$this->host,
			$this->port,
			$errno,
			$errstr,
			$this->sendTimeoutSec + ($this->sendTimeoutUsec / 1000000)
		);

		// Connect failed?
		if ($this->stream === FALSE) {
			$error = 'Could not connect to '.$this->host.':'.$this->port.' ('.$errstr.' ['.$errno.'])';
			throw new Kafka_Exception_Socket_Connection($error);
		}

		// Set to blocking mode when keeping a persistent connection,
		// otherwise leave it as non-blocking when polling
		@stream_set_blocking($this->stream, 0);
		//socket_set_option($this->stream, SOL_TCP, TCP_NODELAY, 1);
		//socket_set_option($this->stream, SOL_TCP, SO_KEEPALIVE, 1);
	}

	/**
	 * Closes the socket
	 *
	 * @return void
	 */
	public function close() {
		if (is_resource($this->stream)) {
			fclose($this->stream);
		}
	}

	/**
	 * Read from the socket at most $len bytes.
	 *
	 * This method will not wait for all the requested data, it will return as
	 * soon as any data is received.
	 *
	 * @param integer $len               Maximum number of bytes to read.
	 * @param boolean $verifyExactLength Throw an exception if the number of read bytes is less than $len
	 *
	 * @return string Binary data
	 * @throws Kafka_Exception_Socket
	 */
	public function read($len, $verifyExactLength = false) {
		$null = null;
		$read = array($this->stream);
		$readable = @stream_select($read, $null, $null, $this->recvTimeoutSec, $this->recvTimeoutUsec);
		if ($readable > 0) {
			$remainingBytes = $len;
			$data = $chunk = '';
			while ($remainingBytes > 0) {
				$chunk = fread($this->stream, $remainingBytes);
				if ($chunk === false) {
					$this->close();
					throw new Kafka_Exception_Socket_EOF('Could not read '.$len.' bytes from stream (no data)');
				}
				if (strlen($chunk) === 0) {
					// Zero bytes because of EOF?
					if (feof($this->stream)) {
						$this->close();
						throw new Kafka_Exception_Socket_EOF('Unexpected EOF while reading '.$len.' bytes from stream (no data)');
					}
					// Otherwise wait for bytes
					$readable = @stream_select($read, $null, $null, $this->recvTimeoutSec, $this->recvTimeoutUsec);
					if ($readable !== 1) {
						throw new Kafka_Exception_Socket_Timeout('Timed out reading socket while reading ' . $len . ' bytes with ' . $remainingBytes . ' bytes to go');
					}
					continue; // attempt another read
				}
				$data .= $chunk;
				$remainingBytes -= strlen($chunk);
			}
			if ($len === $remainingBytes || ($verifyExactLength && $len !== strlen($data))) {
				// couldn't read anything at all OR reached EOF sooner than expected
				$this->close();
				throw new Kafka_Exception_Socket_EOF('Read ' . strlen($data) . ' bytes instead of the requested ' . $len . ' bytes');
			}

			return $data;
		}
		if (false !== $readable) {
			$res = stream_get_meta_data($this->stream);
			if (!empty($res['timed_out'])) {
				$this->close();
				throw new Kafka_Exception_Socket_Timeout('Timed out reading '.$len.' bytes from stream');
			}
		}
		$this->close();
		throw new Kafka_Exception_Socket_EOF('Could not read '.$len.' bytes from stream (not readable)');
	}

	/**
	 * Write to the socket.
	 *
	 * @param string $buf The data to write
	 *
	 * @return integer
	 * @throws Kafka_Exception_Socket
	 */
	public function write($buf) {
		$null = null;
		$write = array($this->stream);

		// fwrite to a socket may be partial, so loop until we
		// are done with the entire buffer
		$written = 0;
		$buflen = strlen($buf);
		while ( $written < $buflen ) {
			// wait for stream to become available for writing
			$writable = @stream_select($null, $write, $null, $this->sendTimeoutSec, $this->sendTimeoutUsec);
			if ($writable > 0) {
				// Set a temporary error handler to watch for Broken pipes
				set_error_handler(function ($type, $msg, $file, $line) use ($buflen, &$written) {
					if ($type === E_NOTICE && strpos($msg, 'Broken pipe') !== false) {
						throw new \Kafka_Exception_Socket(
							sprintf('Connection broken while writing %d bytes to stream, completed writing only %d bytes', $buflen, $written)
						);
					}
				});
				try {
					// write remaining buffer bytes to stream
					$wrote = fwrite($this->stream, substr($buf, $written));
				} finally {
					restore_error_handler();
				}
				if ($wrote === -1 || $wrote === false) {
					throw new Kafka_Exception_Socket('Could not write ' . $buflen . ' bytes to stream, completed writing only ' . $written . ' bytes');
				}
				$written += $wrote;
				continue;
			}
			if (false !== $writable) {
				$res = stream_get_meta_data($this->stream);
				if (!empty($res['timed_out'])) {
					throw new Kafka_Exception_Socket_Timeout('Timed out writing ' . strlen($buf) . ' bytes to stream after writing ' . $written . ' bytes');
				}
			}
			throw new Kafka_Exception_Socket('Could not write ' . strlen($buf) . ' bytes to stream');
		}
		return $written;
	}

	/**
	 * Rewind the stream
	 *
	 * @return void
	 */
	public function rewind() {
		if (is_resource($this->stream)) {
			rewind($this->stream);
		}
	}
}
