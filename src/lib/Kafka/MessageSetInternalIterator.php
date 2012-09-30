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
class Kafka_MessageSetInternalIterator extends Kafka_MessageSet
{
	/**
	 * Read the next message. 
	 * Override the parent method: we don't want to increment the byte offset 
	 *
	 * @return string Message (raw)
	 * @throws Kafka_Exception when the message cannot be read from the stream buffer
	 */
	protected function getMessage() {
		$msg = parent::getMessage();
		// do not increment the offset for internal iterators
		$this->validByteCount = 0;
		return $msg;
	}
}
