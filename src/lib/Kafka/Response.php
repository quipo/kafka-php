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
 * Response class
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_Response
{
	/**
	 * Validate the error code from the response
	 * 
	 * @param integer $errorCode Error code
	 *
	 * @return void
	 * @throws Kafka_Exception
	 */
	static public function validateErrorCode($errorCode) {
		switch ($errorCode) {
			case 0:  break; //success
			case 1:  throw new Kafka_Exception_OffsetOutOfRange('OffsetOutOfRange reading response errorCode');
			case 2:  throw new Kafka_Exception_InvalidMessage('InvalidMessage reading response errorCode');
			case 3:  throw new Kafka_Exception_WrongPartition('WrongPartition reading response errorCode');
			case 4:  throw new Kafka_Exception_InvalidFetchSize('InvalidFetchSize reading response errorCode');
			default: throw new Kafka_Exception('Unknown error reading response errorCode (' . $errorCode . ')');
		}
	}
}
