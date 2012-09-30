<?php
/**
 * Kafka Client
 *
 * @category  Libraries
 * @package   Kafka
 * @author    Nick Telford <nick.telford@datasift.com>
 * @copyright 2012 Nick Telford
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link      http://sna-projects.com/kafka/
 */

/**
 * A Registry for Kafka brokers and the partitions they manage.
 *
 * The primary use of this is a facade API on top of ZooKeeper, providing a
 * more friendly interface to some common operations.
 *
 * @category Libraries
 * @package  Kafka
 * @author   Nick Telford <nick.telford@datasift.com>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_Registry_Broker
{
	const BROKER_PATH = '/brokers/ids/%d';

	/**
     * Zookeeper client
     *
     * @var Zookeeper
     */
	private $zookeeper;

	/**
	 * Create a Broker Registry instance backed by the given Zookeeper quorum.
	 *
	 * @param Zookeeper a client for contacting the backing Zookeeper quorum
	 */
	public function __construct(Zookeeper $zookeeper) {
		$this->zookeeper = $zookeeper;
	}

	/**
	 * Get the hostname and port of a broker.
	 *
	 * @param int the id of the brother to get the address of
	 *
	 * @return string the hostname and port of the broker, separated by a colon: host:port
	 */
	public function address($broker) {
		$data = sprintf(self::BROKER_PATH, (int) $broker);
		$result = $this->zookeeper->get($data);

		if (empty($result)) {
			$result = null;
		} else {
			$parts = explode(":", $result);
			$result = $parts[1] . ':' . $parts[2];
		}

		return $result;
	}
}
