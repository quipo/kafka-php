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
class Kafka_Registry_Topic
{
	const TOPIC_PATH  = "/brokers/topics/%s";
	const BROKER_PATH = "/brokers/topics/%s/%d";

	/**
	 * @var Zookeeper
	 */
	private $zookeeper;

	/**
	 * Create a new Topic Reigstry.
	 *
	 * @param Zookeeper $zookeeper the Zookeeper instance to back this TopicRegistry with.
	 */
	public function __construct(Zookeeper $zookeeper)
	{
		$this->zookeeper = $zookeeper;
	}

	/**
	 * Get the partitions on a particular broker for a specific topic.
	 *
	 * @param string $topic  the topic to get the partitions of
	 * @param int    $broker the broker to get the partitions from
	 *
	 * @return int the number of the topics partitions on the broker
	 */
	public function partitionsForBroker($topic, $broker)
	{
		$data = sprintf(self::BROKER_PATH, $topic, (int) $broker);
		$result = $this->zookeeper->get($data);
		return empty($result) ? 0 : (int) $result;
	}

	/**
	 * Get the partitions for a particular topic, grouped by broker.
	 *
	 * @param string $topic the topic to get the partitions of
	 *
	 * @return array the partitions as a map of broker to number of partitions (int -> int)
	 */
	public function partitions($topic)
	{
		$results = array();
		foreach ($this->brokers($topic) as $broker) {
			$results[$broker] = $this->partitionsForBroker($topic, $broker);
		}
		return $results;
	}

	/**
	 * Get the currently active brokers participating in a particular topic.
	 *
	 * @param string $topic the topic to get the brokers for
	 *
	 * @return array an array of brokers (int) that are participating in the topic
	 */
	public function brokers($topic)
	{
		$topicPath = sprintf(self::TOPIC_PATH, $topic);
		if (!@$this->zookeeper->exists($topicPath)) {
			if ($this->zookeeper->getState() != Zookeeper::CONNECTED_STATE) {
				$msg = 'Cannot connect to Zookeeper to fetch brokers for topic ' . $topic;
				throw new Kafka_Exception_ZookeeperConnection($msg);
			}
			return array();
		}
		$children  = $this->zookeeper->getChildren($topicPath);
		if (empty($children)) {
			return array();
		}

		$results = array();
		foreach ($children as $child) {
			$results[] = intval(str_replace($topicPath, '', $child));
		}
		return $results;
	}
}
