<?php
/**
 * Kafka Client
 *
 * @category  Libraries
 * @package   Kafka
 * @author    Lorenzo Alberton <l.alberton@quipo.it>
 * @copyright 2012 Lorenzo Alberton
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link      http://sna-projects.com/kafka/
 */

/**
 * A Registry for Kafka Consumer offsets.
 *
 * The primary use of this is a facade API on top of ZooKeeper, providing a
 * more friendly interface to some common operations.
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_Registry_Offset
{
	const OFFSETS_PATH = '/consumers/%s/offsets/%s';
	const OFFSET_PATH  = '/consumers/%s/offsets/%s/%d-%d';

	/**
	 * @var Zookeeper
	 */
	private $zookeeper;

	/**
	 * @var string
	 */
	private $group;

	/**
	 * Create a new Offset Registry for the given group.
	 *
	 * @param Zookeeper $zookeeper a Zookeeper instance to back this OffsetRegistry
	 * @param string    $group     the consumer group to create this OffsetRegistry for
	 */
	public function __construct(Zookeeper $zookeeper, $group) {
		$this->zookeeper = $zookeeper;
		$this->group = (string) $group;
	}

	/**
	 * Commits the given offset for the given partition
	 *
	 * @param string $topic     the topic the partition belongs to
	 * @param int    $broker    the broker holding the partition
	 * @param int    $partition the partition on the broker
	 * @param int    $offset    the offset to commit
	 */
	public function commit($topic, $broker, $partition, $offset) {
		$path = sprintf(self::OFFSET_PATH, $this->group, $topic, (int) $broker, (int) $partition);
		if (!$this->zookeeper->exists($path)) {
			$this->makeZkPath($path);
			$this->makeZkNode($path, (int)$offset);
		} else {
			$this->zookeeper->set($path, (int) $offset);
		}
	}

	/**
	 * Equivalent of "mkdir -p" on ZooKeeper
	 *
	 * @param string $path  The path to the node
	 * @param mixed  $value The value to assign to each new node along the path
	 *
	 * @return bool
	 */
	protected function makeZkPath($path, $value = 0) {
		$parts = explode('/', $path);
		$parts = array_filter($parts);
		$subpath = '';
		while (count($parts) > 1) {
			$subpath .= '/' . array_shift($parts);
			if (!$this->zookeeper->exists($subpath)) {
				$this->makeZkNode($subpath, $value);
			}
		}
	}

	/**
	 * Create a node on ZooKeeper at the given path
	 *
	 * @param string $path  The path to the node
	 * @param mixed  $value The value to assign to the new node
	 *
	 * @return bool
	 */
	protected function makeZkNode($path, $value) {
		$params = array(
			array(
				'perms'  => Zookeeper::PERM_ALL,
				'scheme' => 'world',
				'id'     => 'anyone',
			)
		);
		return $this->zookeeper->create($path, $value, $params);
	}

	/**
	 * Get the current offset for the specified partition of a topic on a broker.
	 *
	 * @param string $topic     the topic the partition belongs to
	 * @param int    $broker    the broker holding the partition
	 * @param int    $partition the partition on the broker
	 *
	 * @return int the byte offset for the cursor in the partition
	 */
	public function offset($topic, $broker, $partition) {
		$path = sprintf(self::OFFSET_PATH, $this->group, $topic, (int) $broker, (int) $partition);
		if (!$this->zookeeper->exists($path)) {
			return 0;
		}

		$result = $this->zookeeper->get($path);
		return empty($result) ? 0 : $result;
	}

	/**
	 * Gets the current offsets for all partitions of a topic.
	 *
	 * @param string $topic the topic to get the offsets for
	 *
	 * @return array a map of partition (broker + partition ID) to the byte offset offset (int).
	 */
	public function offsets($topic) { //: Map[(Int, Int), Long] == Map[(Broker, Partition), Offset]
		$offsets = array();
		foreach ($this->partitions($topic) as $broker => $partition) {
			if (!isset($offsets[$broker])) {
				$offsets[$broker] = array();
			}

			$offsets[$broker][$partition] = $this->offset($topic, $broker, $partition);
		}
		return $offsets;
	}

	/**
	 * Gets all the partitions for a given topic.
	 *
	 * @param string $topic the topic to get the partitions for
	 *
	 * @return array an associative array of the broker (int) to the number of partitions (int)
	 */
	public function partitions($topic) {
		$offsetsPath = sprintf(self::OFFSETS_PATH, $this->group, $topic);
		if (!$this->zookeeper->exists($offsetsPath)) {
			return array();
		}

		$children = $this->zookeeper->getChildren($offsetsPath);
		$partitions = array();
		foreach ($children as $child) {
			list($broker, $partition) = explode('-', str_replace($offsetsPath, '', $child), 2);
			$partitions[intval($broker)] = intval($partition);
		}
		return $partitions;
	}
}
