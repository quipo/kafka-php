# kafka-php
kafka-php allows you to produce messages to the [Apache Kafka](http://incubator.apache.org/kafka/) distributed publish/subscribe messaging service.

## Requirements

* Minimum PHP version: 5.3.3.
* Apache Kafka 0.6.x or 0.7.x.
* You need to have access to your Kafka instance and be able to connect through TCP. You can obtain a copy and instructions on how to setup kafka at https://github.com/kafka-dev/kafka
* The [PHP Zookeeper extension](https://github.com/andreiz/php-zookeeper) is required if you want to use the Zookeeper-based consumer.

## Installation
Add the lib directory to the PHP include_path and use an autoloader like the one in the examples directory (the code follows the PEAR/Zend one-class-per-file convention).

## Usage
The examples directory contains an example of a Producer and a simple Consumer, and an example of the Zookeeper-based Consumer.

Example Producer:

```php
$producer = new Kafka_Producer('localhost', 9092, Kafka_Encoder::COMPRESSION_NONE);
$messages = array('some', 'messages', 'here');
$topic = 'test';
$bytes = $producer->send($messages, $topic);
```

Example Consumer:

```php
$topic         = 'test';
$partition     = 0;
$offset        = 0;
$maxSize       = 1000000;
$socketTimeout = 5;

while (true) {
    $consumer = new Kafka_SimpleConsumer('localhost', 9092, $socketTimeout, $maxSize);
    $fetchRequest = new Kafka_FetchRequest($topic, $partition, $offset, $maxSize);
    $messages = $consumer->fetch($fetchRequest);
    foreach ($messages as $msg) {
        echo "\nMessage: " . $msg->payload();
    }
    //advance the offset after consuming each MessageSet
    $offset += $messages->validBytes();
    unset($fetchRequest);
}
```


## TODO

- support for Snappy compression

## Contact for questions

Lorenzo Alberton

l.alberton at(@) quipo.it

http://twitter.com/lorenzoalberton

http://alberton.info/
