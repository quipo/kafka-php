# kafka-php
kafka-php allows you to produce messages to the [Apache Kafka](http://incubator.apache.org/kafka/) distributed publish/subscribe messaging service.

## Requirements
Minimum PHP version: 5.3.3.
You need to have access to your Kafka instance and be able to connect through TCP. You can obtain a copy and instructions on how to setup kafka at https://github.com/kafka-dev/kafka
The [PHP Zookeeper extension](https://github.com/andreiz/php-zookeeper) is required if you want to use the Zookeeper-based consumer.

## Installation
Add the lib directory to the include_path and use an autoloader like the one in the examples directory (the code follows the PEAR/Zend one-class-per-file convention).

## Usage
The examples directory contains an example of a Producer and a simple Consumer, and an example of the Zookeeper-based Consumer.

## TODO

- support for Snappy compression

## Contact for questions

Lorenzo Alberton

l.alberton at(@) quipo.it

http://twitter.com/lorenzoalberton

http://alberton.info/
