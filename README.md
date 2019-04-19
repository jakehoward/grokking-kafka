# grokking-kafka

Convincing my brain cells to form a quorum.

## Starting Kafka

This is from https://kafka.apache.org/quickstart, ideally we could spin up a docker container but for some reason the networking wasn't playing ball. Will come back to that (https://github.com/wurstmeister/kafka-docker, https://github.com/wurstmeister/kafka-docker/wiki/Connectivity).

1. Download Kafka from: https://kafka.apache.org/downloads
1. Download the sha and check it, e.g: `gpg --print-md SHA512 ~/Downloads/kafka_2.12-2.2.0.tgz`
1. Unzip it: `tar -xf kafka_2.12-2.2.0.tgz`
1. Open two tabs, in one start zookeeper: `bin/zookeeper-server-start.sh config/zookeeper.properties`
1. In the other start kafka: `bin/kafka-server-start.sh config/server.properties`

## Usage

Run with lein:
```
lein run
```

Build uberjar and run it:
```
lein uberjar
java -jar target/uberjar/grokking-kafka-0.1.0-SNAPSHOT-standalone.jar
```

## Guides/Docs/Resources
- https://kafka.apache.org/22/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
- https://kafka.apache.org/22/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
- https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/2.2.0
- https://github.com/ptaoussanis/nippy
- https://kafka.apache.org/
- https://docs.docker.com/engine/tutorials/networkingcontainers/
- https://the-frey.github.io/2018/04/13/kafka-producers-in-clojure
- https://stackoverflow.com/questions/35788697/leader-not-available-kafka-in-console-producer
- https://github.com/wurstmeister/kafka-docker/issues/169


