# grokking-kafka

Convincing my brain cells to form a quorum.

## Starting kafka - single host
- See: https://github.com/wurstmeister/kafka-docker and: https://github.com/wurstmeister/kafka-docker/wiki/Connectivity

```
docker-compose -f docker-compose-single-broker.yml up -d
```

### Stopping

```
docker-compose -f docker-compose-single-broker.yml stop
```

## Usage

lazy:
```
lein run
```

slightly less lazy:
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


