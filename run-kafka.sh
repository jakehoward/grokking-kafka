#!/usr/bin/env bash

NET_ID="$(docker network list | grep 'kafka' | cut -d ' ' -f 1)"
if [ ! -z "$NET_ID" ]; then
    echo "Kafka net already exists, deleting: $NET_ID..."
    docker network rm "$NET_ID"
fi

echo "Creating new kafka network..."
docker network create kafka

ZOOKEEPER_ID="$(docker ps -a | grep 'zookeeper' | cut -d ' ' -f 1)"
if [ ! -z "$ZOOKEEPER_ID" ]; then
    echo "Zookeper already running, stopping..."
    docker stop zookeeper; docker rm zookeeper;
fi

KAFKA_ID="$(docker ps -a | grep 'kafka')"
if [ ! -z "$KAFKA_ID" ]; then
    echo "Kafka already running, stopping..."
    docker stop kafka; docker rm kafka;
fi

echo "Running zookeeper..."
docker run --network=kafka -d --name=zookeeper -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper:5.2.1

echo "Running kafka..."
docker run --network=kafka -d -p 9092:9092 --name=kafka -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka:5.2.1
