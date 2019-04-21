(ns grokking-kafka.commands
  (:require [clojure.core.async :as async]
            [taoensso.nippy :as nippy])
  (:import [org.apache.kafka.common.serialization ByteArrayDeserializer LongDeserializer StringDeserializer]
           [org.apache.kafka.clients.consumer KafkaConsumer]
           [org.apache.kafka.streams StreamsBuilder KafkaStreams StreamsConfig KeyValue]
           [org.apache.kafka.streams.kstream Materialized Produced KeyValueMapper ValueMapper TimeWindows]
           [org.apache.kafka.common.serialization Serdes]
           [java.time Duration]))


(def tsprintln-ch (async/chan))

(async/go-loop []
    (when-let [m (async/<! tsprintln-ch)]
      (println m)
      (recur)))

(defn tsprintln
  "A thread safe println."
  [& args]
  (async/go (async/>! tsprintln-ch (clojure.string/join " " args))))

(defn print-record [record]
  (let [m (-> record
              (.value)
              nippy/thaw)]
    (tsprintln m)))

;; +++++++++++++++++++++++
;; Basic Publish Subscribe
;; +++++++++++++++++++++++
(defn basic-pub-sub []
  (let [event-consumer-cfg {"bootstrap.servers" "localhost:9092"
                            "group.id" "basic-pub-sub-event-stream-consumer"
                            "auto.offset.reset" "earliest"
                            "enable.auto.commit" "false"
                            "key.deserializer" ByteArrayDeserializer
                            "value.deserializer" ByteArrayDeserializer}
        user-consumer-cfg {"bootstrap.servers" "localhost:9092"
                           "group.id" "basic-pub-sub-user-stream-consumer"
                           "auto.offset.reset" "earliest"
                           "enable.auto.commit" "false"
                           "key.deserializer" ByteArrayDeserializer
                           "value.deserializer" ByteArrayDeserializer}
        event-consumer (doto (KafkaConsumer. event-consumer-cfg)
                         (.subscribe ["event-stream"]))
        user-consumer (doto (KafkaConsumer. user-consumer-cfg)
                        (.subscribe ["user-profile"]))]

    (async/thread
      (while true
        (let [records (.poll event-consumer 10)]
          (doseq [record records]
            (print-record record)
            ;; Not good for throughput
            (.commitSync event-consumer)))))

    (async/thread
      (while true
        (let [records (.poll user-consumer 5)]
          (doseq [record records]
            (print-record record)
            ;; Not good for throughput
            (.commitSync user-consumer)))))))


;; +++++++++++++++++++++++
;; Stream Processing
;; +++++++++++++++++++++++
(defn basic-stream-pipe []
  (let [consumer-cfg {"bootstrap.servers" "localhost:9092"
                      "group.id" "basic-stream-pipe-consumer"
                      "auto.offset.reset" "earliest"
                      "enable.auto.commit" "false"
                      "key.deserializer" ByteArrayDeserializer
                      "value.deserializer" ByteArrayDeserializer}
        output-topic "basic-stream-pipe-output"
        consumer (doto (KafkaConsumer. consumer-cfg)
                   (.subscribe [output-topic]))

        props (java.util.Properties.)
        _ (. props (put StreamsConfig/APPLICATION_ID_CONFIG "basic-stream-pipe"))
        _ (. props (put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"))
        _ (. props (put StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getClass (Serdes/ByteArray))))
        _ (. props (put StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getClass (Serdes/ByteArray))))

        builder (StreamsBuilder.)
        source (. builder (stream "event-stream"))
        _ (. source (to output-topic))

        topology (.build builder)
        streams (KafkaStreams. topology props)]
    (.cleanUp streams)
    (.start streams)

    (println "\n\n" (.describe topology) "\n\n")

    (async/thread
      (while true
        (let [records (.poll consumer 10)]
          (doseq [record records]
            (print-record record)
            ;; Not good for throughput
            (.commitSync consumer)))))))

(defn basic-stream-map []
  (let [consumer-cfg {"bootstrap.servers" "localhost:9092"
                      "group.id" "basic-stream-map-consumer"
                      "auto.offset.reset" "earliest"
                      "enable.auto.commit" "false"
                      "key.deserializer" ByteArrayDeserializer
                      "value.deserializer" ByteArrayDeserializer}
        output-topic "basic-stream-map-output-4"
        consumer (doto (KafkaConsumer. consumer-cfg)
                   (.subscribe [output-topic]))

        props (java.util.Properties.)
        _ (. props (put StreamsConfig/APPLICATION_ID_CONFIG "basic-stream-map"))
        _ (. props (put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"))
        _ (. props (put StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getClass (Serdes/ByteArray))))
        _ (. props (put StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getClass (Serdes/ByteArray))))

        builder (StreamsBuilder.)
        source (. builder (stream "event-stream"))
        sink (. source (mapValues (reify ValueMapper
                                    (apply [_ event]
                                      (nippy/freeze (:event (nippy/thaw event)))))))
        _ (. sink (to output-topic))

        topology (.build builder)
        streams (KafkaStreams. topology props)]
    (.cleanUp streams)
    (.start streams)

    (println "\n\n" (.describe topology) "\n\n")

    (async/thread
      (while true
        (let [records (.poll consumer 10)]
          (doseq [record records]
            (print-record record)
            ;; Not good for throughput
            (.commitSync consumer)))))))


(defn basic-stream-count []
  (let [consumer-cfg {"bootstrap.servers" "localhost:9092"
                      "group.id" "basic-stream-count-consumer"
                      "auto.offset.reset" "earliest"
                      "enable.auto.commit" "false"
                      "key.deserializer" StringDeserializer
                      "value.deserializer" LongDeserializer}
        output-topic "basic-stream-count-output"
        consumer (doto (KafkaConsumer. consumer-cfg)
                   (.subscribe [output-topic]))

        props (java.util.Properties.)
        _ (. props (put StreamsConfig/APPLICATION_ID_CONFIG "basic-stream-count"))
        _ (. props (put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"))
        _ (. props (put StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getClass (Serdes/ByteArray))))
        _ (. props (put StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getClass (Serdes/ByteArray))))

        builder (StreamsBuilder.)
        source (. builder (stream "event-stream"))
        sink (.. source
                 (groupBy (reify KeyValueMapper
                            (apply [_ k event]
                              (nippy/freeze (:user-id (nippy/thaw event))))))
                 (windowedBy (TimeWindows/of (Duration/ofSeconds 10)))
                 (count (Materialized/as "event-count-store-1"))
                 toStream
                 (map (reify KeyValueMapper
                        (apply [_ k count]
                          (KeyValue. (str (nippy/thaw (.key k)) "@" (.. k window start) "->" (.. k window end))
                                     count)))))

        _ (. sink (to output-topic (Produced/with (Serdes/String) (Serdes/Long))))

        topology (.build builder)
        streams (KafkaStreams. topology props)]
    (.cleanUp streams)
    (.start streams)

    (println "\n\n" (.describe topology) "\n\n")

    (async/thread
      (while true
        (let [records (.poll consumer 10)]
          (doseq [record records]
            ;; (print-record record)
            (println "Record:" record)
            ;; Not good for throughput
            (.commitSync consumer)))))))

(def exports {"basic-pub-sub" basic-pub-sub
              "basic-stream-pipe" basic-stream-pipe
              "basic-stream-map" basic-stream-map
              "basic-stream-count" basic-stream-count})
