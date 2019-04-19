(ns grokking-kafka.commands
  (:require [clojure.core.async :as async]
            [taoensso.nippy :as nippy])
  (:import [org.apache.kafka.common.serialization ByteArrayDeserializer]
           [org.apache.kafka.clients.consumer KafkaConsumer]
           [org.apache.kafka.streams StreamsBuilder KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.kstream Materialized Produced KeyValueMapper TimeWindows]
           [org.apache.kafka.common.serialization Serdes]))


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

(defn basic-stream-processing []
  (let [event-consumer-cfg {"bootstrap.servers" "localhost:9092"
                            "group.id" "basic-stream-event-count-consumer"
                            "auto.offset.reset" "earliest"
                            "enable.auto.commit" "false"
                            "key.deserializer" ByteArrayDeserializer
                            "value.deserializer" ByteArrayDeserializer}
        event-count-topic "event-count-by-user"
        count-consumer (doto (KafkaConsumer. event-consumer-cfg)
                         (.subscribe [event-count-topic]))

        builder (StreamsBuilder.)

        props (java.util.Properties.)
        _ (. props (put StreamsConfig/APPLICATION_ID_CONFIG "event-count-application"))
        _ (. props (put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"))
        _ (. props (put StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getClass (Serdes/ByteArray))))
        _ (. props (put StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getClass (Serdes/ByteArray))))

        events (. builder (stream "event-stream"))
        event-counts (.. events
                         (groupBy (reify KeyValueMapper
                                    (apply [_ k event]
                                      (:user-id event))))
                         (windowedBy (TimeWindows/of 5000))
                         (count (Materialized/as "event-count-store")))

        _ (.. event-counts
              toStream
              (to event-count-topic
                  (Produced/with (Serdes/ByteArray) (Serdes/ByteArray))))
        topology (.build builder)
        streams (KafkaStreams. topology props)]

    (println "\n\n" (.describe topology) "\n\n")
    (.start streams)

    (async/thread
      (while true
        (let [records (.poll count-consumer 10)]
          (doseq [record records]
            (print-record record)
            ;; Not good for throughput
            (.commitSync count-consumer)))))))


(def exports {"basic-pub-sub" basic-pub-sub
              "basic-stream-processing" basic-stream-processing})
