(ns grokking-kafka.commands
  (:require [clojure.core.async :as async]
            [taoensso.nippy :as nippy])
  (:import [org.apache.kafka.common.serialization ByteArrayDeserializer]
           [org.apache.kafka.clients.consumer KafkaConsumer]))


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


(def exports {"basic-pub-sub" basic-pub-sub})
