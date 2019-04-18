(ns grokking-kafka.core
  (:require [clojure.core.async :as async]
            [taoensso.nippy :as nippy])
  (:import [org.apache.kafka.common.serialization ByteArraySerializer ByteArrayDeserializer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.consumer KafkaConsumer])
  (:gen-class))

(def tsprintln-ch (async/chan))
(defn tsprintln-runner []
  (async/go-loop []
    (when-let [m (async/<! tsprintln-ch)]
      (println m)
      (recur))))

(defn tsprintln
  "A thread safe println."
  [& args]
  (async/go (async/>! tsprintln-ch (clojure.string/join " " args))))

;; song
(def songs ["Dido - Here With Me"
            "Madonna - Frozen"
            "Kate Bush - Running Up That Hill"
            "Placebo - Every You Every Me"
            "Phil Collins - In the Air Tonight"
            "Johnny Cash - Hurt"
            "The Verve - Bittersweet Symphony"])

(def users
  (->> [{:name "Dalibor Novak"
         :occupation "Wise old man"
         :favourite-food "Ice cream"}

        {:name "Jenny Davie"
         :occupation "Art teacher"
         :favourite-food "Spicy pig trotters"}

        {:name "Andrew Epps"
         :occupation "Biscuit monster"
         :favourite-food "Biscuits"}

        {:name "Jamie Jennings"
         :occupation "Computer Scientist"
         :favourite-food "Regular expressions"}

        {:name "Rosie Ivanova"
         :occupation "Zookeeper"
         :favourite-food "Knowledge"}]
       (map-indexed #(assoc %2 :id (inc %1)))
       (map #(assoc %2 :listening-to %1) (shuffle songs))))

(defn event-stream [ch events-per-second]
  (async/go-loop []
    (let [user-id (rand-int (inc (count users)))
          event {:user-id user-id
                 :event (first (shuffle ["view" "buy" "click"]))
                 :event-time (.toString (java.time.Instant/now))}]
      (async/>! ch event))
    (Thread/sleep (/ 1000 events-per-second))
    (recur)))

(defn user-updates [ch events-per-second]
  (async/go-loop []
    (let [user (first (shuffle users))
          updated-user (assoc user :listening-to (first (shuffle songs)))]
      (async/>! ch updated-user))
    (Thread/sleep (/ 1000 events-per-second))
    (recur)))


(def producer-cfg {"value.serializer" ByteArraySerializer
                   "key.serializer" ByteArraySerializer
                   "bootstrap.servers" "localhost:9092"})

(def producer (KafkaProducer. producer-cfg))

(def event-consumer-cfg
  {"bootstrap.servers" "localhost:9092"
   "group.id" "event-stream-consumer"
   "auto.offset.reset" "earliest"
   "enable.auto.commit" "false"
   "key.deserializer" ByteArrayDeserializer
   "value.deserializer" ByteArrayDeserializer})

(def user-consumer-cfg
  {"bootstrap.servers" "localhost:9092"
   "group.id" "user-stream-consumer"
   "auto.offset.reset" "earliest"
   "enable.auto.commit" "false"
   "key.deserializer" ByteArrayDeserializer
   "value.deserializer" ByteArrayDeserializer})

(def event-consumer (doto (KafkaConsumer. event-consumer-cfg)
                      (.subscribe ["event-stream"])))

(def user-consumer (doto (KafkaConsumer. user-consumer-cfg)
                      (.subscribe ["user-profile"])))

(defn process-event-record [record]
  (let [m (-> record
              (.value)
              nippy/thaw)]
    (tsprintln "Event record seen: " m)))

(defn process-user-record [record]
  (let [m (-> record
              (.value)
              nippy/thaw)]
    (tsprintln "User record seen:" m)))

(defn run []
  (let [event-ch (async/chan)
        user-ch (async/chan)]
    (event-stream event-ch 1)
    (user-updates user-ch 0.5)

    (async/go-loop []
      (when-let [event (async/<! event-ch)]
        (.send producer (ProducerRecord. "event-stream" (nippy/freeze event)))
        (recur)))

    (async/go-loop []
      (when-let [user-profile (async/<! user-ch)]
        (.send producer (ProducerRecord. "user-profile" (nippy/freeze user-profile)))
        (recur)))

    (async/thread
      (while true
        (let [records (.poll event-consumer 10)]
          (doseq [record records]
            (process-event-record record)
            ;; Not good for throughput
            (.commitSync event-consumer)))))

    (async/thread
      (while true
        (let [records (.poll user-consumer 5)]
          (doseq [record records]
            (process-user-record record)
            ;; Not good for throughput
            (.commitSync user-consumer))))))

  (tsprintln-runner)

  (async/chan))

(defn -main
  "The Trial"
  [& args]
  (assert  (>= (count songs) (count users)))
  ;; Don't stop process
  (async/<!! (run)))
