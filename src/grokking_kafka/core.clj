(ns grokking-kafka.core
  (:require [clojure.core.async :as async]
            [taoensso.nippy :as nippy]
            [grokking-kafka.commands :as commands])
  (:import [org.apache.kafka.common.serialization ByteArraySerializer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:gen-class))


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

    (async/chan)))

(defn -main
  "The Trial"
  [& args]
  (assert  (>= (count songs) (count users)))
  (let [command-name (first args)]
    (if (some #(= % command-name) (keys commands/exports))
      (do (println "Running command:" command-name)
          ;; Apply the function
          ((get commands/exports command-name)))
      (do (println command-name "is not a valid command name.")
          (println "Choose one of:" (clojure.string/join ", " (keys commands/exports)))
          (println "For example: lein run" (first (keys commands/exports))))))
  ;; Don't stop process
  (async/<!! (run)))
