(defproject grokking-kafka "0.1.0-SNAPSHOT"
  :description "It's like reading someone's dream"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "0.4.490"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.slf4j/slf4j-simple "1.7.26"]
                 [org.apache.kafka/kafka-clients "2.2.0"]
                 [org.apache.kafka/kafka-streams "2.2.0"]]
  :main ^:skip-aot grokking-kafka.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
