(defproject grokking-kafka "0.1.0-SNAPSHOT"
  :description "It's like reading someone's dream"
  :dependencies [[org.clojure/clojure "1.10.0"]]
  :main ^:skip-aot grokking-kafka.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
