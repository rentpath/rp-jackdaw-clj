(defproject com.rentpath/rp-jackdaw-clj "0.2.2-SNAPSHOT"
  :description "Clojure Kafka components using Jackdaw"
  :url "https://gitthub.com/rentpath/rp-jackdaw-clj"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [com.stuartsierra/component "0.4.0"]
                 [fundingcircle/jackdaw "0.7.4"
                  :exclusions [org.apache.kafka/kafka-clients
                               org.apache.kafka/kafka-streams
                               io.confluent/kafka-avro-serializer
                               io.confluent/kafka-schema-registry-client]]
                 [org.apache.kafka/kafka-clients "2.5.0"]
                 [org.apache.kafka/kafka-streams "2.5.0"]
                 [org.apache.kafka/kafka-streams-test-utils "2.5.0"]
                 [io.confluent/kafka-schema-registry-client "5.5.0"]
                 [io.confluent/kafka-avro-serializer "5.5.0"]
                 [cheshire "5.8.0"]
                 [us.bpsm/edn-java "0.6.0"]
                 [com.fasterxml.jackson.core/jackson-core "2.11.0"]]
  :scm {:url "git@github.com:rentpath/rp-jackdaw-clj.git"}
  :repositories {"confluent" {:url "https://packages.confluent.io/maven/"}}
  :deploy-repositories [["releases" {:url "https://clojars.org/repo/"
                                     :username [:gpg :env/CLOJARS_USERNAME]
                                     :password [:gpg :env/CLOJARS_PASSWORD]
                                     :sign-releases false}]]
  :aot [rp.jackdaw.serdes.homogeneous-edn
        rp.jackdaw.serdes.key-serde
        rp.jackdaw.serdes.value-serde])
