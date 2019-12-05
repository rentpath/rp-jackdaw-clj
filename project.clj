(defproject com.rentpath/rp-jackdaw-clj "0.1.7-SNAPSHOT"
  :description "Clojure Kafka components using Jackdaw"
  :url "https://gitthub.com/rentpath/rp-jackdaw-clj"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [com.stuartsierra/component "0.4.0"]
                 [fundingcircle/jackdaw "0.6.6"]
                 [org.apache.kafka/kafka-streams-test-utils "2.2.0"]
                 [cheshire "5.8.0"]
                 [us.bpsm/edn-java "0.6.0"]]
  :scm {:url "git@github.com:rentpath/rp-jackdaw-clj.git"}
  :deploy-repositories [["releases" {:url "https://clojars.org/repo/"
                                     :username [:gpg :env/CLOJARS_USERNAME]
                                     :password [:gpg :env/CLOJARS_PASSWORD]
                                     :sign-releases false}]]
  :aot [rp.jackdaw.serdes.homogeneous-edn
        rp.jackdaw.serdes.key-serde
        rp.jackdaw.serdes.value-serde])
