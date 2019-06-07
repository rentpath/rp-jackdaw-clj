(ns rp.jackdaw.user
  "Some handy functions for use in the repl."
  (:require [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]))

;;
;; Create and list topics
;;

(defn admin-config
  "Convenient helper for building an admin config map given optional bootstrap servers string."
  ([bootstrap-servers]
   {"bootstrap.servers" bootstrap-servers})
  ([]
   (admin-config "localhost:9092")))

(defn create-topic
  "Creates a Kafka topic."
  [admin-config topic-config]
  (with-open [client (ja/->AdminClient admin-config)]
    (ja/create-topics! client [topic-config])))

(defn list-topics
  "Returns a list of Kafka topics."
  [admin-config]
  (with-open [client (ja/->AdminClient admin-config)]
    (ja/list-topics client)))

(defn topic-exists?
  "Returns true if the topic exists."
  [admin-config topic-config]
  (with-open [client (ja/->AdminClient admin-config)]
    (ja/topic-exists? client topic-config)))


;;
;; Produce and consume records
;;

(defn producer-config
  "Convenient helper for building a producer config map given optional bootstrap servers string."
  ([bootstrap-servers]
   {"bootstrap.servers" bootstrap-servers})
  ([]
   (producer-config "localhost:9092")))

(defn consumer-config
  "Convenient helper for building a consumer config map given optional bootstrap servers string and group id.
  `group-id` defaults to a random UUID.
  `bootstrap-servers` defaults to default port on localhost."
  ([bootstrap-servers group-id]
   {"bootstrap.servers" bootstrap-servers
    "group.id" group-id
    "auto.offset.reset" "earliest"
    "enable.auto.commit" "false"})

  ([bootstrap-servers]
   (consumer-config bootstrap-servers
                    (str (java.util.UUID/randomUUID))))

  ([]
   (consumer-config "localhost:9092")))


(defn publish
  "Takes a producer config, topic config and record value, and (optionally) a key and
  partition number, and produces to a Kafka topic."
  ([producer-config topic-config value]
   (with-open [client (jc/producer producer-config topic-config)]
     @(jc/produce! client topic-config value)))

  ([producer-config topic-config key value]
   (with-open [client (jc/producer producer-config topic-config)]
     @(jc/produce! client topic-config key value)))

  ([producer-config topic-config partition key value]
   (with-open [client (jc/producer producer-config topic-config)]
     @(jc/produce! client topic-config partition key value))))


(def default-polling-interval-ms 200)

(defn get-records
  "Takes a consumer config and topic config, consumes from a Kafka topic, and returns a seq of maps."
  ([consumer-config topic-config]
   (get-records consumer-config topic-config default-polling-interval-ms))

  ([consumer-config topic-config polling-interval-ms]
   (with-open [client (jc/subscribed-consumer consumer-config
                                              [topic-config])]
     (doall (jcl/log client polling-interval-ms seq)))))


(defn get-keyvals
  "Takes a consumer config and topic config, consumes from a Kafka topic, and returns a seq of key-value pairs."
  ([consumer-config topic-config]
   (get-keyvals consumer-config topic-config default-polling-interval-ms))

  ([consumer-config topic-config polling-interval-ms]
   (map (juxt :key :value) (get-records consumer-config topic-config polling-interval-ms))))
