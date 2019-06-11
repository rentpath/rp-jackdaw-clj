(ns rp.jackdaw.user
  "Some handy functions for use in the repl."
  (:require [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]))

;;
;; Create, list, delete topics
;;

(defn admin-config
  "Convenient helper for building an admin config map given optional bootstrap servers string."
  ([bootstrap-servers]
   {"bootstrap.servers" bootstrap-servers})
  ([]
   (admin-config "localhost:9092")))

(defn create-topics!
  "Create multiple Kafka topics given a sequence of topic configs."
  [admin-config topic-configs]
  (with-open [client (ja/->AdminClient admin-config)]
    (ja/create-topics! client topic-configs)))

(defn create-topic!
  "Creates a single Kafka topic given a topic config."
  [admin-config topic-config]
  (create-topics! admin-config [topic-config]))

(defn list-topics
  "Returns a list of Kafka topics."
  [admin-config]
  (with-open [client (ja/->AdminClient admin-config)]
    (ja/list-topics client)))

(defn delete-topics!
  "Delete multiple Kafka topics given a sequence of topic configs."
  [admin-config topic-configs]
  (with-open [client (ja/->AdminClient admin-config)]
    (ja/delete-topics! client topic-configs)))

(defn delete-topic!
  "Delete a single Kafka topic given a topic config."
  [admin-config topic-config]
  (delete-topics! admin-config [topic-config]))

(defn re-delete-topics!
  "Takes an instance of java.util.regex.Pattern and deletes any Kafka
  topics that match."
  [admin-config re]
  (with-open [client (ja/->AdminClient admin-config)]
    (let [topics-to-delete (->> (ja/list-topics client)
                                (filter #(re-find re (:topic-name %))))]
      (ja/delete-topics! client topics-to-delete))))

(defn topic-exists?
  "Returns true if a topic exists with the name specified in the given topic config."
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

(defn produce!
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
