(ns rp.jackdaw.integration-test
  (:require [clojure.test :refer :all]
            [rp.jackdaw.processor :as processor]
            [rp.jackdaw.topic-registry :as registry]
            [jackdaw.streams :as streams]
            [com.stuartsierra.component :as component]
            [cheshire.core :as json]))

;;
;; This demonstrates how one might test a Processor component's topology-builder-fn.
;; It uses the mock components, so it doesn't require a running Kafka.
;;
;; The example topology takes a stream of inputs where each has a number `n`.
;; It outputs a stream with the running total, count and average for each key.
;;

(defn topology-builder-fn
  [{:keys [topic-configs] :as component}]
  (fn [builder]
    (-> builder
        (streams/kstream (:input topic-configs))
        (streams/group-by-key)
        (streams/aggregate (constantly {:total 0
                                        :count 0
                                        :average 0.0})
                           (fn [{:keys [total count] :as agg}
                                [k {:keys [n] :as v}]]
                             (let [total (+ total n)
                                   count (inc count)
                                   average (/ (* 1.0 total) count)]
                               {:total total
                                :count count
                                :average average}))
                           (:output topic-configs))
        (streams/to-kstream)
        (streams/to (:output topic-configs)))))

(deftest test-mock-system
  (let [input-schema {:type "record"
                      :name "InputValue"
                      :fields [{:name "n" :type "long"}]}
        output-schema {:type "record"
                       :name "OutputValue"
                       :fields [{:name "total" :type "long"}
                                {:name "count" :type "long"}
                                {:name "average" :type "double"}]}
        topic-metadata {:input {:topic-name "input"
                                :key-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                                            :key? true
                                            :schema (json/encode "string")}
                                :value-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                                              :key? false
                                              :schema (json/encode input-schema)}}
                        :output {:topic-name "output"
                                 :key-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                                             :key? true
                                             :schema (json/encode "string")}
                                 :value-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                                               :key? false
                                               :schema (json/encode output-schema)}}}
        sys (component/system-map
             :topic-registry (registry/map->MockTopicRegistry {:topic-metadata topic-metadata})
             :processor (component/using
                         ;; Note that a real Processor would also need `:app-config` with at least `"application.id"` and `"bootstrap.servers"`.
                         (processor/map->MockProcessor {:topology-builder-fn topology-builder-fn})
                         [:topic-registry]))
        sys (component/start sys)
        processor (:processor sys)]
    (is (= [] (processor/mock-get-keyvals processor :output)))
    (processor/mock-produce! processor :input "a" {:n 5})
    (is (= [["a" {:total 5 :count 1 :average 5.0}]]
           (processor/mock-get-keyvals processor :output)))
    (processor/mock-produce! processor :input "a" {:n 7})
    (is (= [["a" {:total 12 :count 2 :average 6.0}]]
           (processor/mock-get-keyvals processor :output)))
    (processor/mock-produce! processor :input "a" {:n 9})
    (is (= [["a" {:total 21 :count 3 :average 7.0}]]
           (processor/mock-get-keyvals processor :output)))
    (component/stop sys)))
