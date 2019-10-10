(ns rp.jackdaw.serdes.value-serde
  (:require [jackdaw.serdes.edn :as jackdaw-edn]
            [rp.jackdaw.serdes.homogeneous-edn :as hedn])
  (:gen-class
    :implements [org.apache.kafka.common.serialization.Serde]
    :prefix "EdnValueSerde-"
    :name rp.jackdaw.serdes.EdnValueSerde))

(def EdnValueSerde-configure
  (constantly nil))

(defn EdnValueSerde-serializer
  [& _]
  (hedn/serializer {:compact? true :canonicalize? false :validate? true}))

(defn EdnValueSerde-deserializer
  [& _]
  (jackdaw-edn/deserializer))
