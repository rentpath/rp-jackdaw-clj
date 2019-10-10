(ns rp.jackdaw.serdes.key-serde
  (:require [jackdaw.serdes.edn :as jackdaw-edn]
            [rp.jackdaw.serdes.homogeneous-edn :as hedn])
  (:gen-class
    :implements [org.apache.kafka.common.serialization.Serde]
    :prefix "EdnKeySerde-"
    :name rp.jackdaw.serdes.EdnKeySerde))

(def EdnKeySerde-configure
  (constantly nil))

(defn EdnKeySerde-serializer
  [& _]
  (hedn/serializer {:compact? true :canonicalize? true :validate? true}))

(defn EdnKeySerde-deserializer
  [& _]
  (jackdaw-edn/deserializer))
