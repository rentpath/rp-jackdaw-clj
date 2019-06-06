(ns rp.jackdaw.subscriber
  "A Processor component that simply consumes from one or more topics and calls a callback for each record.
   Like `Processor` it takes `app-config` and `topic-registry`.
   Unlike `Processor` instead of a `topology-builder-fn` it takes:
   `topic-kws`: a collection of keywords identifying topics to consume.
   `callback-fn`: a function that's called for each record; it's passed a map with the keys [:k :v :component]."
  (:require [rp.jackdaw.processor :as processor]
            [jackdaw.streams :as streams]))

(defn topology-builder-fn
  [{:keys [topic-configs topic-kws callback-fn] :as component}]
  (fn [builder]
    (doseq [topic-kw topic-kws]
      (-> builder
          (streams/kstream (get topic-configs topic-kw))
          (streams/for-each! (fn [[k v]] (callback-fn {:component component :k k :v v})))))
    builder))

(defn map->Subscriber
  [m]
  (processor/map->Processor (assoc m :topology-builder-fn topology-builder-fn)))
