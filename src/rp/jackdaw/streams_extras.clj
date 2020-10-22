(ns rp.jackdaw.streams-extras
  (:require [jackdaw.streams.interop])
  (:import [org.apache.kafka.streams.kstream Merger Windows TimeWindows SessionWindows
                                             Suppressed Suppressed$BufferConfig KTable KStream]
           [org.apache.kafka.streams.processor ProcessorSupplier Processor]
           [org.apache.kafka.streams StreamsBuilder]
           [org.apache.kafka.streams.state StoreBuilder]
           [java.time Duration]
           [jackdaw.streams.interop CljKTable CljKStream CljStreamsBuilder]))
;;
;; Provide some "extras" missing from Jackdaw
;;

(defn time-windows
  ^TimeWindows
  ;; Hopping (overlapping) windows, where advance-by-duration is less than window-duration.
  ([^Duration window-duration ^Duration advance-by-duration]
   (let [windows (TimeWindows/of window-duration)]
     (if advance-by-duration
       (.advanceBy windows advance-by-duration)
       windows)))
  ;; Tumbling (non-overlapping, gap-less) windows, where advance-by-duration is the same as window-duration.
  ([window-duration]
   (time-windows window-duration nil)))

(defn session-windows
  ^SessionWindows
  [^Duration inactivity-duration]
  (SessionWindows/with inactivity-duration))

(defmulti grace
  (fn [windows duration]
    (class windows)))

(defmethod grace TimeWindows
  ^TimeWindows
  [^TimeWindows windows ^Duration duration]
  (.grace windows duration))

(defmethod grace SessionWindows
  ^SessionWindows
  [^SessionWindows windows ^Duration duration]
  (.grace windows duration))

;;
;; Keep an eye on https://github.com/FundingCircle/jackdaw/pull/23
;; Once jackdaw has official suppress support we should prefer that.
;;

(defmulti suppress
  (fn [table suppressed]
    (class table)))

(defmethod suppress KTable
  ^KTable
  [^KTable table ^Suppressed suppressed]
  (.suppress table suppressed))

(defmethod suppress CljKTable
  ^CljKTable
  [^CljKTable table ^Suppressed suppressed]
  (CljKTable. (suppress (.ktable table) suppressed)))

;; Returns a value for passing to KTable#suppress method
;; See https://kafka.apache.org/21/javadoc/org/apache/kafka/streams/kstream/Suppressed.html#untilWindowCloses-org.apache.kafka.streams.kstream.Suppressed.StrictBufferConfig-
;; https://cwiki.apache.org/confluence/display/KAFKA/KIP-328%3A+Ability+to+suppress+updates+for+KTables
;; FIXME: When "spill to disk" buffer config is available, use that instead of "unbounded".
;; https://issues.apache.org/jira/browse/KAFKA-7224
(defn suppressed-until-window-closes
  ^Suppressed
  []
  (Suppressed/untilWindowCloses
   (Suppressed$BufferConfig/unbounded)))

(defn add-state-store
  [^CljStreamsBuilder builder ^StoreBuilder state-store-builder]
  (.addStateStore ^StreamsBuilder (.streams-builder builder) state-store-builder)
  builder)


;; FundingCircle.jackdaw.streams/process! fn when passed a ^KStream invokes
;; FundingCircle.jackdaw.streams.lambdas/processor-supplier which takes in a processor-fn and applies that
;; fn as the body of the process method of a concrete implementation of the org.apache.kafka.streams.processor.Processor IFace
;; (see FnProcessor type in FundingCircle.jackdaw.streams.lambdas). This is handy, but it does not work if you want to define
;; your own implementation of the Kafka Processor IFace. An example of a problem with the FundingCircle.jackdaw FnProcessor
;; type is that it does not allow you to define any scheduled (punctuated) callbacks in the init method since that
;; method is already 'pre-defined'. For poi listings enrichment we need to define a scheduled callback to act as a batch timeout,
;; so below we implement our own processor fn that calls a handrolled processor-supplier.
(defprotocol IKStreamProcessor
  (processor
    [kstream processor-supplier-fn]
    [kstream processor-supplier-fn state-store-names]
    "Creates a KStream with the processor supplied by processor-supplier-fn attached to the topology.
    The processor-supplier-fn should reify the org.apache.kafka.streams.processor.Processor IFace."))

(deftype FnProcessorSupplierFactory [processor-supplier-fn]
  ProcessorSupplier
  (get [this]
    (processor-supplier-fn)))

(defn processor-supplier
  "Packages up a Clojure fn in a kstream processor supplier."
  [processor-supplier-fn]
  (FnProcessorSupplierFactory. processor-supplier-fn))

(extend-type CljKStream
  IKStreamProcessor
  (processor
    ([this processor-supplier-fn]
     (processor this processor-supplier-fn []))
    ([this processor-supplier-fn state-store-names]
     (.process ^KStream (.kstream this)
               ^ProcessorSupplier (processor-supplier processor-supplier-fn)
               ^"[Ljava.lang.String;" (into-array String state-store-names)))))
