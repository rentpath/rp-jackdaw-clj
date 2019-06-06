(ns rp.jackdaw.streams-extras
  (:require [jackdaw.streams.interop])
  (:import [org.apache.kafka.streams.kstream Merger Windows TimeWindows SessionWindows Suppressed Suppressed$BufferConfig KTable]
           [org.apache.kafka.streams StreamsBuilder]
           [org.apache.kafka.streams.state StoreBuilder]
           [java.time Duration]
           [jackdaw.streams.interop CljKTable CljStreamsBuilder]))

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
