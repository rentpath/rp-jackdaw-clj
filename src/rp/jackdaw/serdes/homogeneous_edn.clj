(ns rp.jackdaw.serdes.homogeneous-edn
  "Implements an EDN SerDe (Serializer/Deserializer). While the native jackdaw EDN SerDe fulfills
   most purposes, in order for the RentPath Kafka Connect EdnConverter
   (https://search.maven.org/artifact/com.rentpath/kafka-connect-converters/0.1.1/jar) to infer
   the requisite schema from the data itself, said data must conform to a series of further rules
   above and beyond those normally associated with the EDN specification; namely, that all compound
   data structures be consistent, their elements homogeneous in both type and structure. This SerDe
   exists to provide an (optional) means of validating these additional requirements, so as to ensure
   that no data from which a schema may not be autoderived (and thus unable to be sunk) is produced
   to any Kafka topic that is intended for Kafka Connect sinking."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.edn]
            [jackdaw.serdes.fn :as jsfn]
            [jackdaw.serdes.edn :as jackdaw-edn]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log])
  (:import org.apache.kafka.common.serialization.Serdes
           [java.util GregorianCalendar Set]
           [clojure.lang BigInt]
           [us.bpsm.edn.printer Printers Printer$Fn Printer])
  (:gen-class
    :implements [org.apache.kafka.common.serialization.Serde]
    :prefix "EdnSerde-"
    :name rp.jackdaw.serdes.EdnSerde))

(def bytearray-type (Class/forName "[B"))

(defn- soft-space-wrapper
  [& [str-fn]]
  (let [str-fn (or str-fn str)]
    (reify Printer$Fn
      (eval [this v writer]
        (doto writer
          (.softspace)
          (.append (str-fn v))
          (.softspace))))))

(defn- seq-printer
  [open-str close-str]
  (reify Printer$Fn
    (eval [this s writer]
      (.append
        (reduce (fn [^Printer writer v]
                  (.printValue writer v))
                (.append writer open-str)
                s)
        close-str))))

(def set-printer (seq-printer "#{" "}"))
(def vec-printer (seq-printer "[" "]"))
(def list-printer (seq-printer "(" ")"))

(def compact-protocol
  "This defines an extension of the default bpsm EDN lib printer to permit printing of native clojure
   data structures, which it normally has no concept of."
  (doto (Printers/defaultProtocolBuilder)
    (.put clojure.lang.Keyword (soft-space-wrapper))
    (.put clojure.lang.PersistentHashSet set-printer)
    (.put clojure.lang.PersistentTreeSet set-printer)
    (.put clojure.lang.PersistentVector vec-printer)
    (.put clojure.lang.PersistentUnrolledVector vec-printer)
    (.put clojure.lang.PersistentList list-printer)
    (.put clojure.lang.BigInt (soft-space-wrapper pr-str))
    (.build)))

(set! *warn-on-reflection* true)

(s/def ::scalar
  (s/nilable
    (s/or
      :limited-integer int?
      :big-integer #(or (instance? BigInt %) (instance? BigInteger %))
      :limited-decimal float?
      :big-decimal #(instance? BigDecimal %)
      :char #(instance? Character %)
      :string string?
      :uuid uuid?
      :boolean boolean?
      :keyword keyword?
      :symbol symbol?
      :byte-array #(= (type %) bytearray-type)
      :date #(or (inst? %) (instance? GregorianCalendar %)))))

(s/def ::set set?)

(s/def ::schema-key #{:kafka/array
                      :kafka/map-key
                      :kafka/map-value})

(s/def ::struct-key (s/map-of #{:kafka/struct-key} keyword?))

(s/def ::keypath-element (s/or :schema-key ::schema-key
                               :struct-key ::struct-key))

(s/def ::keypath (s/and vector?
                        (s/coll-of ::keypath-element)))

(s/def ::type #{:limited-integer
                :big-integer
                :limited-decimal
                :big-decimal
                :char
                :string
                :uuid
                :boolean
                :keyword
                :date
                :array
                :map
                :struct})

(s/def ::typeset (s/and set?
                        seq
                        (s/coll-of (s/nilable ::type))
                        (fn [v] (<= (count (keep identity v)) 1))))

(s/def ::legal-nil-schema (s/coll-of
                            (fn [[k v]]
                              (or (not= :kafka/map-key (last k))
                                  (not-any? nil? v)))))

(s/def ::schema (s/and (s/map-of ::keypath ::typeset)
                       ::legal-nil-schema))

(s/def ::array (s/or :vector vector?
                     :list list?
                     :set ::set))

(s/def ::map (s/and map?
                    #(not (every? keyword? (keys %)))))

(s/def ::struct (s/and map?
                       #(every? keyword? (keys %))))

(declare compile-schema-and-keysets)

(defn- compile-array-schema
  [data keypath schema-and-keysets]
  (let [new-keypath (conj keypath :kafka/array)]
    (reduce (fn [acc v]
              (compile-schema-and-keysets v new-keypath acc))
            (update-in schema-and-keysets [:schema keypath] conj :array)
            data)))

(defn- compile-map-schema
  [data keypath schema-and-keysets]
  (let [new-key-keypath (conj keypath :kafka/map-key)
        new-value-keypath (conj keypath :kafka/map-value)]
    (reduce-kv (fn [acc k v]
                 (->> acc
                      (compile-schema-and-keysets v new-value-keypath)
                      (compile-schema-and-keysets k new-key-keypath)))
               (update-in schema-and-keysets [:schema keypath] conj :map)
               data)))

(defn- compile-struct-schema
  [data keypath schema-and-keysets]
  (reduce-kv (fn [acc k v]
               (let [new-keypath (conj keypath {:kafka/struct-key k})]
                 (update-in (compile-schema-and-keysets v new-keypath acc)
                            [:keysets keypath]
                            conj
                            k)))
             (update-in schema-and-keysets [:schema keypath] conj :struct)
             data))

(defn- compile-scalar-schema
  [data keypath schema-and-keysets]
  (update-in schema-and-keysets [:schema keypath] conj (first (s/conform ::scalar data))))

(defn- setify-values
  [[k v]]
  [k (into {}
           (map (fn [[k v]] [k (set v)]))
           v)])

(defn- compile-schema-and-keysets
  "Recursively walks the input data structure to compose both A) a schema map of keypath->typeset
   recording the set of all types associated with a given keypath in the tree, and B) a keyset
   map of keypath->keyset detailing the union of all struct keys found at a given keypath, for
   subsequent homogenization w/ rp.jackdaw.serdes.homogeneous-edn/conform."
  [data & [keypath schema-and-keysets]]
  (let [keypath (or keypath [])
        schema-and-keysets (or schema-and-keysets {:schema  {}
                                                   :keysets {}})]
    (into {}
          (map setify-values)
          (cond
            (s/valid? ::array data)
            (compile-array-schema data keypath schema-and-keysets)

            (s/valid? ::map data)
            (compile-map-schema data keypath schema-and-keysets)

            (s/valid? ::struct data)
            (compile-struct-schema data keypath schema-and-keysets)

            :else
            (compile-scalar-schema data keypath schema-and-keysets)))))

(declare conform*)

(defn- conform-array
  [data keysets keypath {:keys [canonicalize?] :as opts}]
  (let [new-keypath (conj keypath :kafka/array)
        conformed (reduce (fn [acc v]
                            (conj acc (conform* v keysets new-keypath opts)))
                          (empty data)
                          data)]
    (if (and canonicalize? (set? conformed))
      (try
        (into (sorted-set) conformed)
        (catch ClassCastException e
          (log/warn "Cannot canonicalize set with compound elements. Passing original value." {:set conformed})
          conformed))
      conformed)))

(defn- conform-map
  [data keysets keypath {:keys [canonicalize?] :as opts}]
  (let [conformed (let [new-key-keypath (conj keypath :kafka/map-key)
                        new-value-keypath (conj keypath :kafka/map-value)]
                    (reduce-kv (fn [acc k v]
                                 (assoc acc (conform* k keysets new-key-keypath opts)
                                            (conform* v keysets new-value-keypath opts)))
                               (empty data)
                               data))]
    (if canonicalize?
      (try
        (into (sorted-map) conformed)
        (catch ClassCastException e
          (log/warn "Cannot canonicalize map with compound keys. Passing original value." {:map conformed})
          conformed))
      conformed)))

(defn- conform-struct
  [data keysets keypath {:keys [canonicalize?] :as opts}]
  (let [conformed (reduce (fn [acc k]
                            (let [new-keypath (conj keypath {:kafka/struct-key k})]
                              (assoc acc k (conform* (get data k) keysets new-keypath opts))))
                          (empty data)
                          (get keysets keypath))]
    (if canonicalize?
      (try
        (into (sorted-map) conformed)
        (catch ClassCastException e
          (log/warn "Cannot canonicalize struct with compound keys. Passing original value." {:struct conformed})
          conformed))
      conformed)))

(defn- conform*
  "Recursively homogenizes all structs in the input data according to the input keysets map,
   acquired via rp.jackdaw.serdes.homogeneous-edn/compile-schema-and-keysets."
  [data keysets keypath {:keys [validate?] :as opts}]
  (cond
    (s/valid? ::array data)
    (conform-array data keysets keypath opts)

    (or (and (not validate?)
             (map? data))
        (s/valid? ::map data))
    (conform-map data keysets keypath opts)

    (and validate?
         (s/valid? ::struct data))
    (conform-struct data keysets keypath opts)

    ;; jodatime DateTimes need to be converted to native java.util.Date values in order to be
    ;; rendered using the inst reader macro.
    (inst? data)
    (if (= (class data) org.joda.time.DateTime)
      (.toDate ^org.joda.time.DateTime data))

    :else
    data))

(defn valid?
  "Will return true if the input data structure conforms to the homogeneous EDN requirements. May
   be provided with an optional previously-compiled schema structure to prevent recompilation."
  [data & [schema]]
  (let [schema (or schema (:schema (compile-schema-and-keysets data)))]
    (s/valid? ::schema schema)))

(defn conform!
  "Conforms the provided data structure according to the passed options.

   If the validate? option is set to true, validates the input as homogeneous in all respects save
   structs, recursively homogenizing that latter, returning the resulting conformed data structure.
   Will throw an exception if not valid and conformable in this case.

   If the canonicalize? option is set to true, will enforce Comparable-type ordering on all map and
   struct keys as well as set elements. If it encounters data using complex keys and/or elements,
   those will be returned as-is and a warning logged."
  [data {:keys [validate?] :as opts}]
  (let [{:keys [schema keysets]} (when validate? (compile-schema-and-keysets data))]
    (if (or (not validate?) (valid? data schema))
      (conform* data keysets [] opts)
      (throw (ex-info "Invalid Schematizable EDN!" {:explanation (s/explain ::schema schema)})))))

(defn serialize*
  "Converts the provided data structure to EDN. If the compact? option is specified, will use the
   bpsm java EDN compact serialization rather than the native clojure prn-str fn."
  [data {:keys [compact?] :as opts}]
  (if compact?
    (Printers/printString compact-protocol data)
    (pr-str data)))

(defn serializer
  "Returns an EDN serializer. If the `:validate?` option is set, the data to be written will first
   be validated as and conformed to the homogeneous EDN spec. Will throw an exception if not valid
   and conformable."
  ([]
   (serializer {}))
  ([opts]
   (jsfn/new-serializer
     {:serialize (fn [_ _ data]
                   (when data
                     (jackdaw-edn/to-bytes
                       (binding [*print-length* false
                                 *print-level* false]
                         (serialize* (conform! data opts) opts)))))})))

(defn serde
  "Returns an optionally-homogeneous EDN serde."
  [& [opts]]
  (Serdes/serdeFrom (serializer opts) (jackdaw-edn/deserializer opts)))

(def EdnSerde-configure
  (constantly nil))

(defn EdnSerde-serializer
  [& _]
  (serializer))

(defn EdnSerde-deserializer
  [& _]
  (jackdaw-edn/deserializer))
