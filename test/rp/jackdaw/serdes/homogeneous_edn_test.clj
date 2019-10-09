(ns rp.jackdaw.serdes.homogeneous-edn-test
  (:require
    [rp.jackdaw.serdes.homogeneous-edn :as sut]
    [clojure.test :refer :all]
    [clojure.spec.alpha :as s])
  (:import (java.util UUID)))

(deftest test-validation
  ;; scalar validity
  (is (sut/valid? true))
  (is (sut/valid? false))
  (is (sut/valid? nil))
  (is (sut/valid? 1))
  (is (sut/valid? (bigint 1)))
  (is (sut/valid? 1.2))
  (is (sut/valid? (bigdec 1.2)))
  (is (sut/valid? "string"))
  (is (sut/valid? :keyword))
  (is (sut/valid? :namespaced/keyword))
  (is (sut/valid? (UUID/randomUUID)))
  (is (sut/valid? (java.util.Date.)))
  ;; no symbols allowed
  (is (not (sut/valid? 'some-symbol)))
  ;; no byte arrays allowed
  (is (not (sut/valid? (.getBytes "bytearray"))))

  ;; ARRAY VALIDITY
  ;; scalar arrays
  (is (sut/valid? []))
  (is (sut/valid? [1 2 3]))
  (is (sut/valid? [:a :b :c nil]))
  ;; compound arrays
  (is (sut/valid? [[1 2 3] [4 5 6]]))
  (is (sut/valid? [{"a" 1 "b" 2} {"c" 3 "d" 4}]))
  (is (sut/valid? [{"a" 1 "b" 2} {"c" 3 "d" nil}]))
  (is (sut/valid? [{"a" 1 "b" 2} {"c" 3 "d" nil}]))

  (is (sut/valid? [{:int 1 :string "string"}
                   {:int 2 :string "anotherstring"}]))
  (is (sut/valid? [{:int 1 :string "string"}
                   {:int 2 :string nil}]))

  {[:vector :map :int]    #{:int}
   [:vector :map :string] #{:string :nil}}

  ;; arrays must be homogeneous in type and schema
  ;; heterogeneous elements
  (is (not (sut/valid? [1 :b :c])))
  ;; heterogeneous and nil elements
  (is (not (sut/valid? [1 :b :c nil])))
  ;; type-homogeneous and schema-heterogeneous elements
  (is (not (sut/valid? [[1 2 3] [:a :b :c]])))
  (is (not (sut/valid? [{"a" 1 "b" 2} {3 "c" 4 "d"}])))
  ;; while these aren't valid out of the box, they can be autoconformed
  (is (sut/valid? [{:int 1 :string "string"}
                   {:float 2.2 :keyword :keyword}]))
  (is (sut/valid? [{:int 1 :string "string"}
                   {:int 2 :string "anotherstring" :float 1.2}]))

  ;; MAP VALIDITY
  ;; scalar maps
  (is (sut/valid? {}))
  (is (sut/valid? {"a" 1 "b" 2}))
  (is (sut/valid? {1 "a" 2 "b"}))
  (is (sut/valid? {"a" 1 "b" nil}))
  ;; compound maps
  (is (sut/valid? {[1 2 3] ["a" "b" "c"]
                   [4 5 6] ["d" "e" "f"]}))
  (is (sut/valid? {{"a" 1} {2 "b"}
                   {"c" 3} {4 "d"}}))
  (is (sut/valid? {{:int 1 :string "string"}        {:float 1.2 :keyword :keyword}
                   {:int 2 :string "anotherstring"} {:float 3.4 :keyword :anotherkeyword}}))

  ;; nil keys not allowed
  (is (not (sut/valid? {1 "a" nil "b"})))

  ;; maps must be homogeneous in type and schema
  ;; heterogeneous keys
  (is (not (sut/valid? {"a" 1 2 2})))
  ;; heterogeneous values
  (is (not (sut/valid? {"a" 1 "b" "b"})))
  ;; heterogeneous keys and values
  (is (not (sut/valid? {"a" 1 2 "b"})))
  (is (not (sut/valid? {[1 2 3]       ["a" "b" "c"]
                        ["d" "e" "f"] [4 5 6]})))
  (is (not (sut/valid? {{"a" 1} {"c" 3}
                        {2 "b"} {4 "d"}})))
  ;; while this isn't valid out of the box, they can be autoconformed
  (is (sut/valid? {{:int 1 :string "string"}      {:int 2 :string "anotherstring"}
                   {:float 1.2 :keyword :keyword} {:float 3.4 :keyword :anotherkeyword}}))

  ;; STRUCT VALIDITY
  ;; heterogeneous scalar maps
  (is (sut/valid? {:int 1 :string "string" :keyword :keyword}))
  (is (sut/valid? {:int 1 :string "string" :keyword nil}))
  ;; nil keys not allowed
  (is (not (sut/valid? {:int 1 nil "string" :keyword :keyword}))))

(deftest testing-conformation
  (testing "conformity with validation"
    (is (= (sut/conform! [{:int 1 :string "string"}
                          {:float 2.2 :keyword :keyword}]
                         {:validate? true})
           [{:int 1 :string "string" :float nil :keyword nil}
            {:float 2.2 :keyword :keyword :int nil :string nil}]))
    (is (= (sut/conform! [{:int 1 :string "string"}
                          {:int 2 :string "anotherstring" :float 1.2}]
                         {:validate? true})
           [{:int 1 :string "string" :float nil}
            {:int 2 :string "anotherstring" :float 1.2}]))
    (is (= (sut/conform! {{:int 1 :string "string"}      {:int 2 :string "anotherstring"}
                          {:float 1.2 :keyword :keyword} {:float 3.4 :keyword :anotherkeyword}}
                         {:validate? true})
           {{:int 1 :string "string" :float nil :keyword nil}
            {:int 2 :string "anotherstring" :float nil :keyword nil}
            {:float 1.2 :keyword :keyword :int nil :string nil}
            {:float 3.4 :keyword :anotherkeyword :int nil :string nil}})))
  (testing "conformity without validation"
    (let [val [{:int 1 :string "string"}
               {:float 2.2 :keyword :keyword}]]
      (is (= (sut/conform! val {:validate? false}) val)))
    (let [val [{:int 1 :string "string"}
               {:int 2 :string "anotherstring" :float 1.2}]]
      (is (= (sut/conform! val {:validate? false}) val)))
    (let [val {{:int 1 :string "string"}      {:int 2 :string "anotherstring"}
               {:float 1.2 :keyword :keyword} {:float 3.4 :keyword :anotherkeyword}}]
      (is (= (sut/conform! val {:validate? false}) val))))
  (testing "conformity with canonicalization"
    (is (= (vec (sut/conform! {:c 3 :z 5 :x 4 :a 1 :b 2} {:canonicalize? true}))
           [[:a 1] [:b 2] [:c 3] [:x 4] [:z 5]]))
    (is (= (vec (sut/conform! {"c" 3 "z" 5 "x" 4 "a" 1 "b" 2} {:canonicalize? true}))
           [["a" 1] ["b" 2] ["c" 3] ["x" 4] ["z" 5]]))
    (is (= (vec (sut/conform! #{:c :z :x :a :b} {:canonicalize? true}))
           [:a :b :c :x :z]))))

(deftest testing-serialization
  (testing "standard"
    (is (= (sut/serialize* {:int    1
                            :str    "hat"
                            :kw     :imakeyword
                            :vec    [1 2 3]
                            :set    #{1 2 3}
                            :map    {"a" 1 "b" 2}
                            :struct {:a 1 :b 2}}
                           {:compact? false})
           "{:int 1, :str \"hat\", :kw :imakeyword, :vec [1 2 3], :set #{1 3 2}, :map {\"a\" 1, \"b\" 2}, :struct {:a 1, :b 2}}")))
  (testing "compact"
    (is (= (sut/serialize* {:int    1
                            :str    "hat"
                            :kw     :imakeyword
                            :vec    [1 2 3]
                            :set    #{1 2 3}
                            :map    {"a" 1 "b" 2}
                            :struct {:a 1 :b 2}}
                           {:compact? true})
           "{:int 1 :str\"hat\":kw :imakeyword :vec[1 2 3]:set#{1 3 2}:map{\"a\"1\"b\"2}:struct{:a 1 :b 2}}"))))
