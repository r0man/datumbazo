(ns database.test.util
  (:require [clj-time.core :refer [now]])
  (:use clojure.test
        database.util))

(deftest test-dissoc-if
  (is (= {:a 1 :b nil} (dissoc-if {:a 1 :b nil} nil?)))
  (is (= {:a 1} (dissoc-if {:a 1 :b nil} nil? :a :b)))
  (is (= {:b nil} (dissoc-if {:a 1 :b nil} (comp not nil?) :a :b))))

(deftest test-parse-float
  (is (nil? (parse-float nil :junk-allowed true)))
  (is (nil? (parse-float "" :junk-allowed true)))
  (is (= (float 0) (parse-float "0")))
  (is (= (float 1) (parse-float "1")))
  (is (= (float 1.1) (parse-float "1.1")))
  (is (= (float 1.1) (parse-float 1.1))))

(deftest test-parse-integer
  (testing "junk allowed"
    (are [string]
      (is (nil? (parse-integer string :junk-allowed true)))
      nil "" "junk"))
  (testing "junk not allwed"
    (are [junk]
      (is (thrown? Exception (parse-integer junk)))
      nil "" "junk"))
  (testing "valid integer"
    (are [string expected]
      (is (= (parse-integer string) expected))
      "-1" -1
      "0" 0
      "25" 25
      "12351" 12351
      "321-mundaka" 321
      2344 2344)))

(deftest test-parse-url
  (let [spec (parse-url "postgresql://tiger:scotch@localhost/example")]
    (is (= "postgresql" (:subprotocol spec)))
    (is (= "tiger" (:user spec)))
    (is (= "scotch" (:password spec)))
    (is (= "//localhost/example" (:subname spec)))
    (is (= "localhost" (:host spec)))
    (is (= 5432 (:port spec)))
    (is (= "example" (:db spec))))
  (let [spec (parse-url "postgresql://tiger:scotch@localhost:1234/example?x=1")]
    (is (= "postgresql" (:subprotocol spec)))
    (is (= "tiger" (:user spec)))
    (is (= "scotch" (:password spec)))
    (is (= "//localhost:1234/example?x=1" (:subname spec)))
    (is (= "localhost" (:host spec)))
    (is (= 1234 (:port spec)))
    (is (= "example" (:db spec)))))

(deftest test-split-args
  (is (= [[] {}]
         (split-args [])))
  (is (= [[1 2 3] {}]
         (split-args [1 2 3])))
  (is (= [["x"] {:page 1 :per-page 20}]
         (split-args ["x" :page 1 :per-page 20])))
  (is (= [["x" :doit] {:page 1 :per-page 20}]
         (split-args ["x" :doit :page 1 :per-page 20]))))

(deftest test-shift-columns
  (is (= {} (shift-columns {} nil)))
  (is (= {:a 1} (shift-columns {:a 1} nil)))
  (is (= {:name "Mundaka" :country {:name "Spain"}}
         (shift-columns {:name "Mundaka" :country_name "Spain"} :country)))
  (is (= {:name "Mundaka" :country {:name "Spain"}}
         (shift-columns {:name "Mundaka" :country-name "Spain"} :country)))
  (is (= {:name "Mundaka" :address {:country {:name "Spain"}}}
         (shift-columns {:name "Mundaka" :country_name "Spain"} :country :address))))

(deftest test-prefix-keywords
  (are [prefix keywords separator expected]
    (is (= expected (prefix-keywords prefix keywords separator)))
    :photo [:id :name] nil [:photoid :photoname]
    :photo [:id :name] "" [:photoid :photoname]
    :photo [:id :name] "." [:photo.id :photo.name]
    :photo [:id :name] "-" [:photo-id :photo-name]))

(deftest test-with-frozen-time
  (let [expected (now)]
    (with-frozen-time expected
      (is (= expected (now)))
      (Thread/sleep 10)
      (is (= expected (now))))))