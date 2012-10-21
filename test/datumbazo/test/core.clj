(ns datumbazo.test.core
  (:require [clojure.java.jdbc :as jdbc])
  (:use datumbazo.core
        datumbazo.test
        datumbazo.test.examples
        clojure.test))

(deftest test-as-identifier
  (is (nil? (as-identifier nil)))
  (is (nil? (as-identifier "")))
  (is (= "continents" (as-identifier :continents)))
  (is (= "continents" (as-identifier {:name :continents})))
  (is (= "public.continents" (as-identifier {:schema :public :name :continents})))
  (jdbc/with-quoted-identifiers \"
    (is (= "\"public\".\"continents\""
           (as-identifier {:schema :public :name :continents})))))

(database-test test-count-rows
  (is (= 0 (count-rows continents-table))))

(database-test test-delete-table
  (is (= 0 (delete-table continents-table)))
  (is (= 0 (count-rows continents-table))))

(database-test test-drop-table
  (is (= 0 (drop-table countries-table)))
  (is (= 0 (drop-table continents-table))))

(deftest test-make-table
  (let [table (make-table :continents)]
    (is (= :continents (:name table))))
  (is (= (make-table :continents)
         (make-table "continents"))))

(database-test test-truncate-table
  (is (= 0 (truncate-table continents-table :cascade true)))
  (is (= 0 (count-rows continents-table)))
  (is (= 0 (truncate-table countries-table)))
  (is (= 0 (count-rows countries-table))))

(deftest test-select
  (is (= "SELECT id, name FROM continents"
         (select continents-table)))
  (is (= "SELECT id, continent-id, name FROM countries"
         (select countries-table))))