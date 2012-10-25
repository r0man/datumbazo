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
  (is (= 0 (count-rows :continents))))

(database-test test-delete-table
  (is (= 0 (delete-table continents-table)))
  (is (= 0 (count-rows :continents))))

(database-test test-drop-table
  (is (= 0 (drop-table :countries)))
  (is (= 0 (drop-table :continents))))

(deftest test-make-table
  (let [table (make-table :continents)]
    (is (= :continents (:name table))))
  (is (= (make-table :continents)
         (make-table "continents"))))

(database-test test-truncate
  (is (= 0 (truncate :continents :cascade true)))
  (is (= 0 (count-rows :continents)))
  (is (= 0 (truncate :countries)))
  (is (= 0 (count-rows :countries))))

(deftest test-select
  (is (= "SELECT id, name FROM continents"
         (select continents-table)))
  (is (= "SELECT id, continent-id, name FROM countries"
         (select countries-table))))
