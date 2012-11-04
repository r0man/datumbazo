(ns datumbazo.test.fixtures
  (:require [clojure.java.io :refer [file resource]])
  (:use clojure.test
        datumbazo.test
        datumbazo.fixtures))

(def fixture-dir
  "resources/db/fixtures/test-db")

(def fixture-file
  (str fixture-dir "/continents.clj"))

(database-test test-enable-triggers
  (enable-triggers :continents))

(database-test test-disable-triggers
  (disable-triggers :continents))

(deftest test-fixture-path
  (is (= "db/fixtures/test-db" (fixture-path "test-db"))))

(deftest test-fixtures
  (is (= (fixture-seq (resource "db/fixtures/test-db"))
         (fixtures "test-db"))))

(deftest test-fixture-seq
  (let [fixtures (fixture-seq fixture-dir)]
    (is (= 1 (count fixtures)))
    (let [fixture (first fixtures)]
      (is (= (file fixture-file)
             (:file fixture)))
      (is (= :continents (:table fixture)))))
  (let [fixtures (fixture-seq (resource "db/fixtures/test-db"))]
    (is (= 1 (count fixtures)))
    (let [fixture (first fixtures)]
      (is (= (file (.getAbsolutePath (file fixture-file)))
             (:file fixture)))
      (is (= :continents (:table fixture))))))

(database-test test-load-fixtures
  (let [fixtures (load-fixtures fixture-dir)]
    (is (= 1 (count fixtures)))))

(database-test test-read-fixture
  (let [fixture (read-fixture :continents fixture-file)]
    (is (= :continents (:table fixture)))
    (is (= fixture-file (:file fixture)))
    (is (= 7 (:records fixture)))))

(deftest test-slurp-rows
  (let [records (slurp-rows fixture-file)]
    (is (= 7 (count records)))))

(database-test test-write-fixture
  (load-fixtures fixture-dir)
  (let [fixture (write-fixture :continents "/tmp/test-write-fixture")]
    (is (= :continents (:table fixture)))
    (is (= "/tmp/test-write-fixture" (:file fixture)))
    (is (= 7 (:records fixture)))
    (is (= (slurp fixture-file)
           (slurp "/tmp/test-write-fixture")))))
