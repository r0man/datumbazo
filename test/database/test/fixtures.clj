(ns database.test.fixtures
  (:require [clojure.java.io :refer [file]])
  (:use clojure.test
        database.test
        database.fixtures))

(def fixture-dir
  "resources/db/fixtures/test-db")

(def fixture-file
  (str fixture-dir "/continents.clj"))

(deftest test-clojure-file?
  (is (not (clojure-file? "NOT-EXISTING")))
  (is (not (clojure-file? fixture-dir)))
  (is (clojure-file? fixture-file)))

(deftest test-find-fixtures
  (let [fixtures (find-fixtures fixture-dir)]
    (is (= 1 (count fixtures)))
    (let [fixture (first fixtures)]
      (is (= (file fixture-file)
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
