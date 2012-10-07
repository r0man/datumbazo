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

(deftest test-slurp-fixtures
  (let [records (slurp-fixtures fixture-file)]
    (is (= 7 (count records)))))

(database-test test-load-fixture
  (let [fixture (load-fixture {:table :continents :file fixture-file})]
    (is (= :continents (:table fixture)))
    (is (= fixture-file (:file fixture)))
    (is (= 7 (count (:records fixture))))))

(database-test test-load-fixtures
  (let [fixtures (load-fixtures fixture-dir)]
    (is (= 1 (count fixtures)))))
