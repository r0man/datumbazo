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
  (enable-triggers :continents)
  (enable-triggers :twitter.users))

(database-test test-disable-triggers
  (disable-triggers :continents)
  (enable-triggers :twitter.users))

(database-test test-dump-fixtures
  (dump-fixtures "/tmp/test-dump-fixtures" [:continents :twitter.users])
  (is (.exists (file "/tmp/test-dump-fixtures/continents.clj")))
  (is (.exists (file "/tmp/test-dump-fixtures/twitter/users.clj"))))

(deftest test-fixture-path
  (is (= "db/fixtures/test-db" (fixture-path "test-db"))))

(deftest test-fixtures
  (is (= (fixture-seq (resource "db/fixtures/test-db"))
         (fixtures "test-db"))))

(deftest test-fixture-seq
  (let [fixtures (fixture-seq fixture-dir)]
    (is (= 2 (count fixtures)))
    (let [fixture (first fixtures)]
      (is (= (file fixture-dir "continents.clj")
             (:file fixture)))
      (is (= :continents (:table fixture))))
    (let [fixture (second fixtures)]
      (is (= (file fixture-dir "twitter" "users.clj")
             (:file fixture)))
      (is (= :twitter.users (:table fixture)))))
  (let [fixtures (fixture-seq (resource "db/fixtures/test-db"))]
    (is (= 2 (count fixtures)))
    (let [fixture (first fixtures)]
      (is (= (.getAbsoluteFile (file "resources/db/fixtures/test-db/continents.clj"))
             (:file fixture)))
      (is (= :continents (:table fixture))))
    (let [fixture (second fixtures)]
      (is (= (.getAbsoluteFile (file "resources/db/fixtures/test-db/twitter/users.clj"))
             (:file fixture)))
      (is (= :twitter.users (:table fixture))))))

(database-test test-load-fixtures
  (let [fixtures (load-fixtures fixture-dir)]
    (is (= 2 (count fixtures)))))

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
  (let [fixture (write-fixture :continents "/tmp/test-write-fixture/continents.clj")]
    (is (= :continents (:table fixture)))
    (is (= "/tmp/test-write-fixture/continents.clj" (:file fixture)))
    (is (= 7 (:records fixture)))
    (is (= (slurp (str fixture-dir "/continents.clj"))
           (slurp "/tmp/test-write-fixture/continents.clj"))))
  (let [fixture (write-fixture :twitter.users "/tmp/test-write-fixture/twitter/users.clj")]
    (is (= :twitter.users (:table fixture)))
    (is (= "/tmp/test-write-fixture/twitter/users.clj" (:file fixture)))
    (is (= 1 (:records fixture)))
    (is (= (slurp (str fixture-dir "/twitter/users.clj"))
           (slurp "/tmp/test-write-fixture/twitter/users.clj")))))
