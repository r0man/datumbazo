(ns datumbazo.test.fixtures
  (:require [clojure.java.io :refer [file resource]])
  (:use clojure.test
        datumbazo.test
        datumbazo.fixtures))

(def fixture-dir
  "test-resources/db/test-db/fixtures")

(def fixture-file
  (str fixture-dir "/continents.edn"))

(database-test test-enable-triggers
  (enable-triggers db :continents)
  (enable-triggers db :twitter.users))

(database-test test-disable-triggers
  (disable-triggers db :continents)
  (enable-triggers db :twitter.users))

(database-test test-dump-fixtures
  (dump-fixtures db "/tmp/test-dump-fixtures" [:continents :twitter.users])
  (is (.exists (file "/tmp/test-dump-fixtures/continents.edn")))
  (is (.exists (file "/tmp/test-dump-fixtures/twitter/users.edn"))))

(deftest test-fixture-path
  (is (= "db/test-db/fixtures" (fixture-path "test-db"))))

(deftest test-fixtures
  (is (= (fixture-seq (resource "db/test-db/fixtures"))
         (fixtures "test-db"))))

(deftest test-fixture-seq
  (let [fixtures (fixture-seq fixture-dir)]
    (is (= 2 (count fixtures)))
    (let [fixture (first fixtures)]
      (is (= (file fixture-dir "continents.edn")
             (:file fixture)))
      (is (= :continents (:table fixture))))
    (let [fixture (second fixtures)]
      (is (= (file fixture-dir "twitter" "users.edn")
             (:file fixture)))
      (is (= :twitter.users (:table fixture)))))
  (let [fixtures (fixture-seq (resource "db/test-db/fixtures"))]
    (is (= 2 (count fixtures)))
    (let [fixture (first fixtures)]
      (is (= (.getAbsoluteFile (file "test-resources/db/test-db/fixtures/continents.edn"))
             (:file fixture)))
      (is (= :continents (:table fixture))))
    (let [fixture (second fixtures)]
      (is (= (.getAbsoluteFile (file "test-resources/db/test-db/fixtures/twitter/users.edn"))
             (:file fixture)))
      (is (= :twitter.users (:table fixture))))))

(database-test test-load-fixtures
  (let [fixtures (load-fixtures db fixture-dir)]
    (is (= 2 (count fixtures)))))

(database-test test-read-fixture
  (let [fixture (read-fixture db :continents fixture-file)]
    (is (= :continents (:table fixture)))
    (is (= fixture-file (:file fixture)))
    (is (= 7 (count (:records fixture))))))

(deftest test-slurp-rows
  (let [records (slurp-rows fixture-file)]
    (is (= 7 (count records)))))

(database-test test-reset-serials
  (reset-serials db :continents)
  (reset-serials db :twitter.users))

(deftest test-serial-seq
  (are [column expected]
       (is (= expected (serial-seq column)))
       {:table :continents :name :id}
       :continents-id-seq
       {:schema :public :table :continents :name :id}
       :continents-id-seq
       {:schema :twitter :table :users :name :id}
       :twitter.users-id-seq))

(database-test test-write-fixture
  (load-fixtures db fixture-dir)
  (let [fixture (write-fixture db :continents "/tmp/test-write-fixture/continents.edn")]
    (is (= :continents (:table fixture)))
    (is (= "/tmp/test-write-fixture/continents.edn" (:file fixture)))
    (is (= 7 (:records fixture)))
    (is (= (slurp (str fixture-dir "/continents.edn"))
           (slurp "/tmp/test-write-fixture/continents.edn"))))
  (let [fixture (write-fixture db :twitter.users "/tmp/test-write-fixture/twitter/users.edn")]
    (is (= :twitter.users (:table fixture)))
    (is (= "/tmp/test-write-fixture/twitter/users.edn" (:file fixture)))
    (is (= 1 (:records fixture)))
    (is (= (slurp (str fixture-dir "/twitter/users.edn"))
           (slurp "/tmp/test-write-fixture/twitter/users.edn")))))
