(ns datumbazo.fixtures-test
  (:require [clojure.java.io :refer [file resource]]
            [clojure.test :refer :all]
            [datumbazo.core-test :refer :all]
            [datumbazo.fixtures :refer :all]
            [datumbazo.test :refer :all]))

(def fixture-dir
  "test-resources/db/test-db/fixtures")

(def fixture-file
  (str fixture-dir "/continents.edn"))

(deftest test-enable-triggers
  (with-backends [db]
    (enable-triggers db :continents)
    (enable-triggers db :twitter.users)))

(deftest test-disable-triggers
  (with-backends [db]
    (disable-triggers db :continents)
    (enable-triggers db :twitter.users)))

(deftest test-delete-fixtures
  (with-backends [db]
    (delete-fixtures db (tables fixture-dir))))

(deftest test-dump-fixtures
  (with-backends [db]
    (dump-fixtures db "/tmp/test-dump-fixtures" [:continents :twitter.users])
    (is (.exists (file "/tmp/test-dump-fixtures/continents.edn")))
    (is (.exists (file "/tmp/test-dump-fixtures/twitter/users.edn")))))

(deftest test-fixture-path
  (is (= "db/test-db/fixtures" (fixture-path "test-db"))))

(deftest test-fixtures
  (is (= (fixture-seq (resource "db/test-db/fixtures"))
         (fixtures "test-db"))))

(deftest test-fixture-seq
  (let [fixtures (fixture-seq fixture-dir)]
    (is (= 5 (count fixtures)))
    (let [fixture (first fixtures)]
      (is (= (file fixture-dir "continents.edn")
             (:file fixture)))
      (is (= :continents (:table fixture))))
    (let [fixture (nth fixtures 2)]
      (is (= (file fixture-dir "twitter" "tweets.edn")
             (:file fixture)))
      (is (= :twitter.tweets (:table fixture)))))
  (let [fixtures (fixture-seq (resource "db/test-db/fixtures"))]
    (is (= 5 (count fixtures)))
    (let [fixture (first fixtures)]
      (is (= (.getAbsoluteFile (file "test-resources/db/test-db/fixtures/continents.edn"))
             (:file fixture)))
      (is (= :continents (:table fixture))))
    (let [fixture (nth fixtures 2)]
      (is (= (.getAbsoluteFile (file "test-resources/db/test-db/fixtures/twitter/tweets.edn"))
             (:file fixture)))
      (is (= :twitter.tweets (:table fixture))))))

(deftest test-serial-sequence
  (with-backends [db]
    (is (= @(serial-sequence db {:table "continents" :name "id"})
           [{:pg_get_serial_sequence "public.continents_id_seq"}]))
    (is (= @(serial-sequence db {:table :continents :name :id})
           [{:pg_get_serial_sequence "public.continents_id_seq"}]))))

(deftest test-load-fixtures
  (with-backends [db]
    (delete-fixtures db (tables fixture-dir))
    (let [fixtures (load-fixtures db fixture-dir)]
      (is (= 5 (count fixtures))))))

(deftest test-read-fixture
  (with-backends [db]
    (delete-continents db)
    (let [fixture (read-fixture db :continents fixture-file)]
      (is (= :continents (:table fixture)))
      (is (= fixture-file (:file fixture)))
      (is (= 7 (count (:records fixture)))))))

(deftest test-slurp-rows
  (let [records (slurp-rows fixture-file)]
    (is (= 7 (count records)))))

(deftest test-reset-serials
  (with-backends [db]
    (reset-serials db :continents)
    (reset-serials db :twitter.users)))

(deftest test-tables
  (is (= [:continents :countries :twitter.tweets :twitter.tweets-users :twitter.users]
         (tables fixture-dir))))

(deftest test-write-fixture
  (with-backends [db]
    (let [fixture (write-fixture db :continents "/tmp/test-write-fixture/continents.edn")]
      (is (= :continents (:table fixture)))
      (is (= "/tmp/test-write-fixture/continents.edn" (:file fixture)))
      (is (= 7 (:records fixture)))
      (is (= (slurp (str fixture-dir "/continents.edn"))
             (slurp "/tmp/test-write-fixture/continents.edn"))))
    (let [fixture (write-fixture db :twitter.tweets "/tmp/test-write-fixture/twitter/tweets.edn")]
      (is (= :twitter.tweets (:table fixture)))
      (is (= "/tmp/test-write-fixture/twitter/tweets.edn" (:file fixture)))
      (is (= 23 (:records fixture)))
      (is (= (set (map :id (read-string (slurp (str fixture-dir "/twitter/tweets.edn")))))
             (set (map :id (read-string (slurp "/tmp/test-write-fixture/twitter/tweets.edn")))))))))
