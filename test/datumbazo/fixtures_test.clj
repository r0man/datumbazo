(ns datumbazo.fixtures-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.fixtures :as fixtures]
            [datumbazo.test :refer :all]))

(def fixture-dir
  "test-resources/db/test-db/fixtures")

(def fixture-file
  (str fixture-dir "/continents.edn"))

(deftest test-enable-triggers
  (with-backends [db]
    (is (= [{:count 0}] (fixtures/enable-triggers db :continents)))
    (is (= [{:count 0}] (fixtures/enable-triggers db :twitter.users)))))

(deftest test-disable-triggers
  (with-backends [db]
    (is (= [{:count 0}] (fixtures/disable-triggers db :continents)))
    (is (= [{:count 0}] (fixtures/enable-triggers db :twitter.users)))))

(deftest test-delete-fixtures
  (with-backends [db]
    (is (nil? (fixtures/delete-fixtures db (fixtures/tables fixture-dir))))))

(deftest test-dump-fixtures
  (with-backends [db]
    (fixtures/dump-fixtures db "/tmp/test-dump-fixtures" [:continents :twitter.users])
    (is (.exists (io/file "/tmp/test-dump-fixtures/continents.edn")))
    (is (.exists (io/file "/tmp/test-dump-fixtures/twitter/users.edn")))))

(deftest test-fixture-path
  (is (= "db/test-db/fixtures" (fixtures/fixture-path "test-db"))))

(deftest test-fixtures
  (is (= (fixtures/fixture-seq (io/resource "db/test-db/fixtures"))
         (fixtures/fixtures "test-db"))))

(deftest test-fixture-seq
  (let [fixtures (fixtures/fixture-seq fixture-dir)]
    (is (= 5 (count fixtures)))
    (let [fixture (first fixtures)]
      (is (= (io/file fixture-dir "continents.edn")
             (:file fixture)))
      (is (= :continents (:table fixture))))
    (let [fixture (nth fixtures 2)]
      (is (= (io/file fixture-dir "twitter" "tweets.edn")
             (:file fixture)))
      (is (= :twitter.tweets (:table fixture)))))
  (let [fixtures (fixtures/fixture-seq (io/resource "db/test-db/fixtures"))]
    (is (= 5 (count fixtures)))
    (let [fixture (first fixtures)]
      (is (= (.getAbsoluteFile (io/file "test-resources/db/test-db/fixtures/continents.edn"))
             (:file fixture)))
      (is (= :continents (:table fixture))))
    (let [fixture (nth fixtures 2)]
      (is (= (.getAbsoluteFile (io/file "test-resources/db/test-db/fixtures/twitter/tweets.edn"))
             (:file fixture)))
      (is (= :twitter.tweets (:table fixture)))))
  (testing ":only option"
    (let [only #{:continents :twitter.users}
          fixtures (fixtures/fixture-seq fixture-dir {:only only})]
      (is (= 2 (count fixtures)))
      (is (= only (set (map :table fixtures)))))))

(deftest test-serial-sequence
  (with-backends [db]
    (is (= @(fixtures/serial-sequence db {:table "continents" :name "id"})
           [{:pg-get-serial-sequence "public.continents_id_seq"}]))
    (is (= @(fixtures/serial-sequence db {:table :continents :name :id})
           [{:pg-get-serial-sequence "public.continents_id_seq"}]))))

(deftest test-load-fixtures
  (with-backends [db]
    (fixtures/delete-fixtures db (fixtures/tables fixture-dir))
    (let [fixtures (fixtures/load-fixtures db fixture-dir)]
      (is (= 5 (count fixtures))))))

(deftest test-load-fixtures-only
  (with-backends [db]
    (fixtures/delete-fixtures db (fixtures/tables fixture-dir))
    (let [fixtures (fixtures/load-fixtures db fixture-dir {:only [:continents]})]
      (is (= 1 (count fixtures))))))

(deftest test-read-fixture
  (with-backends [db]
    @(sql/truncate db [:continents]
       (sql/cascade true))
    (let [fixture (fixtures/read-fixture db :continents fixture-file)]
      (is (= :continents (:table fixture)))
      (is (= fixture-file (:file fixture)))
      (is (= 7 (count (:records fixture)))))))

(deftest test-slurp-rows
  (let [records (fixtures/slurp-rows fixture-file)]
    (is (= 7 (count records)))))

(deftest test-reset-serials
  (with-backends [db]
    (is (nil? (fixtures/reset-serials db :continents)))
    (is (nil? (fixtures/reset-serials db :twitter.users)))))

(deftest test-tables
  (is (= [:continents :countries :twitter.tweets :twitter.tweets-users :twitter.users]
         (fixtures/tables fixture-dir))))

(deftest test-write-fixture
  (with-backends [db]
    (let [fixture (fixtures/write-fixture db :continents "/tmp/test-write-fixture/continents.edn")]
      (is (= :continents (:table fixture)))
      (is (= "/tmp/test-write-fixture/continents.edn" (:file fixture)))
      (is (= 7 (:records fixture)))
      (is (= (slurp (str fixture-dir "/continents.edn"))
             (slurp "/tmp/test-write-fixture/continents.edn"))))
    (let [fixture (fixtures/write-fixture db :twitter.tweets "/tmp/test-write-fixture/twitter/tweets.edn")]
      (is (= :twitter.tweets (:table fixture)))
      (is (= "/tmp/test-write-fixture/twitter/tweets.edn" (:file fixture)))
      (is (= 23 (:records fixture)))
      (is (= (set (map :id (read-string (slurp (str fixture-dir "/twitter/tweets.edn")))))
             (set (map :id (read-string (slurp "/tmp/test-write-fixture/twitter/tweets.edn")))))))))
