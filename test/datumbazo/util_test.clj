(ns datumbazo.util-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all]
            [datumbazo.util :as util]))

(deftest test-absolute-path
  (is (string? (util/absolute-path ""))))

(deftest current-user-test
  (is (= (System/getenv "USER") (util/current-user))))

(deftest test-compact-map
  (is (= {} (util/compact-map {})))
  (is (= {:a 1} (util/compact-map {:a 1})))
  (is (= {:a 1} (util/compact-map {:a 1 :b nil})))
  (is (= {:a 1 :c {:d 1}} (util/compact-map {:a 1 :b nil :c {:d 1 :e nil}}))))

(deftest test-class-symbol
  (are [name expected]
      (= (util/class-symbol {:name name}) expected)
    "tweets" 'Tweet
    "hash-tags" 'HashTag))

(deftest test-edn-file?
  (is (not (util/edn-file? "NOT-EXISTING")))
  (is (not (util/edn-file? "src")))
  (is (util/edn-file? "test-resources/db/test-db/fixtures/continents.edn")))

(deftest test-edn-file-seq
  (let [files (util/edn-file-seq "test-resources/db/test-db/fixtures")]
    (is (not (empty? files)))
    (is (every? util/edn-file? files))))

(deftest test-format-server
  (are [server expected] (= expected (util/format-server server))
    {:server-name "example.com"}
    "example.com"
    {:server-name "example.com" :server-port 123}
    "example.com:123"))

(deftest test-path-split
  (is (= [""] (util/path-split nil)))
  (is (= [""] (util/path-split "")))
  (is (= ["x" "y"] (util/path-split "x/y"))))

(deftest test-path-replace
  (is (= "database" (util/path-replace "src/database" "src"))))

(deftest test-parse-params
  (are [params expected] (= expected (util/parse-params params))
    nil {}
    "" {}
    "a=1" {:a "1"}
    "a=1&b=2" {:a "1" :b "2"}))

(deftest test-parse-schema
  (are [schema expected]
      (do (is (= expected (util/parse-schema schema)))
          (is (= expected (util/parse-schema (util/qualified-name schema)))))
    :public {:name :public}))

(deftest test-qualified-name
  (are [arg expected]
      (= expected (util/qualified-name arg))
    nil ""
    "" ""
    "continents" "continents"
    :continents "continents"
    :public.continents "public.continents"))

(deftest test-slurp-sql
  (let [stmts (util/slurp-sql "test-resources/stmts-simple.sql")]
    (is (= ["DROP TABLE x" "DROP TABLE y"] stmts)))
  (let [stmts (util/slurp-sql "test-resources/stmts-raster.sql")]
    (is (= 3 (count stmts)))))

(deftest test-exec-sql-file
  (with-backends [db]
    (try
      @(sql/drop-table db [:akw_dirpwsfc_2013_02_10t06]
         (sql/if-exists true))
      (is (util/exec-sql-file db "test-resources/stmts-raster.sql"))
      (finally
        @(sql/drop-table db [:akw_dirpwsfc_2013_02_10t06])))))

(deftest test-exec-sql-file-select
  (with-backends [db]
    (let [file "/tmp/test-exec-sql-file-select.sql"]
      (spit file "SELECT * from countries;")
      (is (= (util/exec-sql-file db file) file)))))

(deftest test-library-loaded?
  (is (util/library-loaded? :joda-time))
  (is (util/library-loaded? :postgis))
  (is (util/library-loaded? :postgresql)))

(deftest test-sql-stmt-seq
  (with-open [reader (io/reader "test-resources/stmts-multiline.sql")]
    (let [stmts (util/sql-stmt-seq reader)]
      (is (seq? stmts))
      (is (= stmts
             ["SELECT 1;"
              "CREATE TABLE x ( id INTEGER );"
              "SELECT * FROM x;"
              "DROP x;"])))))

(deftest test-execute-one-all-batch
  (with-backends [db]
    (let [query (sql/select db [:*]
                  (sql/from :continents)
                  (sql/order-by :name))]
      (is (= (util/fetch-batch query) @query)))))

(deftest test-parse-url
  (doseq [url [nil "" "x"]]
    (is (thrown? clojure.lang.ExceptionInfo (util/parse-url url))))
  (let [db (util/parse-url "mysql://localhost:5432/datumbazo")]
    (is (nil? (:pool db)))
    (is (= "localhost" (:server-name db)))
    (is (= 5432 (:server-port db)))
    (is (= "datumbazo" (:name db)))
    (is (nil? (:query-params db)))
    (is (= :mysql (:scheme db))))
  (let [db (util/parse-url "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2")]
    (is (nil? (:pool db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "localhost" (:server-name db)))
    (is (= 5432 (:server-port db)))
    (is (= "datumbazo" (:name db)))
    (is (= {:a "1" :b "2"} (:query-params db)))
    (is (= :postgresql (:scheme db))))
  (let [db (util/parse-url "c3p0:postgresql://localhost/datumbazo")]
    (is (= :c3p0 (:pool db)))
    (is (= "localhost" (:server-name db)))
    (is (nil? (:server-port db)))
    (is (= "datumbazo" (:name db)))
    (is (nil? (:query-params db)))
    (is (= :postgresql (:scheme db)))))

(deftest test-format-url
  (let [url "postgresql://tiger:scotch@localhost/datumbazo?a=1&b=2"]
    (is (= (util/format-url (util/parse-url url)) url)))
  (let [url "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2"]
    (is (= (util/format-url (util/parse-url "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2"))
           "postgresql://tiger:scotch@localhost/datumbazo?a=1&b=2"))))
