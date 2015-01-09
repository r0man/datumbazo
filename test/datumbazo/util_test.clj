(ns datumbazo.util-test
  (:refer-clojure :exclude [distinct group-by])
  (:use clojure.test
        datumbazo.util
        datumbazo.core
        datumbazo.test))

(deftest test-absolute-path
  (is (string? (absolute-path ""))))

(deftest current-user-test
  (is (= (System/getenv "USER") (current-user))))

(deftest test-compact-map
  (is (= {} (compact-map {})))
  (is (= {:a 1} (compact-map {:a 1})))
  (is (= {:a 1} (compact-map {:a 1 :b nil})))
  (is (= {:a 1 :c {:d 1}} (compact-map {:a 1 :b nil :c {:d 1 :e nil}}))))

(deftest test-edn-file?
  (is (not (edn-file? "NOT-EXISTING")))
  (is (not (edn-file? "src")))
  (is (edn-file? "test-resources/db/test-db/fixtures/continents.edn")))

(deftest test-edn-file-seq
  (let [files (edn-file-seq "test-resources/db/test-db/fixtures")]
    (is (not (empty? files)))
    (is (every? edn-file? files))))

(deftest test-format-server
  (are [server expected]
    (is (= expected (format-server server)))
    {:host "example.com"}
    "example.com"
    {:host "example.com" :port 123}
    "example.com:123"))

(deftest test-path-split
  (is (= [""] (path-split nil)))
  (is (= [""] (path-split "")))
  (is (= ["x" "y"] (path-split "x/y"))))

(deftest test-path-replace
  (is (= "database" (path-replace "src/database" "src"))))

(deftest test-parse-params
  (are [params expected]
    (is (= expected (parse-params params)))
    nil {}
    "" {}
    "a=1" {:a "1"}
    "a=1&b=2" {:a "1" :b "2"}))

(deftest test-parse-schema
  (are [schema expected]
    (do (is (= expected (parse-schema schema)))
        (is (= expected (parse-schema (qualified-name schema)))))
    :public {:name :public}))

(deftest test-qualified-name
  (are [arg expected]
    (is (= expected (qualified-name arg)))
    nil ""
    "" ""
    "continents" "continents"
    :continents "continents"
    :public.continents "public.continents"))

(deftest test-slurp-sql
  (let [stmts (slurp-sql "test-resources/stmts-simple.sql")]
    (is (= ["DROP TABLE x" "DROP TABLE y"] stmts)))
  (let [stmts (slurp-sql "test-resources/stmts-raster.sql")]
    (is (= 2 (count stmts)))))

(deftest test-exec-sql-file
  (with-test-db [db]
    @(drop-table db [:akw-dirpwsfc-2013-02-10t06]
       (if-exists true))
    (exec-sql-file db "test-resources/stmts-raster.sql")))
