(ns datumbazo.vendor-test
  (:require [clojure.test :refer :all]
            [datumbazo.db :refer [parse-url]]
            [datumbazo.vendor :refer :all]))

(deftest test-subname-mysql
  (let [spec (parse-url "mysql://tiger:scotch@localhost/datumbazo?profileSQL=true")]
    (is (= "//localhost/datumbazo?profileSQL=true" (subname spec)))))

(deftest test-subname-oracle
  (let [spec (parse-url "oracle://tiger:scotch@localhost/datumbazo")]
    (is (= ":tiger/scotch@localhost:datumbazo" (subname spec)))))

(deftest test-subname-postgresql
  (let [spec (parse-url "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true")]
    (is (= "//localhost:5432/datumbazo?ssl=true" (subname spec)))))

(deftest test-subname-sqlite
  (let [spec (parse-url "sqlite://tmp/datumbazo.sqlite")]
    (is (= "//tmp/datumbazo.sqlite" (subname spec)))))

(deftest test-subname-sqlserver
  (let [spec (parse-url "sqlserver://tiger:scotch@localhost/datumbazo")]
    (is (= "//localhost;database=datumbazo;user=tiger;password=scotch" (subname spec)))))

(deftest test-subname-vertica
  (let [spec (parse-url "vertica://tiger:scotch@localhost/datumbazo")]
    (is (= "//localhost/datumbazo" (subname spec)))))
