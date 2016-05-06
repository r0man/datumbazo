(ns datumbazo.pool.c3p0-test
  (:require [clojure.test :refer :all]
            [datumbazo.pool.core :refer [db-pool]]
            [datumbazo.test :refer :all]
            [datumbazo.util :refer [parse-url]])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource
           java.sql.Connection))

(def db-url
  (str "c3p0:" (:postgresql connections)))

(deftest test-db-pool
  (with-open [pool (db-pool (parse-url db-url))]
    (is (instance? ComboPooledDataSource pool))))

(deftest test-connection
  (with-open [pool (db-pool (parse-url db-url))
              conn (.getConnection pool)]
    (is (instance? Connection conn))))
