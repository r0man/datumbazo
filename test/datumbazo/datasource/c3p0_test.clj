(ns datumbazo.datasource.c3p0-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.datasource :refer [datasource]]
            [datumbazo.test :refer :all]
            [datumbazo.util :refer [parse-url]])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource
           java.sql.Connection))

(def db-url
  (str "c3p0:" (:postgresql connections)))

(deftest test-datasource
  (with-open [pool (datasource (parse-url db-url))]
    (is (instance? ComboPooledDataSource pool))))

(deftest test-connection
  (with-drivers [db db {:pool :c3p0}]
    (is (nil? (sql/connection db)))
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db))))))

(deftest test-get-connection
  (with-open [pool (datasource (parse-url db-url))
              conn (.getConnection pool)]
    (is (instance? Connection conn))))
