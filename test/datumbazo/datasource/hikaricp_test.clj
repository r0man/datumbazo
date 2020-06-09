(ns datumbazo.datasource.hikaricp-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.datasource :refer [datasource]]
            [datumbazo.test :refer :all]
            [datumbazo.util :refer [parse-url]])
  (:import com.zaxxer.hikari.HikariDataSource
           java.sql.Connection))

(def db-url
  (str "hikaricp:" (:postgresql connections)))

(deftest test-datasource
  (with-open [pool (datasource (parse-url db-url))]
    (is (instance? HikariDataSource pool))))

(deftest test-connection
  (with-drivers [db db {:pool :hikaricp}]
    (is (nil? (sql/connection db)))
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db)))
      @(sql/select db [1]))))

(deftest test-get-connection
  (with-open [pool (datasource (parse-url db-url))
              conn (.getConnection pool)]
    (is (instance? Connection conn))))
