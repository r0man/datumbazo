(ns datumbazo.pool.bonecp-test
  (:require [clojure.test :refer :all]
            [datumbazo.pool.core :refer [db-pool]]
            [datumbazo.test :refer :all]
            [datumbazo.util :refer [parse-url]])
  (:import com.jolbox.bonecp.BoneCPDataSource
           java.sql.Connection))

(def db-url
  (str "bonecp:" (:postgresql connections)))

(deftest test-db-pool
  (with-open [pool (db-pool (parse-url db-url))]
    (is (instance? BoneCPDataSource pool))))

(deftest test-connection
  (with-open [pool (db-pool (parse-url db-url))
              conn (.getConnection pool)]
    (is (instance? Connection conn))))
