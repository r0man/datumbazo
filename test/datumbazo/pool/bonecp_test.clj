(ns datumbazo.pool.bonecp-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as db]
            [datumbazo.pool.core :refer [db-pool]]
            [datumbazo.test :refer :all])
  (:import com.jolbox.bonecp.BoneCPDataSource
           java.sql.Connection))

(def db-url
  (str "bonecp:" (:postgresql connections)))

(deftest test-db-pool
  (with-open [pool (db-pool (db/parse-url db-url))]
    (is (instance? BoneCPDataSource pool))))

(deftest test-connection
  (with-open [pool (db-pool (db/parse-url db-url))
              conn (.getConnection pool)]
    (is (instance? Connection conn))))
