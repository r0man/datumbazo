(ns datumbazo.datasource.bonecp-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.datasource :refer [datasource]]
            [datumbazo.test :refer :all]
            [datumbazo.util :refer [parse-url]])
  (:import [com.jolbox.bonecp BoneCPDataSource ConnectionHandle]))

(def db-url
  (str "bonecp:" (:postgresql connections)))

(deftest test-datasource
  (with-open [pool (datasource (parse-url db-url))]
    (is (instance? BoneCPDataSource pool))))

(deftest test-connection
  (with-drivers [db db {:pool :bonecp}]
    (is (nil? (sql/connection db)))
    (sql/with-connection [db db]
      (is (instance? ConnectionHandle (sql/connection db)))
      @(sql/select db [1]))))

(deftest test-get-connection
  (with-open [pool (datasource (parse-url db-url))
              conn (.getConnection pool)]
    (is (instance? ConnectionHandle conn))))
