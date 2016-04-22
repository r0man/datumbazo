(ns datumbazo.pool.c3p0-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as db]
            [datumbazo.pool.core :refer [db-pool]]
            [datumbazo.test :refer :all])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(def db-url
  (str "c3p0:" (:postgresql connections)))

(deftest test-db-pool
  (let [pool (db-pool (db/parse-url db-url))]
    (is (instance? ComboPooledDataSource pool))))

;; (deftest test-select-pool-c3p0
;;   (with-test-db [db "c3p0:postgresql://tiger:scotch@localhost:5432/datumbazo"]
;;     (is (instance? Connection (:connection db)))
;;     (is (instance? ComboPooledDataSource (:datasource db)))
;;     (is (= @(select db [(as 1 :a)])
;;            [{:a 1}]))))

