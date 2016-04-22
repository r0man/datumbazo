(ns datumbazo.pool.hikaricp-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as db]
            [datumbazo.pool.core :refer [db-pool]]
            [datumbazo.test :refer :all])
  (:import com.zaxxer.hikari.HikariDataSource))

(def db-url
  (str "hikaricp:" (:postgresql connections)))

(deftest test-db-pool
  (let [pool (db-pool (db/parse-url db-url))]
    (is (instance? HikariDataSource pool))))

;; (deftest test-select-pool-c3p0
;;   (with-test-db [db "c3p0:postgresql://tiger:scotch@localhost:5432/datumbazo"]
;;     (is (instance? Connection (:connection db)))
;;     (is (instance? ComboPooledDataSource (:datasource db)))
;;     (is (= @(select db [(as 1 :a)])
;;            [{:a 1}]))))

