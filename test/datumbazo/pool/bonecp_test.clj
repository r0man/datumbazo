(ns datumbazo.pool.bonecp-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as db]
            [datumbazo.pool.core :refer [db-pool]]
            [datumbazo.test :refer :all])
  (:import com.jolbox.bonecp.BoneCPDataSource))

(def db-url
  (str "bonecp:" (:postgresql connections)))

(deftest test-new-pool
  (let [pool (db-pool (db/parse-url db-url))]
    (is (instance? BoneCPDataSource pool))))

;; (deftest test-select-pool-bonecp
;;   (with-test-db [db "bonecp:postgresql://tiger:scotch@localhost:5432/datumbazo"]
;;     (is (instance? Connection (:connection db)))
;;     (is (instance? BoneCPDataSource (:datasource db)))
;;     (is (= @(select db [(as 1 :a)])
;;            [{:a 1}]))))
