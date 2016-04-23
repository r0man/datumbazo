(ns datumbazo.driver.core-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [datumbazo.core :refer [with-db]]
            [datumbazo.driver.core :as d]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(deftest test-with-connection
  (with-db [db (:postgresql connections)]
    (d/with-connection [db db]
      (let [connection (d/connection db)]
        (is (instance? Connection connection))
        (d/with-connection [db db]
          (is (= (d/connection db) connection)))))))

(deftest test-with-connection-rollback
  (with-db [db (:postgresql connections) {:rollback? true}]
    (let [connection (d/connection db)]
      (is (instance? Connection (d/connection db)))
      (d/with-connection [db db]
        (is (= (d/connection db) connection))))))
