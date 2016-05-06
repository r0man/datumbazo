(ns datumbazo.connection-test
  (:require [clojure.test :refer :all]
            [datumbazo.db :refer [with-db]]
            [datumbazo.connection :refer :all]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(deftest test-with-connection
  (with-db [db (:postgresql connections)]
    (with-connection [db db]
      (let [connection (connection db)]
        (is (instance? Connection connection))))))

