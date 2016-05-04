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
        (is (instance? Connection connection))))))
