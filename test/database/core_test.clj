(ns database.core-test
  (:require [slingshot.slingshot :refer [throw+ try+]])
  (:use clojure.test
        database.core))

(deftest test-with-connection
  (with-connection :test-database
    (is true))
  (try+
   (with-connection :unknown-database (is false))
   (catch [:type :database.core/connection-not-found] {:keys [name]}
     (is (= :unknown-database name)))))