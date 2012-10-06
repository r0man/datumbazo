(ns database.test.connection
  (:require [clojure.java.jdbc :as jdbc])
  (:use database.connection
        clojure.test))

(deftest test-with-connection
  (with-connection :bs-database
    (is (jdbc/connection)))
  (is (thrown? IllegalArgumentException (with-connection :unknown))))

(deftest test-wrap-connection
  ((wrap-connection
    (fn [request] (is (jdbc/connection))) :bs-database)
   {}))