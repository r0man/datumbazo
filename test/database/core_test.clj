(ns database.core-test
  (:use clojure.test
        database.core))

(deftest test-with-connection
  (are [spec]
    (with-connection spec
      (is true))
    :test-database
    "jdbc:postgresql://localhost/test")
  (are [spec]
    (is (thrown? IllegalArgumentException (with-connection spec (is false))))
    nil {} :unknown-database))