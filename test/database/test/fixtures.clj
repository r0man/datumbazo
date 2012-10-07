(ns database.test.fixtures
  (:use clojure.test
        database.fixtures))

(deftest test-fixtures-in-directory
  (let [fixtures (fixtures-in-directory "resources/db/fixtures/test-db")]
    (is (= 1 (count fixtures)))))

(deftest test-fixtures-on-classpath
  (let [fixtures (fixtures-on-classpath "db/fixtures/test-db")]
    (is (= 1 (count fixtures)))))