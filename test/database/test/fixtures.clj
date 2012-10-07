(ns database.test.fixtures
  (:use clojure.test
        database.fixtures))

(deftest test-fixture-file?
  (is (not (fixture-file? "NOT-EXISTING")))
  (is (not (fixture-file? "resources/db/fixtures/test-db")))
  (is (fixture-file? "resources/db/fixtures/test-db/continents.clj")))

(deftest test-fixtures-in-directory
  (let [fixtures (fixtures-in-directory "resources/db/fixtures/test-db")]
    (is (= 1 (count fixtures)))))

(deftest test-fixtures-on-classpath
  (let [fixtures (fixtures-on-classpath "db/fixtures/test-db")]
    (is (= 1 (count fixtures)))))

(deftest test-slurp-fixtures
  (let [records (slurp-fixtures "resources/db/fixtures/test-db/continents.clj")]
    (is (= 7 (count records)))))