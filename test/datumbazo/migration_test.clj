(ns datumbazo.migration-test
  (:use clojure.test
        datumbazo.migration
        datumbazo.test))

(deftest test-migration-deploy-path
  (are [name expected]
    (is (= expected (migration-deploy-path "." name)))
    nil nil
    "create continents" "./deploy/create-continents.sql"))

(deftest test-migration-revert-path
  (are [name expected]
    (is (= expected (migration-revert-path "." name)))
    nil nil
    "create continents" "./revert/create-continents.sql"))

(deftest test-migration-test-path
  (are [name expected]
    (is (= expected (migration-test-path "." name)))
    nil nil
    "create continents" "./test/create-continents.sql"))
