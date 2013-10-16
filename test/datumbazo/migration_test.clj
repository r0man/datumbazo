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

(deftest test-make-migration
  (let [migration (make-migration root-dir "Create continents")]
    (is (= "Create continents" (:name migration)))
    (is (= (str root-dir "/deploy/create-continents.sql") (:deploy migration)))
    (is (= (str root-dir "/revert/create-continents.sql") (:revert migration)))
    (is (= (str root-dir "/test/create-continents.sql") (:test migration)))))

(deftest test-create-migration
  (let [migration (make-migration root-dir "Create continents")]
    (create-migration migration)
    (is (file-exists? (:deploy migration)))
    (is (file-exists? (:revert migration)))
    (is (file-exists? (:test migration)))))
