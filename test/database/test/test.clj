(ns database.test.test
  (:use [leiningen.env.core :only (current-environment)]
        clojure.test
        database.test))

(database-test test-database-test
  (load-environments)
  (is (re-matches #".*/database_test" (:subname (:database (current-environment))))))

(deftest test-load-environments
  (is (map? (load-environments))))
