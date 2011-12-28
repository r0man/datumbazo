(ns database.test.connection
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:use [leiningen.env.core :only (with-environment)]
        clojure.test
        database.connection
        database.test))

(deftest test-current-db-spec
  (load-environments)
  (let [spec (current-db-spec)]
    (is (= "org.postgresql.Driver" (:classname spec)))
    (is (= "database" (:password spec)))
    (is (= "//localhost/database_development" (:subname spec)))
    (is (= "postgresql" (:subprotocol spec)))
    (is (= "database" (:user spec))))
  (with-environment :test
    (let [spec (current-db-spec)]
      (is (= "org.postgresql.Driver" (:classname spec)))
      (is (= "database" (:password spec)))
      (is (= "//localhost/database_test" (:subname spec)))
      (is (= "postgresql" (:subprotocol spec)))
      (is (= "database" (:user spec))))))

(deftest test-db-spec
  (let [spec (current-db-spec)]
    (is (= spec (db-spec {:database spec})))
    (is (= spec (db-spec {:databases {:default spec}})))
    (is (= spec (db-spec {:databases {:analytic spec}} :analytic)))))

(deftest test-make-pool
  (let [pool (make-pool (current-db-spec))]
    (is (instance? ComboPooledDataSource pool))))
