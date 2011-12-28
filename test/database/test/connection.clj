(ns database.test.connection
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:use [leiningen.env.core :only (with-environment)]
        clojure.test
        database.connection
        database.test))

(def test-spec
  {:classname "org.postgresql.Driver"
   :subprotocol "postgresql"
   :subname "//localhost/database_test"
   :user "database"
   :password "database"})

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
  (is (= test-spec (db-spec {:database test-spec})))
  (is (= test-spec (db-spec {:databases {:default test-spec}})))
  (is (= test-spec (db-spec {:databases {:analytic test-spec}} :analytic))))

(deftest test-make-pool
  (let [pool (make-pool (current-db-spec))]
    (is (instance? ComboPooledDataSource pool))))
