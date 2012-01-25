(ns database.test.connection
  (:use clojure.test
        database.connection
        database.test))

(deftest test-connection-spec
  (let [spec (connection-spec)]
    (is (map? spec))))

(deftest test-connection
  (let [connection (connection)]
    (is (map? connection))
    (is (instance? javax.sql.DataSource (:datasource connection)))))

(database-test test-naming-strategy
  (let [strategy (naming-strategy)]
    (is (fn? (:keys strategy)))
    (is (not (= identity (:keys strategy))))
    (is (fn? (:fields strategy)))
    (is (not (= identity (:fields strategy))))))
