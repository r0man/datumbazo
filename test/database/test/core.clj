(ns database.test.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.internal :as internal])
  (:use clojure.test
        database.core
        database.tables
        database.test
        database.test.examples))

(database-test test-create-table
  (let [table (find-table :photo-thumbnails)]
    (is (instance? database.tables.Table (create-table table)))
    (is (thrown? Exception (create-table table)))))

(database-test test-drop-table
  (let [table (find-table :photo-thumbnails)]
    (is (create-table table))
    (is (drop-table table))
    (is (drop-table table :if-exists true))
    (is (thrown? Exception (drop-table table)))))

(load-environments)
