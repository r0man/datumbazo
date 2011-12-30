(ns database.test.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.internal :as internal])
  (:use clojure.test
        database.core
        database.columns
        database.tables
        database.postgis
        database.test
        database.test.examples))

(database-test test-add-column
  (let [table (create-table test-table)
        column (make-column :x :integer)]
    (is (= column (add-column table column)))))

(database-test test-create-table-with-photo-thumbnails
  (let [table (find-table :photo-thumbnails)]
    (is (instance? database.tables.Table (create-table table)))
    (is (thrown? Exception (create-table table)))))

(database-test test-create-with-continents
  (let [table (find-table :continents)]
    (is (instance? database.tables.Table (create-table table)))
    (is (thrown? Exception (create-table table)))))

(deftest test-deftable
  (let [table (find-table :continents)
        fields (database.test.examples.Continent/getBasis)]
    (is (= (count (:columns table)) (count fields)))
    (is (= (map column-symbol (:columns table)) fields))))

(database-test test-delete-rows
  (let [table (create-table test-table)]
    (delete-rows table)
    (delete-rows table ["1 = 1"])))

(database-test test-drop-table
  (let [table (find-table :photo-thumbnails)]
    (is (create-table table))
    (is (drop-table table))
    (is (drop-table table :if-exists true))
    (is (thrown? Exception (drop-table table)))))
