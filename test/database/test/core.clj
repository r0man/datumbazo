(ns database.test.core
  (:import java.sql.Timestamp)
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
  (create-table languages)
  (let [column (make-column :x :integer)]
    (is (= column (add-column languages column)))))

(database-test test-create-table-with-languages
  (is (instance? database.tables.Table (create-table languages)))
  (is (thrown? Exception (create-table languages))))

(database-test test-create-table-with-photo-thumbnails
  (is (instance? database.tables.Table (create-table photo-thumbnails)))
  (is (thrown? Exception (create-table photo-thumbnails))))

(deftest test-deftable
  (let [table (find-table :languages)
        fields (database.test.examples.Language/getBasis)]
    (is (= (count (:columns table)) (count fields)))
    (is (= (map column-symbol (:columns table)) fields))))

(database-test test-delete-record
  (create-table languages)
  (let [record (insert-record languages german)]
    (delete-record languages record)
    (insert-record languages german)))

(database-test test-delete-rows
  (create-table languages)
  (let [language (insert-record languages german)]
    (delete-rows languages)
    (insert-record languages german)
    (delete-rows languages ["name = ?" (:name language)])
    (insert-record languages german)))

(database-test test-drop-table
  (create-table languages)
  (is (drop-table languages))
  (is (drop-table languages :if-exists true))
  (is (thrown? Exception (drop-table languages))))

(database-test test-insert-record
  (create-table languages)
  (let [record (insert-record languages german)]
    (is (number? (:id record)))
    (is (= (:name record)))
    (is (= "Indo-European" (:family record)))
    (is (= "de" (:iso-639-1 record)))
    (is (= "deu" (:iso-639-2 record)))
    (is (instance? Timestamp (:created-at record)))
    (is (instance? Timestamp (:updated-at record)))))
