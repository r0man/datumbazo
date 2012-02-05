(ns database.test.core
  (:import java.sql.Timestamp org.postgresql.util.PSQLException)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.internal :as internal])
  (:use [korma.core :exclude (join table)]
        [korma.sql.fns :only (pred-or)]
        clojure.test
        database.core
        database.columns
        database.tables
        database.postgis
        database.test
        database.test.examples))

(database-test test-add-column
  (let [column (make-column :x :integer)]
    (is (= column (add-column :languages column)))))

(database-test test-create-table-with-languages
  (drop-table :languages)
  (is (instance? database.tables.Table (create-table :languages)))
  (is (thrown? Exception (create-table :languages))))

(database-test test-create-table-with-photo-thumbnails
  (drop-table photo-thumbnails)
  (is (instance? database.tables.Table (create-table photo-thumbnails)))
  (is (thrown? Exception (create-table photo-thumbnails))))

(database-test test-delete-record
  (is (nil? (delete-record :languages nil)))
  (is (nil? (delete-record :languages {})))
  (let [record (insert-record :languages german)]
    (is (= record (delete-record :languages record)))
    (insert-record :languages german)))

(database-test test-delete-all
  (let [language (insert-record :languages german)]
    (is (= 1 (delete-all :languages)))
    (is (= 0 (delete-all :languages)))))

(database-test test-delete-where
  (let [language (insert-record :languages german)]
    (is (= 1 (delete-where :languages ["name = ?" (:name language)])))
    (is (= 0 (delete-where :languages ["name = ?" (:name language)])))
    (insert-record :languages german)))

(database-test test-drop-table
  (is (drop-table :languages))
  (is (drop-table :languages :if-exists true))
  (is (thrown? Exception (drop-table :languages))))

(database-test test-find-by-column
  (let [language (insert-record :languages german)]
    (is (empty? (find-by-column :languages :name nil)))
    (is (empty? (find-by-column :languages :name "NOT-EXISTING")))
    (is (= [language] (find-by-column :languages :name (:name language))))
    (is (= [language] (find-by-column :languages :created-at (:created-at language))))))

(database-test test-reload-record
  (is (nil? (reload-record :languages {})))
  (let [language (insert-record :languages german)]
    (is (= language (reload-record :languages language)))))

(database-test test-insert-record
  (is (nil? (insert-record :languages nil)))
  (is (nil? (insert-record :languages {})))
  (let [record (insert-record :languages german)]
    (is (number? (:id record)))
    (is (= "German" (:name record)))
    (is (= "Indo-European" (:family record)))
    (is (= "de" (:iso-639-1 record)))
    (is (= "deu" (:iso-639-2 record)))
    (is (= "http://example.com/languages/1-German" (:url record)))
    (is (instance? Timestamp (:created-at record)))
    (is (instance? Timestamp (:updated-at record))))
  ;; (is (thrown? Exception (insert-record :languages german)))
  )

(database-test test-update-record
  (is (nil? (update-record :languages nil)))
  (is (nil? (update-record :languages {})))
  (is (nil? (update-record :languages german)))
  (let [record (update-record :languages (assoc (insert-record :languages german) :name "Deutsch"))]
    (is (number? (:id record)))
    (is (= "Deutsch" (:name record)))
    (is (= "Indo-European" (:family record)))
    (is (= "de" (:iso-639-1 record)))
    (is (= "deu" (:iso-639-2 record)))
    (is (= "http://example.com/languages/1-Deutsch" (:url record)))
    (is (instance? Timestamp (:created-at record)))
    (is (instance? Timestamp (:updated-at record)))
    (is (= record (update-record :languages record)))))

(database-test test-save-record
  (let [language (save-record :languages german)]
    (is (pos? (:id language)))
    (is (= language (save-record :languages language)))))

(database-test test-select-by-column
  (let [language (insert-record :languages german)]
    (is (empty? (exec (select-by-column :languages :name nil))))
    (is (empty? (exec (select-by-column :languages :name "NOT-EXISTING"))))
    (is (= [language] (exec (select-by-column :languages :name (:name language)))))
    (is (= [language] (exec (select-by-column :languages :created-at (:created-at language)))))))

(deftest test-table
  (is (table? (table :languages)))
  (is (= :languages (:name (table :languages))))
  (is (= (table :languages) (table (table :languages)))))

(deftest test-unique-key-clause
  (are [record expected]
    (is (= expected (unique-key-clause :languages record)))
    {} (pred-or)
    {:id 1} (pred-or {:id 1})
    ;; TODO: How?
    ;; {:id 1 :iso-639-1 "de"} (pred-or {:id 1} {:iso-639-1 "de"})
    ;; {:id 1 :iso-639-1 "de" :undefined "column"} {:id 1 :iso-639-1 "de"}
    ))
