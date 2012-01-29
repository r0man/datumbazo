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
    (is (= column (add-column language-table column)))))

(database-test test-create-table-with-languages
  (drop-table language-table)
  (is (instance? database.tables.Table (create-table language-table)))
  (is (thrown? Exception (create-table language-table))))

(database-test test-create-table-with-photo-thumbnails
  (drop-table photo-thumbnails)
  (is (instance? database.tables.Table (create-table photo-thumbnails)))
  (is (thrown? Exception (create-table photo-thumbnails))))

(database-test test-delete-record
  (is (nil? (delete-record language-table nil)))
  (is (nil? (delete-record language-table {})))
  (let [record (insert-record language-table german)]
    (is (= record (delete-record language-table record)))
    (insert-record language-table german)))

(database-test test-delete-all
  (let [language (insert-record language-table german)]
    (is (= 1 (delete-all language-table)))
    (is (= 0 (delete-all language-table)))))

(database-test test-delete-where
  (let [language (insert-record language-table german)]
    (is (= 1 (delete-where language-table ["name = ?" (:name language)])))
    (is (= 0 (delete-where language-table ["name = ?" (:name language)])))
    (insert-record language-table german)))

(database-test test-drop-table
  (is (drop-table language-table))
  (is (drop-table language-table :if-exists true))
  (is (thrown? Exception (drop-table language-table))))

(database-test test-find-by-column
  (let [language (insert-record language-table german)]
    (is (empty? (find-by-column language-table :name nil)))
    (is (empty? (find-by-column language-table :name "NOT-EXISTING")))
    (is (= [language] (find-by-column language-table :name (:name language))))
    (is (= [language] (find-by-column language-table :created-at (:created-at language))))))

(database-test test-reload-record
  (is (nil? (reload-record language-table {})))
  (let [language (insert-record language-table german)]
    (is (= language (reload-record language-table language)))))

(database-test test-insert-record
  (is (nil? (insert-record language-table nil)))
  (is (nil? (insert-record language-table {})))
  (let [record (insert-record language-table german)]
    (is (number? (:id record)))
    (is (= "German" (:name record)))
    (is (= "Indo-European" (:family record)))
    (is (= "de" (:iso-639-1 record)))
    (is (= "deu" (:iso-639-2 record)))
    (is (instance? Timestamp (:created-at record)))
    (is (instance? Timestamp (:updated-at record))))
  ;; (is (thrown? Exception (insert-record language-table german)))
  )

(database-test test-update-record
  (is (nil? (update-record language-table nil)))
  (is (nil? (update-record language-table {})))
  (is (nil? (update-record language-table german)))
  (let [record (update-record language-table (assoc (insert-record language-table german) :name "Deutsch"))]
    (is (number? (:id record)))
    (is (= "Deutsch" (:name record)))
    (is (= "Indo-European" (:family record)))
    (is (= "de" (:iso-639-1 record)))
    (is (= "deu" (:iso-639-2 record)))
    (is (instance? Timestamp (:created-at record)))
    (is (instance? Timestamp (:updated-at record)))
    (is (= record (update-record language-table record)))))

(database-test test-save-record
  (let [language (save-record :languages german)]
    (is (pos? (:id language)))
    (is (= language (save-record :languages language)))))

(database-test test-select-by-column
  (let [language (insert-record language-table german)]
    (is (empty? (exec (select-by-column language-table :name nil))))
    (is (empty? (exec (select-by-column language-table :name "NOT-EXISTING"))))
    (is (= [language] (exec (select-by-column language-table :name (:name language)))))
    (is (= [language] (exec (select-by-column language-table :created-at (:created-at language)))))))

(deftest test-table
  (is (table? (table :languages)))
  (is (= :languages (:name (table :languages))))
  (is (= (table :languages) (table (table :languages)))))

(deftest test-unique-key-clause
  (are [record expected]
    (is (= expected (unique-key-clause language-table record)))
    {} (pred-or)
    {:id 1} (pred-or {:id 1})
    ;; TODO: How?
    ;; {:id 1 :iso-639-1 "de"} (pred-or {:id 1} {:iso-639-1 "de"})
    ;; {:id 1 :iso-639-1 "de" :undefined "column"} {:id 1 :iso-639-1 "de"}
    ))
