(ns database.test.examples
  (:import java.sql.Timestamp)
  (:use [clojure.string :only (lower-case upper-case)]
        [migrate.core :only (defmigration)]
        clojure.test
        database.core
        database.util
        database.tables
        database.test))

(deftable languages
  [[:id :serial :primary-key? true :serialize #'parse-int]
   [:name :text :unique? true :not-null? true]
   [:family :text :not-null? true]
   [:iso-639-1 :varchar :length 2 :unique? true :not-null? true :serialize #'lower-case]
   [:iso-639-2 :varchar :length 3 :unique? true :not-null? true :serialize #'lower-case]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]])

(deftable photo-thumbnails
  [[:id :serial :primary-key? true]
   [:title :text]
   [:description :text]
   [:taken-at :timestamp-with-time-zone]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]])

(defmigration "2011-12-31T10:00:00"
  "Create the languages table."
  (create-table (table :languages))
  (drop-table (table :languages)))

(defmigration "2011-12-31T11:00:00"
  "Create the photo thumbnails table."
  (create-table (table :photo-thumbnails))
  (drop-table (table :photo-thumbnails)))

(def languages (find-table :languages))

(def german
  (make-language
   :id 1
   :name "German"
   :family "Indo-European"
   :iso-639-1 "DE"
   :iso-639-2 "DEU"))

(def photo-thumbnails (find-table :photo-thumbnails))

(deftest test-deserialize-language
  (is (= nil (deserialize-language nil)))
  (is (= {} (deserialize-language {})))
  (let [language (deserialize-language german)]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "DE" (:iso-639-1 language)))
    (is (= "DEU" (:iso-639-2 language)))))

(database-test test-language-by-id
  (let [record (insert-language german)]
    (is (nil? (language-by-id nil)))
    (is (nil? (language-by-id 0)))
    (is (= record (language-by-id (:id record))))
    (is (= record (language-by-id (str (:id record)))))))

(database-test test-language-by-iso-639-1
  (let [record (insert-language german)]
    (is (nil? (language-by-iso-639-1 nil)))
    (is (nil? (language-by-iso-639-1 "")))
    (is (nil? (language-by-iso-639-1 "xx")))
    (is (= record (language-by-iso-639-1 (:iso-639-1 record))))
    (is (= record (language-by-iso-639-1 (upper-case (:iso-639-1 record)))))))

(database-test test-language-by-iso-639-2
  (let [record (insert-language german)]
    (is (nil? (language-by-iso-639-2 nil)))
    (is (nil? (language-by-iso-639-2 "")))
    (is (nil? (language-by-iso-639-2 "xxx")))
    (is (= record (language-by-iso-639-2 (:iso-639-2 record))))
    (is (= record (language-by-iso-639-2 (upper-case (:iso-639-2 record)))))))

(database-test test-languages-by-family
  (let [record (insert-language german)]
    (is (empty? (languages-by-family nil)))
    (is (empty? (languages-by-family "")))
    (is (empty? (languages-by-family "unknown")))
    (is (= [record] (languages-by-family (:family record))))))

(database-test test-insert-language
  (is (nil? (insert-language nil)))
  (is (nil? (insert-language {})))
  (let [record (insert-language (assoc german :id nil))]
    (is (number? (:id record)))
    (is (not (= -1 (:id record)))) ; filtered, because serial
    (is (= "German" (:name record)))
    (is (= "Indo-European" (:family record)))
    (is (= "de" (:iso-639-1 record)))
    (is (= "deu" (:iso-639-2 record)))
    (is (instance? Timestamp (:created-at record)))
    (is (instance? Timestamp (:updated-at record)))))

(database-test test-update-language
  (is (nil? (update-language nil)))
  (is (nil? (update-language {})))
  (let [record (update-language (assoc (insert-language german) :name "Deutsch"))]
    (is (number? (:id record)))
    (is (= "Deutsch" (:name record)))
    (is (= "Indo-European" (:family record)))
    (is (= "de" (:iso-639-1 record)))
    (is (= "deu" (:iso-639-2 record)))
    (is (instance? Timestamp (:created-at record)))
    (is (instance? Timestamp (:updated-at record)))
    (is (= record (update-language record)))))

(database-test test-delete-language
  (is (nil? (delete-language nil)))
  (is (nil? (delete-language {})))
  (let [language (insert-language german)]
    (is (= language (delete-language language)))
    (insert-language german)))

(deftest test-serialize-language
  (is (= nil (serialize-language nil)))
  (is (= {} (serialize-language {})))
  (let [language (serialize-language (assoc german :url "http://germany.de"))]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "de" (:iso-639-1 language)))
    (is (= "deu" (:iso-639-2 language)))
    (is (not (contains? (set (keys language)) :url)))))
