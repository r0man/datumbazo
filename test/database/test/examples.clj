(ns database.test.examples
  (:import org.joda.time.DateTime)
  (:use [clojure.string :only (lower-case upper-case)]
        [clj-time.coerce :only (to-date-time to-timestamp)]
        [migrate.core :only (defmigration)]
        [korma.core :exclude (join table)]
        clojure.test
        database.core
        database.registry
        database.serialization
        database.tables
        database.test
        validation.core))

(defn language-url [language]
  (if (:id language)
    (format "http://example.com/languages/%s-%s" (:id language) (:name language))))

(defvalidate language
  (presence-of :name)
  (min-length-of :name 2)
  (max-length-of :name 32)
  (presence-of :family)
  (min-length-of :family 2)
  (max-length-of :family 32)
  (presence-of :iso-639-1)
  (exact-length-of :iso-639-1 2)
  (presence-of :iso-639-2)
  (exact-length-of :iso-639-2 3))

(defvalidate user
  (presence-of :nick)
  (min-length-of :nick 2)
  (max-length-of :nick 32)
  (presence-of :email)
  (min-length-of :nick 2)
  (max-length-of :nick 256))

(deftable languages
  [[:id :serial :primary-key? true]
   [:name :text :unique? true :not-null? true]
   [:family :text :not-null? true]
   [:iso-639-1 :varchar :length 2 :unique? true :not-null? true :serialize #'lower-case]
   [:iso-639-2 :varchar :length 3 :unique? true :not-null? true :serialize #'lower-case]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]]
  :url language-url
  :validate validate-language!)

(deftable photo-thumbnails
  [[:id :serial :primary-key? true]
   [:title :text]
   [:description :text]
   [:taken-at :timestamp-with-time-zone]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]])

(deftable users
  [[:id :serial :primary-key? true]
   [:nick :text :unique? true :not-null? true]
   [:email :text :unique? true :not-null? true :serialize #'lower-case]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]]
  :validate validate-user!)

(defmigration "2011-12-31T10:00:00"
  "Create the languages table."
  (create-table (table :languages))
  (drop-table (table :languages)))

(defmigration "2011-12-31T11:00:00"
  "Create the photo thumbnails table."
  (create-table (table :photo-thumbnails))
  (drop-table (table :photo-thumbnails)))

(defmigration "2012-08-03T20:00:00"
  "Create the users table."
  (create-table (table :users))
  (drop-table (table :users)))

(def language-table (find-table :languages))

(def bodhi
  (make-user
   :id 1
   :nick "Bodhi"
   :email "bodhi@example.com"))

(def german
  (make-language
   :id 1
   :name "German"
   :family "Indo-European"
   :iso-639-1 "DE"
   :iso-639-2 "DEU"))

(def spanish
  (make-language
   :id 2
   :name "Spanish"
   :family "Indo-European"
   :iso-639-1 "ES"
   :iso-639-2 "ESP"))

(def photo-thumbnails (find-table :photo-thumbnails))

(deftest test-deserialize-language
  (is (= nil (deserialize-language nil)))
  (is (= {} (deserialize-language {})))
  (let [language (deserialize-language german)]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "DE" (:iso-639-1 language)))
    (is (= "DEU" (:iso-639-2 language)))
    (is (= (language-url language) (:url language)))))

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

(database-test test-languages*
  (is (empty? (languages)))
  (let [german (save-language german)
        spanish (save-language spanish)]
    (is (= [german spanish] (exec (languages*))))))

(database-test test-languages
  (is (empty? (languages)))
  (let [german (save-language german)
        spanish (save-language spanish)]
    (is (= [german spanish] (languages)))
    (is (= [german] (languages :page 1 :per-page 1)))
    (is (= [spanish] (languages :page 2 :per-page 1)))))

(database-test test-languages-by-family
  (let [record (insert-language german)]
    (is (empty? (languages-by-family nil)))
    (is (empty? (languages-by-family "")))
    (is (empty? (languages-by-family "unknown")))
    (is (= [record] (languages-by-family (:family record))))
    (is (= [record] (languages-by-family (:family record) :page 1 :per-page 2)))
    (is (empty? (languages-by-family (:family record) :page 2 :per-page 2)))))

(database-test test-insert-language
  (is (thrown? Exception (insert-language nil)))
  (is (thrown? Exception (insert-language {})))
  (let [record (insert-language german)]
    (is (number? (:id record)))
    (is (not (= -1 (:id record)))) ; filtered, because serial
    (is (= "German" (:name record)))
    (is (= "Indo-European" (:family record)))
    (is (= "de" (:iso-639-1 record)))
    (is (= "deu" (:iso-639-2 record)))
    (is (= (language-url record) (:url record)))
    (is (instance? DateTime (:created-at record)))
    (is (instance? DateTime (:updated-at record)))))

(database-test test-insert-user
  (is (thrown? Exception (insert-user nil)))
  (is (thrown? Exception (insert-user {})))
  (let [record (insert-user bodhi)]
    (is (number? (:id record)))
    (is (not (= -1 (:id record)))) ; filtered, because serial
    (is (= "Bodhi" (:nick record)))
    (is (= "bodhi@example.com" (:email record)))
    (is (instance? DateTime (:created-at record)))
    (is (instance? DateTime (:updated-at record)))))

(database-test test-update-language
  (is (thrown? Exception (update-language nil)))
  (is (thrown? Exception (update-language {})))
  (let [record (update-language (assoc (insert-language german) :name "Deutsch"))]
    (is (number? (:id record)))
    (is (= "Deutsch" (:name record)))
    (is (= "Indo-European" (:family record)))
    (is (= "de" (:iso-639-1 record)))
    (is (= "deu" (:iso-639-2 record)))
    (is (= (language-url record) (:url record)))
    (is (instance? DateTime (:created-at record)))
    (is (instance? DateTime (:updated-at record)))
    (is (= record (update-language record)))))

(database-test test-delete-language
  (is (nil? (delete-language nil)))
  (is (nil? (delete-language {})))
  (let [language (insert-language german)]
    (is (= language (delete-language language)))
    (is (= (language-url language) (:url language)))
    (insert-language german)))

(database-test test-save-language
  (is (thrown? Exception (save-language nil)))
  (is (thrown? Exception (save-language {})))
  (let [language (save-language german)]
    (is (pos? (:id language)))
    (is (=  language (save-language language)))))

(deftest test-serialize-language
  (is (= nil (serialize-language nil)))
  (is (= {} (serialize-language {})))
  (let [language (serialize-language (assoc german :url "http://germany.de"))]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "de" (:iso-639-1 language)))
    (is (= "deu" (:iso-639-2 language)))
    (is (not (contains? (set (keys language)) :url)))))
