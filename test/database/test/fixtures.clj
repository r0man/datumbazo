(ns database.test.fixtures
  (:import org.joda.time.DateTime)
  (:use [clojure.string :only (lower-case upper-case)]
        [clj-time.coerce :only (to-date-time to-timestamp)]
        [migrate.core :only (defmigration)]
        clojure.test
        database.columns
        database.core
        database.fixtures
        database.registry
        database.serialization
        database.tables
        database.util
        database.test
        validation.core))

;; (clojure.pprint/pprint
;;  (query-only
;;   (select (entity :photo-thumbnails)
;;           (join :photos
;;                 (= :photo-thumbnails.photo-id :photos.id))
;;           (fields :photo-thumbnails.id :photo-thumbnails.width :photo-thumbnails.heigth)
;;           (shift-fields :photos :photo [:id :title]))))

;; (select (entity :photo-thumbnails)
;;         (join :photos (= :photo-thumbnails.photo-id :photos.id))
;;         (fields :photo-thumbnails.id :photo-thumbnails.width :photo-thumbnails.heigth)
;;         (shift-fields :photos :photo [:id :title]))

;; (sql-only
;;  (-> (select* :photo-thumbnails)
;;      (fields :photo-thumbnails.id :photo-thumbnails.width :photo-thumbnails.heigth)
;;      ;; (shift-fields :photos :photo [:id :title])
;;      (join :photos (= :photo-thumbnails.photo-id :photos.id))
;;      (exec)))

;; PHOTO THUMBNAILS

(def street-art-berlin-small
  (make-photo-thumbnail
   :id 1
   :photo-id (:id street-art-berlin)
   :url "http://www.flickr.com/photos/eric795/6818646318/sizes/s/in/photostream"
   :width 240
   :heigth 180))

(def street-art-berlin-medium
  (make-photo-thumbnail
   :id 2
   :photo-id (:id street-art-berlin)
   :url "http://www.flickr.com/photos/eric795/6818646318/sizes/m/in/photostream"
   :width 500
   :heigth 375))

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

(database-test test-languages-entity
  (let [entity languages-entity]
    (is (= :id (:pk entity)))
    (is (= "languages" (:name entity)))
    (is (= "languages" (:table entity)))
    (is (nil? (:aliases entity)))
    (is (nil? (:db entity)))
    (is (empty? (:prepares entity)))
    (is (= {} (:rel entity)))
    (is (every? fn? (:transforms entity)))
    (is (= [:updated-at :created-at :iso-639-2 :iso-639-1 :family :name :id] (:fields entity)))))

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

(database-test test-insert-photo
  (is (thrown? Exception (insert-photo nil)))
  (is (thrown? Exception (insert-photo {})))
  (let [record (insert-photo street-art-berlin)]
    (is (number? (:id record)))
    (is (not (= -1 (:id record)))) ; filtered, because serial
    (is (= "Street Art Berlin" (:title record)))
    (is (nil? (:taken-at record))) ; can be nil
    (is (instance? DateTime (:created-at record)))
    (is (instance? DateTime (:updated-at record)))))

(database-test test-insert-photo-thumbnail
  (is (thrown? Exception (insert-photo-thumbnail nil)))
  (is (thrown? Exception (insert-photo-thumbnail {})))
  (insert-photo street-art-berlin)
  (let [record (insert-photo-thumbnail street-art-berlin-small)]
    (is (number? (:id record)))
    (is (not (= -1 (:id record)))) ; filtered, because serial
    (is (number? (:photo-id record)))
    (is (= "http://www.flickr.com/photos/eric795/6818646318/sizes/s/in/photostream" (:url record)))
    (is (= 240 (:width record)))
    (is (= 180 (:heigth record)))
    (is (instance? DateTime (:created-at record)))
    (is (instance? DateTime (:updated-at record))))
  (let [record (insert-photo-thumbnail street-art-berlin-medium)]
    (is (number? (:id record)))
    (is (not (= -1 (:id record)))) ; filtered, because serial
    (is (number? (:photo-id record)))
    (is (= "http://www.flickr.com/photos/eric795/6818646318/sizes/m/in/photostream" (:url record)))
    (is (= 500 (:width record)))
    (is (= 375 (:heigth record)))
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
