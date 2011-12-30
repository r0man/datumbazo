(ns database.test.examples
  (:use [clojure.string :only (lower-case)]
        clojure.test
        database.core
        database.tables
        database.test))

(deftable languages
  [[:id :serial :primary-key true]
   [:name :text :unique? true :not-null? true]
   [:family :text :not-null? true]
   [:iso-639-1 :varchar :length 2 :unique? true :not-null? true :serialize #'lower-case]
   [:iso-639-2 :varchar :length 3 :unique? true :not-null? true :serialize #'lower-case]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]])

(deftable photo-thumbnails
  [[:id :serial :primary-key true]
   [:title :text]
   [:description :text]
   [:taken-at :timestamp-with-time-zone]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]])

(def photo-thumbnails (find-table :photo-thumbnails))

(def languages (find-table :languages))
(def german {:name "German" :family "Indo-European" :iso-639-1 "DE" :iso-639-2 "DEU"})

(deftest test-deserialize-language
  (let [language (deserialize-language {:name "German" :iso-639-1 "DE" :iso-639-2 "DEU"})]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "DE" (:iso-639-1 language)))
    (is (= "DEU" (:iso-639-2 language)))))

(deftest test-serialize-language
  (let [language (serialize-language {:name "German" :iso-639-1 "DE" :iso-639-2 "DEU"})]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "de" (:iso-639-1 language)))
    (is (= "deu" (:iso-639-2 language)))))

;; (database-test test-insert-language
;;   (create-table languages)
;;   (let [record (insert-language german)]
;;     (is (number? (:id record)))
;;     (is (= (:name record)))
;;     (is (= "Indo-European" (:family record)))
;;     (is (= "de" (:iso-639-1 record)))
;;     (is (= "deu" (:iso-639-2 record)))
;;     (is (instance? Timestamp (:created-at record)))
;;     (is (instance? Timestamp (:updated-at record)))))

(load-environments)
