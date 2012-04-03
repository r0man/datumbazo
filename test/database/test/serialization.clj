(ns database.test.serialization
  (:import java.sql.Timestamp)
  (:use [clojure.string :only (lower-case)]
        [clj-time.core :only (date-time)]
        clojure.test
        database.columns
        database.tables
        database.serialization
        database.fixtures))

(deftest test-deserialize-column
  (let [column (make-column :created-at :timestamp-with-time-zone)]
    (is (nil? (deserialize-column column nil)))
    (is (= (date-time 1970 1 1) (deserialize-column column (Timestamp. 0))))
    (is (= (date-time 1970 1 1) (deserialize-column column "1970-01-01T00:00:00.000Z"))))
  (let [column (make-column :iso-639-1 :varchar :length 2)]
    (is (= "DE" (deserialize-column column "DE"))))
  (let [column (make-column :iso-639-1 :varchar :length 2 :deserialize lower-case)]
    (is (nil? (deserialize-column column nil)))
    (is (= "de" (deserialize-column column "DE")))))

(deftest test-deserialize-record
  (let [language (deserialize-record :languages {:name "German" :iso-639-1 "DE" :iso-639-2 "DEU"})]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "DE" (:iso-639-1 language)))
    (is (= "DEU" (:iso-639-2 language)))))

(deftest test-serialize-column
  (let [column (make-column :created-at :timestamp-with-time-zone)]
    (is (nil? (deserialize-column column nil)))
    (is (= (Timestamp. 0) (serialize-column column (date-time 1970 1 1))))
    (is (= (Timestamp. 0) (serialize-column column "1970-01-01T00:00:00.000Z"))))
  (let [column (make-column :iso-639-1 :varchar :length 2)]
    (is (= "DE" (serialize-column column "DE"))))
  (let [column (make-column :iso-639-1 :varchar :length 2 :serialize lower-case)]
    (is (nil? (serialize-column column nil)))
    (is (= "de" (serialize-column column "DE")))))

(deftest test-serialize-record
  (let [language (serialize-record :languages {:name "German" :iso-639-1 "DE" :iso-639-2 "DEU" :not-existing "column"})]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "de" (:iso-639-1 language)))
    (is (= "deu" (:iso-639-2 language)))
    (is (not (contains? (set (keys language)) :not-existing)))))
