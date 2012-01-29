(ns database.test.serialization
  (:use [clojure.string :only (lower-case)]
        clojure.test
        database.columns
        database.tables
        database.serialization
        database.test.examples))

(deftest test-deserialize-column
  (let [column (make-column :iso-639-1 :varchar :length 2)]
    (is (= "DE" (deserialize-column column "DE"))))
  (let [column (make-column :iso-639-1 :varchar :length 2 :deserialize lower-case)]
    (is (nil? (deserialize-column column nil)))
    (is (= (:iso-639-1 (deserialize-column column "DE"))))))

(deftest test-deserialize-record
  (let [language (deserialize-record language-table {:name "German" :iso-639-1 "DE" :iso-639-2 "DEU"})]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "DE" (:iso-639-1 language)))
    (is (= "DEU" (:iso-639-2 language)))))

(deftest test-serialize-column
  (let [column (make-column :iso-639-1 :varchar :length 2)]
    (is (= "DE" (serialize-column column "DE"))))
  (let [column (make-column :iso-639-1 :varchar :length 2 :serialize lower-case)]
    (is (nil? (serialize-column column nil)))
    (is (= "de" (serialize-column column "DE")))))

(deftest test-serialize-record
  (let [language (serialize-record language-table {:name "German" :iso-639-1 "DE" :iso-639-2 "DEU" :not-existing "column"})]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "de" (:iso-639-1 language)))
    (is (= "deu" (:iso-639-2 language)))
    (is (not (contains? (set (keys language)) :not-existing)))))
