(ns database.test.serialization
  (:use [clojure.string :only (lower-case)]
        clojure.test
        database.columns
        database.tables
        database.serialization
        database.test.examples))

(deftest test-deserialize-column
  (let [column (make-column :iso-639-1 :varchar :length 2)]
    (is (= "DE" (:iso-639-1 (deserialize-column column {:iso-639-1 "DE"})))))
  (let [column (make-column :iso-639-1 :varchar :length 2 :deserialize lower-case)]
    (is (nil? (:iso-639-1 (deserialize-column column {}))))
    (is (= "de" (:iso-639-1 (deserialize-column column {:iso-639-1 "DE"}))))))

(deftest test-deserialize-row
  (let [language (deserialize-row languages {:name "German" :iso-639-1 "DE" :iso-639-2 "DEU"})]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "DE" (:iso-639-1 language)))
    (is (= "DEU" (:iso-639-2 language)))))

(deftest test-serialize-column
  (let [column (make-column :iso-639-1 :varchar :length 2)]
    (is (= "DE" (:iso-639-1 (serialize-column column {:iso-639-1 "DE"})))))
  (let [column (make-column :iso-639-1 :varchar :length 2 :serialize lower-case)]
    (is (nil? (:iso-639-1 (serialize-column column {}))))
    (is (= "de" (:iso-639-1 (serialize-column column {:iso-639-1 "DE"}))))))

(deftest test-serialize-row
  (let [language (serialize-row languages {:name "German" :iso-639-1 "DE" :iso-639-2 "DEU"})]
    (is (map? language))
    (is (= "German" (:name language)))
    (is (= "de" (:iso-639-1 language)))
    (is (= "deu" (:iso-639-2 language)))))
