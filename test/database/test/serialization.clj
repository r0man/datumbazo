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

(deftest test-serialize-column
  (let [column (make-column :iso-639-1 :varchar :length 2)]
    (is (= "DE" (:iso-639-1 (serialize-column column {:iso-639-1 "DE"})))))
  (let [column (make-column :iso-639-1 :varchar :length 2 :serialize lower-case)]
    (is (nil? (:iso-639-1 (serialize-column column {}))))
    (is (= "de" (:iso-639-1 (serialize-column column {:iso-639-1 "DE"}))))))

(deftest test-deserialize-row
  (let [table (make-table :languages [[:iso-639-1 :varchar :length 2 :deserialize lower-case]])]
    (is (= {} (deserialize-row table {})))
    (is (= {:iso-639-1 "de"} (deserialize-row table {:iso-639-1 "DE"})))))

(deftest atest-serialize-row
  (let [table (make-table :languages [[:iso-639-1 :varchar :length 2 :serialize lower-case]])]
    (is (= {} (serialize-row table {})))
    (is (= {:iso-639-1 "de"} (serialize-row table {:iso-639-1 "DE"})))))
