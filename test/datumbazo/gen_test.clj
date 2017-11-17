(ns datumbazo.gen-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.generators :as gen]
            [datumbazo.gen :as gens]
            [datumbazo.test :refer :all]))

(deftest test-column-int
  (with-backends [db]
    (is (->> (gens/column db :continents.id)
             (gen/sample)
             (every? int?)))))

(deftest test-column-text
  (with-backends [db]
    (is (->> (gens/column db :continents.name)
             (gen/sample)
             (every? string?)))))

(deftest test-row
  (with-backends [db]
    (is (->> (gens/row db :continents)
             (gen/sample)
             (every? map?)))))
