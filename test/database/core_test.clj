(ns database.core-test
  (:use clojure.test
        database.core))

(deftable models
  "The weather models table."
  (schema :weather)
  (column :id :serial)
  (column :name :text))

(deftable variables
  "The weather variables table."
  (schema :weather)
  (column :id :serial)
  (column :name :text))

(deftable models-variables
  "The join table between weather models and variables."
  (schema :weather)
  (column :model-id :integer)
  (column :variable-id :integer))

(deftest test-make-table
  (let [table (make-table :continents)]
    (is (= :continents (:name table))))
  (is (= (make-table :continents) (make-table "continents"))))

(deftest test-deftable
  (let [table models]
    (is (= :weather (:schema table)))
    (is (= :models (:name table)))
    (is (= [:id :name] (:columns table)))
    (is (= {:name {:name :name}
            :id {:name :id}}
           (:column table)))
    (is (= "The weather models table." (:doc table)))))

;; (clojure.pprint/pprint models)

(deftest test-with-connection
  (are [spec]
    (with-connection spec
      (is true))
    :test-database
    "jdbc:postgresql://localhost/test")
  (are [spec]
    (is (thrown? IllegalArgumentException (with-connection spec (is false))))
    nil {} :unknown-database))
