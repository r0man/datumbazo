(ns datumbazo.create-table-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [datumbazo.core :refer :all]
            [datumbazo.test :refer :all]))

(defn create-measurement
  "Create the measurement table."
  [db]
  @(create-table db :measurement
     (column :city-id :int :not-null? true)
     (column :logdate :date :not-null? true)
     (column :peaktmp :int)
     (column :unitsales :int)))

(deftest test-create-table
  (with-backends [db db]
    (is (= (create-measurement db)
           [{:count 0}]))))

(deftest test-create-table-inherits-check
  (with-backends [db db]
    (create-measurement db)
    (is (= @(create-table db :measurement-y2006m02
              (check '(and (>= :logdate "2006-02-01")
                           (< :logdate "2006-03-01")))
              (inherits :measurement))
           [{:count 0}]))))

(deftest test-create-table-inherits-check-multiple
  (with-backends [db db]
    (create-measurement db)
    (is (= @(create-table db :measurement-y2006m02
              (check '(>= :logdate "2006-02-01"))
              (check '(< :logdate "2006-03-01"))
              (inherits :measurement))
           [{:count 0}]))))
