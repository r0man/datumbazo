(ns datumbazo.db.postgresql.associations-test
  (:require [clojure.test :refer [deftest is]]
            [datumbazo.db.postgresql.associations :as a]
            [datumbazo.test :refer :all]))

;; Belongs to

(deftest test-belongs-to
  (with-test-db [db db]
    (setup-test-table db :authors)
    (setup-test-table db :books)
    (let [books (books db)
          authors (a/belongs-to db books :books :authors)]
      (is (= authors (for [book books
                           :let [author (author-by-book db book)]]
                       (-> (select-keys author [:id])
                           (not-empty))))))))

;; Has many

(deftest test-has-many
  (with-test-db [db db]
    (setup-test-table db :authors)
    (setup-test-table db :books)
    (let [authors (authors db)
          books (a/has-many db authors :authors :books)]
      (is (not (empty? books)))
      (is (= (for [books books]
               (set books))
             (for [author authors]
               (set (map #(select-keys % [:id])
                         (books-by-author db author)))))))))

(deftest test-has-many-limit
  (with-test-db [db db]
    (setup-test-table db :authors)
    (setup-test-table db :books)
    (let [authors (authors db)]
      (is (= (a/has-many db authors :authors :books)
             [[{:id 4} {:id 1}]
              [{:id 5} {:id 2}]
              [{:id 3} {:id 8} {:id 6}]]))
      (is (= (a/has-many db authors :authors :books {:limit 0})
             [[] [] []]))
      (is (= (a/has-many db authors :authors :books {:limit 1})
             [[{:id 4}]
              [{:id 5}]
              [{:id 3}]]))
      (is (= (a/has-many db authors :authors :books {:limit 2})
             [[{:id 4} {:id 1}]
              [{:id 5} {:id 2}]
              [{:id 3} {:id 8}]])))))

(deftest test-has-many-offset
  (with-test-db [db db]
    (setup-test-table db :authors)
    (setup-test-table db :books)
    (let [authors (authors db)]
      (is (= (a/has-many db authors :authors :books {:offset 0})
             [[{:id 4} {:id 1}]
              [{:id 5} {:id 2}]
              [{:id 3} {:id 8} {:id 6}]]))
      (is (= (a/has-many db authors :authors :books {:offset 1})
             [[{:id 1}]
              [{:id 2}]
              [{:id 8} {:id 6}]]))
      (is (= (a/has-many db authors :authors :books {:offset 2})
             [[]
              []
              [{:id 6}]])))))

(deftest test-has-many-limit-offset
  (with-test-db [db db]
    (setup-test-table db :authors)
    (setup-test-table db :books)
    (let [authors (authors db)]
      (is (= (a/has-many db authors :authors :books {:limit 2 :offset 1})
             [[{:id 1}]
              [{:id 2}]
              [{:id 8} {:id 6}]]))
      (is (= (a/has-many db authors :authors :books {:limit 1 :offset 1})
             [[{:id 1}]
              [{:id 2}]
              [{:id 8}]])))))

(deftest test-has-many-order-by
  (with-test-db [db db]
    (setup-test-table db :authors)
    (setup-test-table db :books)
    (let [authors (authors db)]
      (is (= (->> {:order-by `(order-by :books.name)}
                  (a/has-many db authors :authors :books))
             [[{:id 1} {:id 4}]
              [{:id 2} {:id 5}]
              [{:id 3} {:id 6} {:id 8}]]))
      (is (= (->> {:order-by `(order-by (desc :books.name))}
                  (a/has-many db authors :authors :books))
             [[{:id 4} {:id 1}]
              [{:id 5} {:id 2}]
              [{:id 8} {:id 6} {:id 3}]])))))

;; Has one

(deftest test-has-one
  (with-test-db [db db]
    (setup-test-table db :suppliers)
    (setup-test-table db :accounts)
    (let [suppliers (suppliers db)
          accounts (a/has-one db suppliers :suppliers :accounts)]
      (is (not (empty? accounts)))
      (is (= accounts (for [supplier suppliers]
                        (-> (account-by-supplier db supplier)
                            (select-keys [:id])
                            (not-empty))))))))

;; Has and belongs to many

(deftest test-has-and-belongs-to-many
  (with-test-db [db db]
    (setup-test-table db :patients)
    (setup-test-table db :physicians)
    (setup-test-table db :appointments)
    (is (= (a/has-and-belongs-to-many
            db (patients db) :patients :physicians
            {:join-table :appointments})
           [[{:id 201} {:id 202} {:id 203}]
            [{:id 201} {:id 202}]
            [{:id 203}]]))))

(deftest test-has-and-belongs-to-many-limit-offset
  (with-test-db [db db]
    (setup-test-table db :patients)
    (setup-test-table db :physicians)
    (setup-test-table db :appointments)
    (is (= (a/has-and-belongs-to-many
            db (patients db) :patients :physicians
            {:join-table :appointments
             :limit 1
             :offset 1})
           [[{:id 202}]
            [{:id 202}]
            []]))))

(deftest test-has-and-belongs-to-many-order-by
  (with-test-db [db db]
    (setup-test-table db :patients)
    (setup-test-table db :physicians)
    (setup-test-table db :appointments)
    (is (= (->> {:join-table :appointments
                 :order-by `(order-by (desc :physicians.name))}
                (a/has-and-belongs-to-many db (patients db) :patients :physicians))
           [[{:id 203} {:id 202} {:id 201}]
            [{:id 202} {:id 201}]
            [{:id 203}]]))))
