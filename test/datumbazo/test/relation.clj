(ns datumbazo.test.relation
  (:refer-clojure :exclude [group-by])
  (:use clojure.test
        datumbazo.relation
        sqlingvo.core
        datumbazo.test)
  (:import datumbazo.relation.Relation))

(database-test test-count
  (is (= 1 (count (Relation. (select 1))))))

(deftest test-equiv
  (is (.equiv (Relation. (select 1))
              (Relation. (select 1))))
  (is (not (.equiv (Relation. (select 1))
                   (Relation. (select 2))))))

(database-test test-seq
  (is (= [{:?column? 1}] (seq (Relation. (select 1))))))

(deftest test-to-string
  (is (= "[\"SELECT 1\"]" (str (Relation. (select 1))))))

(comment

  (use 'datumbazo.connection)
  (use 'datumbazo.fixtures)

  (with-connection :test-db
    (println (seq (Relation. (-> (select *) (from :continents))))))

  (with-connection :test-db
    (seq (Relation. (-> (select :id :name :created-at)
                        (from :continents)
                        (order-by [:id :name])
                        (limit 1)
                        (offset 1)))))

  (with-connection :test-db
    (load-fixtures "resources/db/test-db/fixtures"))

  (with-connection :test-db
    (seq (Relation. (select (select 1) (select "x")))))
  )
