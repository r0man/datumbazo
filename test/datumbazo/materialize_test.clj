(ns datumbazo.materialize-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]))

(def my-db
  (sql/new-db "postgresql://tiger:scotch@localhost:6875/materialize"))

(deftest test-select
  (is (= [{:?column? 1, :?column?_2 2, :?column?_3 3}]
         @(sql/select my-db [1 2 3]))))

(deftest test-explore

  (doseq [view [:key-sums :lhs :pseudo-source]]
    @(sql/drop-view my-db view
       (sql/if-exists true)))

  (is (= [{:count 0}]
         @(sql/create-materialized-view my-db :pseudo-source [:key :value]
            (sql/values [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["c" 6] ["c" 7]]))))

  (is (= [{:key "a", :value 1}
          {:key "a", :value 2}
          {:key "a", :value 3}
          {:key "a", :value 4}
          {:key "b", :value 5}
          {:key "c", :value 6}
          {:key "c", :value 7}]
         @(sql/select my-db [:*]
            (sql/from :pseudo-source)
            (sql/order-by :key :value))))

  (is (= [{:key "a", :sum 10}
          {:key "b", :sum 5}
          {:key "c", :sum 13}]
         @(sql/select my-db [:key '(sum :value)]
            (sql/from :pseudo-source)
            (sql/group-by :key)
            (sql/order-by 1 2))))

  (is (= [{:count 0}]
         @(sql/create-materialized-view my-db :key-sums []
            (sql/select my-db [:key '(sum :value)]
              (sql/from :pseudo-source)
              (sql/group-by :key)))))

  (is (= [{:sum 28}]
         @(sql/select my-db ['(sum :sum)]
            (sql/from :key-sums))))

  (is (= [{:count 0}]
         @(sql/create-materialized-view my-db :lhs [:key :value]
            (sql/values [["x" "a"] ["y" "b"] ["z" "c"]]))))

  (is (= [{:key "x", :sum 10}
          {:key "y", :sum 5}
          {:key "z", :sum 13}]
         @(sql/select my-db [:lhs.key '(sum :rhs.value)]
            (sql/from :lhs)
            (sql/join (sql/as :pseudo-source :rhs) '(on (= :lhs.value :rhs.key)))
            (sql/group-by :lhs.key)
            (sql/order-by 1 2)))))
