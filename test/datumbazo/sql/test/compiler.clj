(ns datumbazo.sql.test.compiler
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        datumbazo.sql.compiler))

(deftest test-compile-cascade
  (is (nil? (compile-sql {:op :cascade :cascade false})))
  (is (= ["CASCADE"] (compile-sql {:op :cascade :cascade true}))))

(deftest test-compile-continue-identity
  (is (nil? (compile-sql {:op :continue-identity :continue-identity false})))
  (is (= ["CONTINUE IDENTITY"] (compile-sql {:op :continue-identity :continue-identity true}))))

(deftest test-compile-column
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :column :name :created-at}
       ["created-at"]
       {:op :column :table :continents :name :created-at}
       ["continents.created-at"]
       {:op :column :schema :public :table :continents :name :created-at}
       ["public.continents.created-at"]
       {:op :column :schema :public :table :continents :name :created-at :alias :c}
       ["public.continents.created-at AS c"])
  (jdbc/with-quoted-identifiers \"
    (is (= ["\"public\".\"continents\".\"created-at\" AS \"c\""]
           (compile-sql {:op :column :schema :public :table :continents :name :created-at :alias :c})))))

(deftest test-compile-sql
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :nil}
       ["NULL"]
       {:op :constant :form 1}
       ["1"]
       {:op :keyword :form :continents.created-at}
       ["continents.created-at"]
       {:op :expressions :children [{:op :constant :form 1}]}
       ["1"]
       {:op :expressions :children [{:op :constant :form "x"}]}
       ["?" "x"]
       {:op :expressions :children [{:op :constant :form 1} {:op :constant :form "x"}]}
       ["1, ?" "x"]
       {:op :fn-call :name 'max :args [{:op :keyword :form :created-at}]}
       ["max(created-at)"]
       {:op :fn-call :name 'greatest :args [{:op :constant :form 1} {:op :constant :form 2}]}
       ["greatest(1, 2)"]
       {:op :fn-call :name 'ST_AsText :args [{:op :fn-call :name 'ST_Centroid :args [{:op :constant :form "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"}]}]}
       ["ST_AsText(ST_Centroid(?))" "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"])
  (jdbc/with-quoted-identifiers \"
    (is (= ["\"continents\".\"created-at\""]
           (compile-sql {:op :keyword :form :continents.created-at})))))

(deftest test-compile-expressions
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :expressions :children [{:op :keyword :form :created-at}]}
       ["created-at"]
       {:op :expressions :children [{:op :keyword :form :name} {:op :keyword :form :created-at}]}
       ["name, created-at"]))

(deftest test-compile-drop-table
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :drop-table :tables [{:op :table :name :continents}]}
       ["DROP TABLE continents"]
       {:op :drop-table :tables [{:op :table :name :continents}] :cascade {:op :cascade :cascade true}}
       ["DROP TABLE continents CASCADE"]
       {:op :drop-table :tables [{:op :table :name :continents}] :restrict {:op :restrict :restrict true}}
       ["DROP TABLE continents RESTRICT"]
       {:op :drop-table :tables [{:op :table :name :continents}] :if-exists {:op :if-exists :if-exists true}}
       ["DROP TABLE IF EXISTS continents"]
       {:op :drop-table :tables [{:op :table :name :continents}]
        :cascade {:op :cascade :cascade true}
        :restrict {:op :restrict :restrict true}
        :if-exists {:op :if-exists :if-exists true}}
       ["DROP TABLE IF EXISTS continents CASCADE RESTRICT"]))

(deftest test-compile-number
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :constant :form 1}
       ["1"]
       {:op :constant :form 3.14}
       ["3.14"]))

(deftest test-compile-if-exists
  (is (nil? (compile-sql {:op :if-exists :if-exists false})))
  (is (= ["IF EXISTS"] (compile-sql {:op :if-exists :if-exists true}))))

(deftest test-compile-limit
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :limit :count 1}
       ["LIMIT 1"]
       {:op :limit :count nil}
       ["LIMIT ALL"]))

(deftest test-compile-restrict
  (is (nil? (compile-sql {:op :restrict :restrict false})))
  (is (= ["RESTRICT"] (compile-sql {:op :restrict :restrict true}))))

(deftest test-compile-restart-identity
  (is (nil? (compile-sql {:op :restart-identity :restart-identity false})))
  (is (= ["RESTART IDENTITY"] (compile-sql {:op :restart-identity :restart-identity true}))))

;; (deftest test-compile-select
;;   (are [ast expected]
;;        (is (= expected (compile-sql ast)))
;;        {:op :select :expressions {:op :expressions :children []} :from [{:op :table :name :continents}]}
;;        ["SELECT * FROM continents"]
;;        {:op :select :expressions {:op :expressions :children []} :from [{:op :table :name :continents}] :limit {:op :limit :count 1}}
;;        ["SELECT * FROM continents LIMIT 1"]
;;        {:op :select :expressions {:op :expressions :children []} :from [{:op :table :name :continents}] :offset {:op :offset :start 1}}
;;        ["SELECT * FROM continents OFFSET 1"]
;;        {:op :select :expressions {:op :expressions :children []} :from [{:op :table :name :continents}] :limit {:op :limit :count 1} :offset {:op :offset :start 1}}
;;        ["SELECT * FROM continents LIMIT 1 OFFSET 1"]))

(deftest test-compile-string
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :constant :form 1}
       ["1"]
       {:op :constant :form 3.14}
       ["3.14"]))

(deftest test-compile-offset
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :offset :start 1}
       ["OFFSET 1"]
       {:op :offset :start nil}
       ["OFFSET 0"]))

(deftest test-compile-table
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :table :name :continents}
       ["continents"]
       {:op :table :schema :public :name :continents}
       ["public.continents"]
       {:op :table :schema :public :name :continents :alias :c}
       ["public.continents AS c"])
  (jdbc/with-quoted-identifiers \"
    (is (= ["\"public\".\"continents\" AS \"c\""]
           (compile-sql {:op :table :schema :public :name :continents :alias :c})))))

(deftest test-compile-truncate-table
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :truncate-table :tables [{:op :table :name :continents}]}
       ["TRUNCATE TABLE continents"]
       {:op :truncate-table :tables [{:op :table :name :continents}] :cascade {:op :cascade :cascade true}}
       ["TRUNCATE TABLE continents CASCADE"]
       {:op :truncate-table :tables [{:op :table :name :continents}] :restrict {:op :restrict :restrict true}}
       ["TRUNCATE TABLE continents RESTRICT"]
       {:op :truncate-table :tables [{:op :table :name :continents}] :restart-identity {:op :restart-identity :restart-identity true}}
       ["TRUNCATE TABLE continents RESTART IDENTITY"]
       {:op :truncate-table :tables [{:op :table :name :continents}] :continue-identity {:op :continue-identity :continue-identity true}}
       ["TRUNCATE TABLE continents CONTINUE IDENTITY"]
       {:op :truncate-table :tables [{:op :table :name :continents}]
        :restart-identity {:op :restart-identity :restart-identity true}
        :continue-identity {:op :continue-identity :continue-identity true}
        :cascade {:op :cascade :cascade true}
        :restrict {:op :restrict :restrict true}}
       ["TRUNCATE TABLE continents RESTART IDENTITY CONTINUE IDENTITY CASCADE RESTRICT"]))
