(ns datumbazo.core-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.io :refer :all]
            [datumbazo.test :refer :all]
            [inflections.core :refer [hyphenate underscore]]))

(deftest test-with
  (with-backends [db]
    (is (= @(sql/with db [:x (sql/select db [:*]
                               (sql/from :continents))]
              (sql/select db [:*]
                (sql/from :x)))
           @(sql/select db [:*]
              (sql/from :continents))))))

(deftest test-drop-table-if-exists
  (with-test-dbs [db]
    (is (= @(sql/drop-table db [:not-existing]
              (sql/if-exists true))
           [{:count 0}]))))

(deftest test-delete
  (with-backends [db]
    (is (= @(sql/delete db :countries)
           [{:count 2}]))))

(deftest test-drop-table
  (with-backends [db]
    (is (= @(sql/drop-table db [:countries])
           [{:count 0}]))))

(deftest test-truncate
  (with-backends [db]
    (is (= @(sql/truncate db [:countries])
           [{:count 0}]))))

(deftest test-new-db
  (let [db (sql/new-db "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true")]
    (is (instance? sqlingvo.db.Database db))
    (is (instance? datumbazo.driver.jdbc.clojure.Driver (:driver db)))
    (is (= :postgresql (:scheme db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "localhost" (:server-name db)))
    (is (= 5432 (:server-port db)))
    (is (= "datumbazo" (:name db)))
    (is (= {:ssl "true"} (:query-params db)))))

(deftest test-with-db
  (sql/with-db [db "postgresql://tiger:scotch@localhost:5432/datumbazo"]
    (is (instance? sqlingvo.db.Database db))
    (is (instance? datumbazo.driver.jdbc.clojure.Driver (:driver db)))
    (is (nil? (:connection db)))
    (is (= :postgresql (:scheme db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "localhost" (:server-name db)))
    (is (= 5432 (:server-port db)))
    (is (= "datumbazo" (:name db)))))

(deftest test-with-connection
  (sql/with-db [db "postgresql://tiger:scotch@localhost:5432/datumbazo"]
    (sql/with-connection [db db]
      (is (instance? sqlingvo.db.Database db))
      (is (instance? java.sql.Connection (sql/connection db))))))

(deftest test-deref-drop-table
  (with-backends [db]
    (let [table :test-deref-drop-table]
      @(sql/create-table db table
         (sql/column :a :integer))
      (is (= @(sql/drop-table db [table])
             [{:count 0}]))
      (is (= @(sql/drop-table db [table]
                (sql/if-exists true))
             [{:count 0}])))))

(deftest test-create-test-table
  (with-backends [db]
    (is (= (create-test-table db :empsalary)
           [{:count 0}]))))

(deftest test-insert-test-table
  (with-backends [db]
    (create-test-table db :empsalary)
    (is (= (insert-test-table db :empsalary)
           [{:count 10}]))))

;; (deftest test-select
;;   (with-backends [db]
;;     (setup-countries db)
;;     (is (= @(sql/select db [:*]
;;               (sql/from :countries))
;;            countries))))

;; (deftest test-select-columns
;;   (with-backends [db]
;;     (setup-countries db)
;;     (is (= @(sql/select db [:id]
;;               (sql/from :countries))
;;            (map #(select-keys % [:id]) countries)))))

;; (deftest test-select-where-equals
;;   (with-backends [db]
;;     (setup-countries db)
;;     (is (= @(sql/select db [:id :name]
;;               (sql/from :countries)
;;               (sql/where '(= :name "Spain")))
;;            [{:id 1 :name "Spain"}]))))

;; (deftest test-insert
;;   (with-backends [db]
;;     (drop-countries db)
;;     (create-countries db)
;;     (is (= @(sql/insert db :countries []
;;               (sql/values countries))
;;            [{:count 4}]))))

;; (deftest test-insert-returning
;;   (with-backends [db]
;;     (drop-countries db)
;;     (create-countries db)
;;     (is (= @(sql/insert db :countries []
;;               (sql/values countries)
;;               (sql/returning :*))
;;            countries))))

;; (deftest test-insert-returning-column
;;   (with-backends [db]
;;     (drop-countries db)
;;     (create-countries db)
;;     (is (= @(sql/insert db :countries []
;;               (sql/values countries)
;;               (sql/returning :id))
;;            (map #(select-keys % [:id]) countries)))))

(deftest test-except
  (with-backends [db]
    (is (= (set @(sql/except
                  (sql/select db [(sql/as '(generate_series 1 3) :x)])
                  (sql/select db [(sql/as '(generate_series 3 4) :x)])))
           #{{:x 1} {:x 2}}))))

(deftest test-union
  (with-backends [db]
    (is (= @(sql/union
             (sql/select db [(sql/as 1 :x)])
             (sql/select db [(sql/as 1 :x)]))
           [{:x 1}]))))

(deftest test-union-all
  (with-backends [db]
    (is (= @(sql/union
             {:all true}
             (sql/select db [(sql/as 1 :x)])
             (sql/select db [(sql/as 1 :x)]))
           [{:x 1} {:x 1}]))))

(deftest test-intersect
  (with-backends [db]
    (is (= @(sql/intersect
             (sql/select db [(sql/as '(generate_series 1 2) :x)])
             (sql/select db [(sql/as '(generate_series 2 3) :x)]))
           [{:x 2}]))))

(deftest test-select-array
  (with-backends [db]
    (is (= @(sql/select db [[1 2]])
           [{:array [1 2]}]))))

(deftest test-select-regconfig
  (with-backends [db]
    (is (= @(sql/select db [(sql/as '(cast "english" :regconfig) :x)])
           [{:x "english"}]))))

(deftest test-with-transaction
  (with-drivers [db db {:rollback false}]
    (sql/with-connection [db db]
      @(sql/create-table db :test-with-transaction
         (sql/column :x :int))
      (try (sql/with-transaction [db db]
             @(sql/insert db :test-with-transaction []
                (sql/values [{:x 1}]))
             (throw (ex-info "boom" {})))
           (catch Exception e)))
    (sql/with-connection [db db]
      (is (empty? @(sql/select db [:*]
                     (sql/from :test-with-transaction))))
      @(sql/drop-table db [:test-with-transaction]
         (sql/if-exists true)))))

(deftest test-with-nested-transaction
  (with-drivers [db db {:rollback false}]
    (sql/with-connection [db db]
      (sql/with-transaction [db db]
        ;; TODO: How does this work in clojure.java.jdbc?
        (when (= (:backend db) 'jdbc.core)
          @(sql/drop-table db [:test-with-nested-transaction]
             (sql/if-exists true))
          @(sql/create-table db :test-with-nested-transaction
             (sql/column :x :int))
          (try (sql/with-transaction [db]
                 @(sql/insert db :test-with-nested-transaction []
                    (sql/values {:x 1}))
                 (throw (ex-info "boom" {})))
               (catch Exception e))
          (is (empty? @(sql/select db [:*] (sql/from :test-with-nested-transaction))))
          @(sql/drop-table db [:test-with-nested-transaction]))))))

(deftest test-sql-name
  (with-backends [db]
    (let [db (assoc db :sql-name underscore)]
      (with-test-table db :empsalary
        (is (= @(sql/select db [:*]
                  (sql/from :empsalary)
                  (sql/where '(= :empno 10)))
               [{:depname "develop"
                 :empno 10
                 :salary 5200
                 :enroll_date #inst "2007-08-01T00:00:00.000-00:00"}]))))))

(deftest test-sql-name-and-keyword
  (with-backends [db {:sql-name underscore :sql-keyword hyphenate}]
    (with-test-table db :empsalary
      (is (= @(sql/select db [:*]
                (sql/from :empsalary)
                (sql/where '(= :empno 10)))
             [{:depname "develop"
               :empno 10
               :salary 5200
               :enroll-date #inst "2007-08-01T00:00:00.000-00:00"}])))))

(deftest test-explain
  (with-backends [db]
    (let [result (first @(sql/explain db (sql/select db [1])))]
      (is (re-matches
           #"Result  \(cost=\d+.\d+..\d+.\d+ rows=\d+ width=\d+\)"
           (get result (keyword "query plan")))))))

(deftest test-print-explain
  (with-backends [db]
    (is (re-matches
         #"Result  \(cost=\d+.\d+..\d+.\d+ rows=\d+ width=\d+\)\n"
         (with-out-str (sql/print-explain (sql/select db [1])))))))

(deftest test-values
  (with-backends [db]
    (is (= @(sql/values db [[1 "one"] [2 "two"] [3 "three"]])
           [{:column1 1 :column2 "one"}
            {:column1 2 :column2 "two"}
            {:column1 3 :column2 "three"}]))))

(deftest test-insert-jsonb
  (with-backends [db]
    @(sql/create-table db :my-table
       (sql/column :a :jsonb))
    (is (= @(sql/insert db :my-table [:a]
              (sql/values [{:a (jsonb [1 2 3])}])
              (sql/returning :*))
           [{:a [1 2 3]}]))))

(deftest test->>
  (with-backends [db]
    (is (= @(sql/select db [`(->> (cast "[1,2,3]" :json) 2)])
           [{:?column? "3"}]))))

(deftest test-select->-number
  (with-backends [db]
    (is (= @(sql/select db [`(-> (cast "[1,2,3]" :json) 2)])
           [{:?column? 3}]))))

(deftest test-select->-string
  (with-backends [db]
    (is (= @(sql/select db [`(-> (cast "{\"a\":1, \"b\": 2}" :json) "b")])
           [{:?column? 2}]))))

(deftest test-select->-alias
  (with-backends [db]
    (is (= @(sql/select db [(sql/as `(-> (cast "[1,2,3]" :json) 2) :x)])
           [{:x 3}]))))

(deftest test-select->>-number
  (with-backends [db]
    (is (= @(sql/select db [`(->> (cast "[1,2,3]" :json) 2)])
           [{:?column? "3"}]))))

(deftest test-select->>-string
  (with-backends [db]
    (is (= @(sql/select db [`(->> (cast "{\"a\":1, \"b\": 2}" :json) "b")])
           [{:?column? "2"}]))))

(deftest test->>-nested
  (with-backends [db]
    (is (= @(sql/select db [`(-> (cast "{\"a\":1, \"c\": {\"d\": 1}}" :json) "c" "d")])
           [{:?column? 1}]))))

(deftest test-join
  (with-backends [db]
    (is (= @(sql/select db [(sql/as :countries.name :country)
                            (sql/as :continents.name :continent)]
              (sql/from :countries)
              (sql/join :continents.id :countries.continent-id)
              (sql/order-by 1))
           [{:country "Indonesia", :continent "Asia"}
            {:country "Spain", :continent "Europe"}]))))

(deftest test-select-in-cast
  (with-backends [db]
    (is (= @(sql/select db [(sql/as 1 :x)]
              (sql/where `(in 1 ((cast "1" :integer) (cast "2" :integer)))))
           [{:x 1}]))))

(deftest test-result-meta-data
  (with-backends [db]
    (let [sql (sql/select db [:*]
                (sql/from :countries))]
      (is (= (sql/ast (-> @sql meta :datumbazo/stmt))
             (sql/ast sql))))))
