(ns datumbazo.enum-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all]))

(defn create-mood-type! [db]
  @(sql/create-type db :mood
     (sql/enum [:sad :ok :happy])))

(defn create-person-table! [db]
  @(sql/create-table db :person
     (sql/column :name :text)
     (sql/column :mood :mood)))

(defn insert-people! [db]
  @(sql/insert db :person []
     (sql/values
      [{:name "Larry"
        :mood '(cast "sad" :mood)}
       {:name "Curly"
        :mood '(cast "ok" :mood)}])
     (sql/returning :*)))

(deftest test-create-type
  (with-backends [db]
    (is (= (create-mood-type! db)
           [{:count 0}]))))

(deftest test-insert-type
  (with-backends [db]
    (create-mood-type! db)
    (create-person-table! db)
    (is (= (insert-people! db)
           [{:name "Larry" :mood "sad"}
            {:name "Curly" :mood "ok"}]))))

(deftest test-select-type
  (with-backends [db]
    (create-mood-type! db)
    (create-person-table! db)
    (insert-people! db)
    (is (= @(sql/select db [:*]
              (sql/from :person)
              (sql/where `(> :mood (cast "sad" :mood))))
           [{:name "Curly" :mood "ok"}]))))
