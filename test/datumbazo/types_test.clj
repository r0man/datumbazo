(ns datumbazo.types-test
  (:require [clojure.test :refer :all]
            [datumbazo.people :as people]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all]))

(defn create-mood-type! [db]
  @(sql/create-type db :mood
     (sql/enum [:sad :ok :happy])))

(defn create-person-table! [db]
  @(sql/create-table db :person
     (sql/column :name :text :primary-key? true)
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

(deftest test-create-type-in-schema
  (with-backends [db]
    (is (= @(sql/create-schema db :my-schema)
           [{:count 0}]))
    (is (= @(sql/create-type db :my-schema.mood
              (sql/enum ["sad" "ok" "happy"]))
           [{:count 0}]))
    (is (= @(sql/create-table db :person
              (sql/column :name :text)
              (sql/column :mood :my-schema.mood))
           [{:count 0}]))
    (is (= @(sql/insert db :person []
              (sql/values
               [{:name "Larry"
                 :mood '(cast "sad" :my-schema.mood)}])
              (sql/returning :*))
           [{:name "Larry"
             :mood "sad"}]))))

(deftest test-drop-type
  (with-backends [db]
    (create-mood-type! db)
    (is (= @(sql/drop-type db [:mood])
           [{:count 0}]))
    (is (= @(sql/drop-type db [:mood]
              (sql/if-exists true))
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

(deftest test-insert-cast!
  (with-backends [db]
    (create-mood-type! db)
    (create-person-table! db)
    (is (= {:name "Larry" :mood "sad"}
           (people/insert! db {:name "Larry" :mood "sad"})))
    (is (= {:mood "sad", :name "Bob"}
           (people/insert! db {:name "Bob" :mood '(cast "sad" :mood )})))))

(deftest test-update-cast!
  (with-backends [db]
    (create-mood-type! db)
    (create-person-table! db)
    (let [row {:name "Larry" :mood "sad"}]
      (is (= row (people/insert! db row)))
      (is (= {:name "Larry" :mood "ok"}
             (people/update! db {:name "Larry" :mood "ok"})))
      (is (= {:name "Larry" :mood "happy"}
             (people/update! db {:name "Larry" :mood '(cast "happy" :mood )}))))))
