(ns datumbazo.test.core
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [upper-case]]
            [validation.core :refer :all]
            [slingshot.slingshot :refer [try+]])
  (:use clojure.test
        datumbazo.core
        datumbazo.test))

(defvalidate continent
  (presence-of :name)
  (uniqueness-of :continents :name :if new-record?)
  (presence-of :code)
  (exact-length-of :code 2)
  (uniqueness-of :continents :code :if new-record?))

(deftable continents
  "The continents database table."
  (column :id :serial)
  (column :name :text :unique? true)
  (column :code :text :unique? true)
  (column :geometry :geometry :hidden? true)
  (prepare validate-continent!))

(deftable countries
  "The countries database table."
  (column :id :serial)
  (column :continent-id :integer :references :continents/id)
  (column :name :text :unique? true)
  (column :geometry :geometry :hidden? true))

(deftable twitter-users
  "The Twitter users database table."
  (table :twitter.users)
  (column :id :bigint)
  (column :screen-name :text :not-null? true)
  (column :name :text :not-null? true)
  (column :followers-count :integer :not-null? true :default 0)
  (column :friends-count :integer :not-null? true :default 0)
  (column :retweet-count :integer :not-null? true :default 0)
  (column :statuses-count :integer :not-null? true :default 0)
  (column :verified :boolean :not-null? true :default false)
  (column :possibly-sensitive :boolean :not-null? true :default false)
  (column :location :text)
  (column :time-zone :text)
  (column :lang :varchar :length 2)
  (column :url :text)
  (column :profile-image-url :text)
  (column :created-at :timestamp-with-time-zone :not-null? true :default "now()")
  (column :updated-at :timestamp-with-time-zone :not-null? true :default "now()"))

(deftable twitter-tweets
  "The Twitter tweets database table."
  (table :twitter.tweets)
  (column :id :bigint)
  (column :user-id :integer :references :twitter.users/id)
  (column :retweeted :boolean :not-null? true :default false)
  (column :text :text :not-null? true)
  (column :created-at :timestamp-with-time-zone :not-null? true :default "now()")
  (column :updated-at :timestamp-with-time-zone :not-null? true :default "now()"))

(def africa
  {:name "Africa" :code "af"})

(def europe
  {:name "Europe" :code "eu"})

(defn save-africa []
  (save-continent africa))

(defn save-europe []
  (save-continent europe))

(deftest test-columns
  (let [columns (columns twitter-tweets-table)]
    (is (= 6 (count columns)))
    (is (= [:id :user-id :retweeted :text :created-at :updated-at]
           (map :name columns)))))

(deftest test-continents-table
  (let [table continents-table]
    (is (nil? (:schema table)))
    (is (= :continents (:name table)))
    (is (= [:id :name :code :geometry] (:columns table)))
    (let [column (:id (:column table))]
      (is (nil? (:schema column)))
      (is (= :continents (:table column)))
      (is (= :id (:name column)))
      (is (= :serial (:type column))))
    (let [column (:name (:column table))]
      (is (nil? (:schema column)))
      (is (= :continents (:table column)))
      (is (= :name (:name column)))
      (is (= :text (:type column))))
    (let [column (:geometry (:column table))]
      (is (nil? (:schema column)))
      (is (= :continents (:table column)))
      (is (= :geometry (:name column)))
      (is (= :geometry (:type column)))
      (is (= true (:hidden? column))))))

(deftest test-continents-pagination
  (is (= (sql (continents* :page 2 :per-page 20))
         ["SELECT continents.id, continents.name, continents.code FROM continents LIMIT 20 OFFSET 20"])))

(deftest test-countries-table
  (let [table countries-table]
    (is (nil? (:schema table)))
    (is (= :countries (:name table)))
    (is (= [:id :continent-id :name :geometry] (:columns table)))
    (let [column (:id (:column table))]
      (is (nil? (:schema column)))
      (is (= :countries (:table column)))
      (is (= :id (:name column)))
      (is (= :serial (:type column))))
    (let [column (:continent-id (:column table))]
      (is (nil? (:schema column)))
      (is (= :countries (:table column)))
      (is (= :continent-id (:name column)))
      (is (= :integer (:type column))))
    (let [column (:name (:column table))]
      (is (nil? (:schema column)))
      (is (= :countries (:table column)))
      (is (= :name (:name column)))
      (is (= :text (:type column))))))

(database-test test-drop-continents
  (is (= "Drop the continents database table."
         (:doc (meta #'drop-continents))))
  (drop-countries)
  (is (= 0 (drop-continents)))
  (is (= 0 (drop-continents
            (if-exists true)))))

(database-test test-delete-continents
  (is (= "Delete all rows in the continents database table."
         (:doc (meta #'delete-continents))))
  (is (= 0 (delete-continents)))
  (is (= 0 (count-all :continents))))

(database-test test-delete-continent
  (is (= "Delete the continent from the database table."
         (:doc (meta #'delete-continent))))
  (is (= 0 (delete-continent europe)))
  (let [europe (insert-continent europe)]
    (is (= 1 (delete-continent europe)))))

(database-test test-delete-countries
  (is (= "Delete all rows in the countries database table."
         (:doc (meta #'delete-countries))))
  (is (= 0 (delete-countries)))
  (is (= 0 (count-all :countries))))

(database-test test-insert-continent
  (try+
   (insert-continent {})
   (catch [:type :validation.core/error] {:keys [errors record]}
     (is (= {} record))
     (is (= ["can't be blank."] (:name errors)))
     (is (= ["has the wrong length (should be 2 characters)." "can't be blank."] (:code errors)))))
  (let [row (insert-continent europe)]
    (is (number? (:id row)))
    (is (= "Europe" (:name row)))
    (is (= "eu" (:code row)))
    (is (thrown? Exception (insert-continent row)))))

(database-test test-insert-continents
  (let [rows (insert-continents [africa europe])]
    (let [row (first rows)]
      (is (number? (:id row)))
      (is (= "Africa" (:name row)))
      (is (= "af" (:code row))))
    (let [row (second rows)]
      (is (number? (:id row)))
      (is (= "Europe" (:name row)))
      (is (= "eu" (:code row))))))

(database-test test-save-continent
  (let [row (save-continent europe)]
    (is (number? (:id row)))
    (is (= "Europe" (:name row)))
    (is (= "eu" (:code row)))
    (is (= row (save-continent row)))))

(database-test test-truncate-continents
  (is (= "Truncate the continents database table."
         (:doc (meta #'truncate-continents))))
  (is (= 0 (truncate-continents
            (cascade true))))
  (is (= 0 (truncate-continents
            (cascade true)
            (if-exists true) )))
  (is (= 0 (count-all :continents))))

(database-test test-truncate-countries
  (is (= "Truncate the countries database table."
         (:doc (meta #'truncate-countries))))
  (is (= 0 (truncate-countries)))
  (is (= 0 (count-all :countries))))

(database-test test-update-continent
  (try+
   (update-continent {})
   (catch [:type :validation.core/error] {:keys [errors record]}
     (is (= {} record))
     (is (= ["can't be blank."] (:name errors)))
     (is (= ["has the wrong length (should be 2 characters)." "can't be blank."] (:code errors)))))
  (is (nil? (update-continent europe)))
  (let [europe (insert-continent europe)
        continent (update-continent (assoc europe :name "Europa"))]
    (is (number? (:id continent)))
    (is (= "Europa" (:name continent)))
    (is (= "eu" (:code continent)))
    (is (= continent (update-continent continent)))
    (let [continent (update-continent (assoc continent :name "Europe"))]
      (is (number? (:id continent)))
      (is (= "Europe" (:name continent)))
      (is (= "eu" (:code continent))))))

(database-test test-continents
  (is (empty? (continents)))
  (let [europe (save-continent europe)
        africa (save-continent africa)]
    (is (= #{africa europe} (set (continents))))
    (is (= [africa] (continents :page 1 :per-page 1 :order-by :name)))
    (is (= [europe] (continents :page 2 :per-page 1 :order-by :name)))))

(database-test test-countries
  (is (empty? (countries))))

(database-test test-continent-by-id
  (is (nil? (continent-by-id nil)))
  (is (nil? (continent-by-id 1)))
  (let [europe (save-continent europe)]
    (is (= europe (continent-by-id (:id europe))))
    (is (= europe (continent-by-id (str (:id europe)))))
    (is (= europe (continent-by-id (str (:id europe) "-europe"))))))

(database-test test-continents-by-name
  (is (nil? (continent-by-name nil)))
  (is (nil? (continent-by-name "Europe")))
  (let [europe (save-continent europe)]
    (is (= europe (continent-by-name (:name europe))))))

(database-test test-continents-by-id
  (is (empty? (continents-by-id 1)))
  (is (empty? (continents-by-id "1")))
  (let [europe (save-continent europe)]
    (is (= [europe] (continents-by-id (:id europe))))
    (is (= [europe] (continents-by-id (str (:id europe)))))))

(database-test test-continents-by-name
  (is (empty? (continents-by-name nil)))
  (is (empty? (continents-by-name "Europe")))
  (let [europe (save-continent europe)]
    (is (= [europe] (continents-by-name (:name europe))))
    (is (= (continents-by-name (:name europe))
           (continents-by-name (upper-case (:name europe)))))))

(database-test test-twitter-users
  (is (empty? (twitter-users))))

(database-test test-twitter-tweets
  (is (empty? (twitter-tweets))))

(deftest test-twitter-users-table
  (let [table twitter-users-table]
    (is (= :twitter (:schema table)))
    (is (= :users (:name table)))
    (is (= [:id :screen-name :name :followers-count :friends-count :retweet-count
            :statuses-count :verified :possibly-sensitive :location :time-zone
            :lang :url :profile-image-url :created-at :updated-at]
           (:columns table)))
    (let [column (:id (:column table))]
      (is (= :twitter (:schema column)))
      (is (= :users (:table column)))
      (is (= :id (:name column)))
      (is (= :bigint (:type column))))))

(deftest test-twitter-tweets-table
  (let [table twitter-tweets-table]
    (is (= :twitter (:schema table)))
    (is (= :tweets (:name table)))
    (is (= [:id :user-id :retweeted :text :created-at :updated-at] (:columns table)))
    (let [column (:id (:column table))]
      (is (= :twitter (:schema column)))
      (is (= :tweets (:table column)))
      (is (= :id (:name column)))
      (is (= :bigint (:type column))))))

(database-test test-save-twitter-user
  (let [user (save-twitter-user
              {:created-at #inst "2011-02-22T06:29:06.000-00:00"
               :default-profile-image false
               :description ""
               :favourites-count 0
               :followers-count 1864
               :friends-count 4
               :id 255879714
               :lang "en"
               :listed-count 61
               :location ""
               :name "FinancePress"
               :profile-image-url "http://a0.twimg.com/profile_images/1251163314/finpress2_normal.png"
               :screen-name "thefinancepress"
               :statuses-count 92687
               :time-zone nil
               :url nil
               :verified false})]
    (is (= (dissoc user :updated-at)
           (dissoc (save-twitter-user user) :updated-at)))))

(database-test test-count-all
  (is (= 0 (count-all :continents))))

(database-test test-insert-twitter-user
  (let [user (insert-twitter-user
              {:listed-count 0,
               :default-profile-image true,
               :time-zone nil,
               :name "Twitter",
               :location nil,
               :profile-image-url nil,
               :friends-count 0,
               :followers-count 0,
               :lang nil,
               :url nil,
               :updated-at #inst "2012-10-06T18:22:58.640-00:00",
               :created-at #inst "2012-10-06T18:22:58.640-00:00",
               :screen-name "twitter 2",
               :retweet-count 0,
               :possibly-sensitive false,
               :statuses-count 0,
               :verified false,
               :id "9"
               :description nil})]
    (is (= 9 (:id user)))))

(database-test test-validation
  (let [continent {}]
    (try+
     (insert-continent continent)
     (is false)
     (catch [:type :validation.core/error] {:keys [errors record]}
       (is (= continent record))
       (is (= errors (:errors (meta record))))))
    (try+
     (update-continent continent)
     (is false)
     (catch [:type :validation.core/error] {:keys [errors record]}
       (is (= continent record))
       (is (= errors (:errors (meta record))))))))

(database-test test-array
  (is (= [{:array [1 2]}]
         (run (select [[1 2]])))))

(database-test test-array-concat
  (is (= [{:?column? [1 2 3 4 5 6]}]
         (run (select ['(|| [1 2] [3 4] [5 6])])))))
