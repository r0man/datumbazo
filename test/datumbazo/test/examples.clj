(ns datumbazo.test.examples
  (:refer-clojure :exclude [group-by])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [upper-case]])
  (:use clojure.test
        datumbazo.core
        datumbazo.test))

(deftable continents
  "The continents database table."
  (column :id :serial)
  (column :name :text :unique? true))

(deftable countries
  "The countries database table."
  (column :id :serial)
  (column :continent-id :integer :references :continents/id)
  (column :name :text :unique? true))

(deftable twitter-users
  "The Twitter users database table."
  (schema :twitter)
  (table :users)
  (column :id :serial)
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
  (schema :twitter)
  (table :tweets)
  (column :id :serial)
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

(deftest test-continents-table
  (is (= :continents (:name continents-table)))
  (is (= [:id :name] (:columns continents-table)))
  (let [column (:id (:column continents-table))]
    (is (= :id (:name column)))
    (is (= :serial (:type column))))
  (let [column (:name (:column continents-table))]
    (is (= :name (:name column)))
    (is (= :text (:type column)))))

(deftest test-countries-table
  (is (= :countries (:name countries-table)))
  (is (= [:id :continent-id :name] (:columns countries-table)))
  (let [column (:id (:column countries-table))]
    (is (= :id (:name column)))
    (is (= :serial (:type column))))
  (let [column (:continent-id (:column countries-table))]
    (is (= :continent-id (:name column)))
    (is (= :integer (:type column))))
  (let [column (:name (:column countries-table))]
    (is (= :name (:name column)))
    (is (= :text (:type column)))))

(database-test test-drop-continents
  (is (= "Drop the continents database table."
         (:doc (meta #'drop-continents))))
  (drop-countries)
  (is (= 0 (drop-continents)))
  (is (= 0 (drop-continents :if-exists true))))

(database-test test-delete-continents
  (is (= "Delete all rows in the continents database table."
         (:doc (meta #'delete-continents))))
  (is (= 0 (delete-continents)))
  (is (= 0 (count-all :continents))))

(database-test test-delete-countries
  (is (= "Delete all rows in the countries database table."
         (:doc (meta #'delete-countries))))
  (is (= 0 (delete-countries)))
  (is (= 0 (count-all :countries))))

(database-test test-insert-continent
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
  (is (= 0 (truncate-continents :cascade true)))
  (is (= 0 (truncate-continents :cascade true :if-exists true)))
  (is (= 0 (count-all :continents))))

(database-test test-truncate-countries
  (is (= "Truncate the countries database table."
         (:doc (meta #'truncate-countries))))
  (is (= 0 (truncate-countries)))
  (is (= 0 (count-all :countries))))

(database-test test-update-continent
  (is (nil? (update-continent europe)))
  (let [europe (insert-continent europe)
        row (update-continent (assoc europe :name "Europa"))]
    (is (number? (:id row)))
    (is (= "Europa" (:name row)))
    (is (= "eu" (:code row)))
    (let [row (update-continent (assoc row :name "Europe"))]
      (is (number? (:id row)))
      (is (= "Europe" (:name row)))
      (is (= "eu" (:code row))))))

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
    (is (= europe (continent-by-id (str (:id europe)))))))

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
    (is (= user (save-twitter-user user)))))
