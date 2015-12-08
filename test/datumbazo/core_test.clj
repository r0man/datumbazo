(ns datumbazo.core-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clj-time.core :refer [now]]
            [clj-time.coerce :refer [to-timestamp]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [upper-case]]
            [clojure.java.io :refer [file]]
            [validation.core :refer :all]
            [datumbazo.test :refer :all]
            [datumbazo.validation :refer [new-record? uniqueness-of]]
            [slingshot.slingshot :refer [try+]])
  (:use clojure.test
        datumbazo.core
        datumbazo.io))

(defvalidate continent
  (presence-of :name)
  (uniqueness-of db :continents :name :if new-record?)
  (presence-of :code)
  (exact-length-of :code 2)
  (uniqueness-of db :continents :code :if new-record?))

(deftable continents
  "The continents database table."
  (column :id :serial :primary-key? true)
  (column :name :text :unique? true)
  (column :code :text :unique? true)
  (column :geometry :geometry :hidden? true)
  (prepare validate-continent!))

(deftable countries
  "The countries database table."
  (column :id :serial :primary-key? true)
  (column :continent-id :integer :references :continents/id)
  (column :name :text :unique? true)
  (column :geometry :geometry :hidden? true))

(deftable ratings
  (column :id :serial)
  (column :user-id :integer :not-null? true :references :users/id)
  (column :spot-id :integer :not-null? true :references :spots/id)
  (column :rating :integer :not-null? true)
  (column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
  (column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))
  (primary-key :user-id :spot-id :created-at))

(deftable users
  (column :id :integer)
  (column :nick :varchar :length 255)
  (primary-key :nick))

(deftest test-continent-by-pk*
  (with-test-db [db]
    (is (= ["SELECT \"continents\".\"id\", \"continents\".\"name\", \"continents\".\"code\" FROM \"continents\" WHERE (\"continents\".\"id\" = 1)"]
           (sql (continent-by-pk* db {:id 1}))))))

(deftest test-continent-by-pk
  (with-test-db [db]
    (is (empty? (continent-by-pk db {:id 1})))))

;; TODO: Create table
(deftest test-rating-by-pk
  (with-test-db [db]
    (let [time (now)]
      ;; (is (= [(str "SELECT \"ratings\".\"user_id\", \"ratings\".\"spot_id\", \"ratings\".\"rating\", \"ratings\".\"created_at\", \"ratings\".\"updated_at\" "
      ;;              "FROM \"ratings\" WHERE ((\"ratings\".\"user_id\" = ?) and (\"ratings\".\"spot_id\" = ?) and (\"ratings\".\"created_at\" = ?))")
      ;;         1 2 (to-timestamp time)]
      ;;        (sql (rating-by-pk* db {:user-id 1 :spot-id 2 :created-at time}))))
      (is (= [(str "SELECT \"ratings\".\"user-id\", \"ratings\".\"spot-id\", \"ratings\".\"rating\", \"ratings\".\"created-at\", \"ratings\".\"updated-at\" "
                   "FROM \"ratings\" WHERE ((\"ratings\".\"user-id\" = NULL) and (\"ratings\".\"spot-id\" = NULL) and (\"ratings\".\"created-at\" = NULL))")]
             (sql (rating-by-pk* db {:user-id 1 :spot-id 2 :created-at time})))))))

(deftable twitter-users
  "The Twitter users database table."
  (table :twitter.users)
  (column :id :bigint :primary-key? true)
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
  (column :id :bigint :primary-key? true)
  (column :user-id :integer :references :twitter.users/id)
  (column :retweeted :boolean :not-null? true :default false)
  (column :text :text :not-null? true)
  (column :created-at :timestamp-with-time-zone :not-null? true :default "now()")
  (column :updated-at :timestamp-with-time-zone :not-null? true :default "now()"))

(deftable tweets-users
  "The join table between tweets and users."
  (table :twitter.tweets-users)
  (column :user-id :integer :not-null? true :references :users/id)
  (column :tweet-id :integer :not-null? true :references :tweets/id))

(defn africa [db]
  (continent-by-name db "Africa"))

(defn antarctica [db]
  (continent-by-name db "Antarctica"))

(defn europe [db]
  (continent-by-name db "Europe"))

(defn save-africa []
  (save-continent db africa))

(defn save-europe []
  (save-continent db europe))

(deftest test-with-db
  (with-db [db "postgresql://tiger:scotch@localhost:5432/datumbazo"]
    (is (instance? sqlingvo.db.Database db))
    (is (:connection db))))

(deftest test-columns
  (let [columns (columns twitter-tweets-table)]
    (is (= 6 (count columns)))
    (is (= [:id :user-id :retweeted :text :created-at :updated-at]
           (map :name columns)))))

(deftest test-prefix
  (are [x y expected]
      (= (prefix x y) expected)
    :a :b :a.b
    :continents :id :continents.id))

(deftest test-column-keys
  (is (= (column-keys continents-table)
         [:id :name :code :geometry]))
  (is (= (column-keys continents-table :prefix? true)
         [:continents.id
          :continents.name
          :continents.code
          :continents.geometry])))

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
  (is (= ["SELECT \"continents\".\"id\", \"continents\".\"name\", \"continents\".\"code\" FROM \"continents\" LIMIT 20 OFFSET 20"]
         (sql (continents* db {:page 2 :per-page 20})))))

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

(deftest test-drop-continents
  (with-test-db [db]
    (is (= "Drop the continents database table."
           (:doc (meta #'drop-continents))))
    (drop-countries db)
    (is (= 0 (drop-continents db)))
    (is (= 0 (drop-continents db (if-exists true))))))

(deftest test-delete-continents
  (with-test-db [db]
    (is (= "Delete all rows in the continents database table."
           (:doc (meta #'delete-continents))))
    (is (= 7 (delete-continents db)))
    (is (= 0 (delete-continents db)))
    (is (= 0 (count-all db :continents)))))

(deftest test-delete-continent
  (with-test-db [db]
    (is (= "Delete the continent from the database table."
           (:doc (meta #'delete-continent))))
    (let [europe (europe db)]
      (is (= 1 (delete-continent db europe)))
      (is (= 0 (delete-continent db europe))))))

(deftest test-delete-countries
  (with-test-db [db]
    (is (= "Delete all rows in the countries database table."
           (:doc (meta #'delete-countries))))
    (is (= 0 (delete-countries db)))
    (is (= 0 (count-all db :countries)))))

(deftest test-insert-continent
  (with-test-db [db]
    (try+
     (insert-continent db {})
     (catch [:type :validation.core/error] {:keys [errors record]}
       (is (= {} record))
       (is (= ["can't be blank."] (:name errors)))
       (is (= ["has the wrong length (should be 2 characters)." "can't be blank."] (:code errors)))))
    (let [europe (europe db)]
      (delete-continent db europe)
      (let [row (insert-continent db europe)]
        (is (number? (:id row)))
        (is (= "Europe" (:name row)))
        (is (= "EU" (:code row)))
        (is (thrown? Exception (insert-continent db row)))))))

(deftest test-insert-continents
  (with-test-db [db]
    (let [continents [(africa db) (europe db)]]
      (delete-continents db)
      (let [rows (insert-continents db continents)]
        (let [row (first rows)]
          (is (number? (:id row)))
          (is (= "Africa" (:name row)))
          (is (= "AF" (:code row))))
        (let [row (second rows)]
          (is (number? (:id row)))
          (is (= "Europe" (:name row)))
          (is (= "EU" (:code row))))))))

(deftest test-update-columns-best-row-identifiers
  (with-test-db [db]
    (is (= {:id 1} (update-columns-best-row-identifiers db continents-table {:id 1 :name "Europe"})))
    (is (= {:user-id 8841372 :tweet-id 285415218434154496}
           (update-columns-best-row-identifiers db tweets-users-table {:user-id 8841372 :tweet-id 285415218434154496})))))

(deftest test-update-columns-unique-columns
  (with-test-db [db]
    (is (= {:id 1 :name "Europe"} (update-columns-unique-columns db continents-table {:id 1 :name "Europe"})))
    (is (= {:user-id 8841372 :tweet-id 285415218434154496}
           (update-columns-unique-columns db tweets-users-table {:user-id 8841372 :tweet-id 285415218434154496})))))

(deftest test-where-clause-columns
  (with-test-db [db]
    (is (= {:id 1} (where-clause-columns db continents-table {:id 1})))
    (is (= {:name "Europe"} (where-clause-columns db continents-table {:name "Europe"})))
    (is (= {:user-id 8841372 :tweet-id 285415218434154496}
           (where-clause-columns db tweets-users-table {:user-id 8841372 :tweet-id 285415218434154496})))))

(deftest test-update-clause
  (with-test-db [db]
    (let [[_ clause] ((update-clause db continents-table {:id 1}) nil)]
      (is (map? clause)))))

(deftest test-create-table-compound-primary-key
  (with-test-db [db]
    @(create-table db :users
       (column :id :serial :primary-key? true))
    @(create-table db :spots
       (column :id :serial :primary-key? true))
    (is (= @(create-table db :ratings
              (column :id :serial)
              (column :user-id :integer :not-null? true :references :users/id)
              (column :spot-id :integer :not-null? true :references :spots/id)
              (column :rating :integer :not-null? true)
              (column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
              (column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))
              (primary-key :user-id :spot-id :created-at))
           [{:count 0}]))))

(deftest test-save-continent
  (with-test-db [db]
    (let [row (save-continent db (europe db))]
      (is (number? (:id row)))
      (is (= "Europe" (:name row)))
      (is (= "EU" (:code row)))
      (is (= row (save-continent db row)))
      ;; (is (= row (save-continent db (dissoc row :id))))
      )))

(deftest test-truncate-continents
  (with-test-db [db]
    (is (= "Truncate the continents database table."
           (:doc (meta #'truncate-continents))))
    (is (= 0 (truncate-continents db (cascade true))))
    (is (= 0 (truncate-continents db (cascade true) (if-exists true) )))
    (is (= 0 (count-all db :continents)))))

(deftest test-truncate-countries
  (with-test-db [db]
    (is (= "Truncate the countries database table."
           (:doc (meta #'truncate-countries))))
    (is (= 0 (truncate-countries db)))
    (is (= 0 (count-all db :countries)))))

(deftest test-update-continent
  (with-test-db [db]
    (try+
     (update-continent db {})
     (catch [:type :validation.core/error] {:keys [errors record]}
       (is (= {} record))
       (is (= ["can't be blank."] (:name errors)))
       (is (= ["has the wrong length (should be 2 characters)." "can't be blank."] (:code errors)))))
    (try+
     (update-continent db (europe db))
     (catch [:type :validation.core/error] {:keys [errors record]}
       (is (= (europe db) record))
       (is (= "has already been taken" (:name errors)))))
    (let [europe (europe db)
          continent (update-continent db (assoc europe :name "Europa"))]
      (is (number? (:id continent)))
      (is (= "Europa" (:name continent)))
      (is (= "EU" (:code continent)))
      (is (= continent (update-continent db continent)))
      ;; (is (= continent (update-continent db (dissoc continent :id))))
      (let [continent (update-continent db (assoc continent :name "Europe"))]
        (is (number? (:id continent)))
        (is (= "Europe" (:name continent)))
        (is (= "EU" (:code continent)))))))

(deftest test-continents
  (with-test-db [db]
    (is (= 7 (count (continents db))))
    (is (= [(africa db)] (continents db {:page 1 :per-page 1 :order-by :name})))
    (is (= [(antarctica db)] (continents db {:page 2 :per-page 1 :order-by :name})))))

(deftest test-countries
  (with-test-db [db]
    (is (= 0 (count (countries db))))))

(deftest test-countries-by-continent-id
  (with-test-db [db]
    (is (empty? (countries-by-continent-id db (:id (europe db)))))))

(deftest test-continent-by-id
  (with-test-db [db]
    (is (nil? (continent-by-id db nil)))
    (is (nil? (continent-by-id db -1)))
    (is (= (europe db) (continent-by-id db (:id (europe db)))))
    (is (= (europe db) (continent-by-id db (str (:id (europe db))))))
    (is (= (europe db) (continent-by-id db (str (:id (europe db)) "-europe"))))))

(deftest test-continent-by-name
  (with-test-db [db]
    (is (nil? (continent-by-name db nil)))
    (is (nil? (continent-by-name db "unknown")))
    (is (= (europe db) (continent-by-name db (:name (europe db)))))))

(deftest test-continents-by-id
  (with-test-db [db]
    (is (empty? (continents-by-id db -1)))
    (is (empty? (continents-by-id db "-1")))
    (is (= ["SELECT \"continents\".\"id\", \"continents\".\"name\", \"continents\".\"code\" FROM \"continents\" WHERE (\"continents\".\"id\" = -1)"]
           (sql (continents-by-id* db -1))))
    (is (= [(europe db)] (continents-by-id db (:id (europe db)))))
    (is (= [(europe db)] (continents-by-id db (str (:id (europe db))))))))

(deftest test-continents-by-name
  (with-test-db [db]
    (is (empty? (continents-by-name db nil)))
    (is (= [(continent-by-name db "Europe")] (continents-by-name db "Europe")))
    (is (= ["SELECT \"continents\".\"id\", \"continents\".\"name\", \"continents\".\"code\" FROM \"continents\" WHERE (\"continents\".\"name\" = ?)" (citext "Europe")]
           (sql (continents-by-name* db "Europe"))))
    (is (= [(europe db)] (continents-by-name db (:name (europe db)))))
    (is (= (continents-by-name db (:name (europe db)))
           (continents-by-name db (upper-case (:name (europe db))))))))

(deftest test-twitter-users
  (with-test-db [db]
    (is (= 33 (count (twitter-users db))))))

(deftest test-twitter-tweets
  (with-test-db [db]
    (is (= 23 (count (twitter-tweets db))))))

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

(deftest test-save-twitter-user
  (with-test-db [db]
    (let [user (->> {:created-at #inst "2011-02-22T06:29:06.000-00:00"
                     :default-profile-image false
                     :description ""
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
                     :verified false}
                    (save-twitter-user db))]
      (is (= (dissoc user :updated-at)
             (dissoc (save-twitter-user db user) :updated-at))))))

(deftest test-count-all
  (with-test-db [db]
    (is (= 7 (count-all db :continents)))))

(deftest test-insert-twitter-user
  (with-test-db [db]
    (let [user (->> {:listed-count 0,
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
                     :description nil}
                    (insert-twitter-user db))]
      (is (= 9 (:id user))))))

(deftest test-validation
  (with-test-db [db]
    (let [continent {}]
      (try+
       (insert-continent db continent)
       (is false)
       (catch [:type :validation.core/error] {:keys [errors record]}
         (is (= continent record))
         (is (= errors (:errors (meta record))))))
      (try+
       (update-continent db continent)
       (is false)
       (catch [:type :validation.core/error] {:keys [errors record]}
         (is (= continent record))
         (is (= errors (:errors (meta record)))))))))

(deftest test-array
  (with-test-db [db]
    (is (= [{:array [1 2]}]
           @(select db [[1 2]])))))

(deftest test-array-concat
  (with-test-db [db]
    (is (= [{:?column? [1 2 3 4 5 6]}]
           @(select db ['(|| [1 2] [3 4] [5 6])])))))

;; PostgreSQL JSON Support Functions

(deftest test-array-to-json
  (with-test-db [db]
    (is (= [{:array_to_json [[1 5] [99 100]]}]
           @(select db [`(array_to_json (cast "{{1,5},{99,100}}" ~(keyword "int[]")))])))))

(deftest test-row-to-json
  (with-test-db [db]
    (is (= [{:row_to_json {:f1 1, :f2 "foo"}}]
           @(select db ['(row_to_json (row 1 "foo"))])))))

(deftest test-to-json
  (with-test-db [db]
    (is (= [{:to_json "Fred said \"Hi.\""}]
           @(select db ['(to_json "Fred said \"Hi.\"")])))))

(deftest test-with
  (with-test-db [db]
    (is (= @(with db [:x (select db [:*] (from :continents))]
              (select db [:*] (from :x)))
           @(select db [:*] (from :continents))))))

;; DB TESTS

(deftest test-cast-string-to-int
  (with-test-dbs [db]
    (when-not (= :mysql (:name db))
      (is (= @(select db [`(cast "1" :int)])
             [(case (:subprotocol db)
                "mysql" {(keyword "cast('1' as int)") 1}
                "postgresql" {:int4 1}
                "sqlite" {(keyword "cast(? as int)") 1})])))))

(deftest test-select-1
  (with-test-dbs [db]
    (is (= @(select db [1])
           [(case (:subprotocol db)
              "postgresql" {:?column? 1}
              {:1 1})]))))

(deftest test-select-1-2-3
  (with-test-dbs [db]
    (is (= @(select db [1 2 3])
           [(case (:subprotocol db)
              "postgresql" {:?column? 1 :?column?_2 2 :?column?_3 3}
              {:1 1 :2 2 :3 3})]))))

(deftest test-select-1-as-n
  (with-test-dbs [db]
    (is (= @(select db [(as 1 :n)])
           [{:n 1}]))))

(deftest test-select-x-as-n
  (with-test-dbs [db]
    (is (= @(select db [(as "x" :n)])
           [{:n "x"}]))))

(deftest test-test-select-1-2-3-as
  (with-test-dbs [db]
    (is (= @(select db [(as 1 :a) (as 2 :b) (as 3 :c)])
           [{:a 1, :b 2, :c 3}]))))

(deftest test-select-1-in-list
  (with-test-dbs [db #{:postgresql :sqlite}]
    (is (= [{:a 1}] @(select db [(as 1 :a)] (where `(in 1 ~(list 1 2 3))))))))

(deftest test-concat-strings
  (with-test-dbs [db]
    (is (= [(case (:subprotocol db)
              "mysql" {(keyword "('a' || 'b' || 'c')") 0} ;; not string concat, but OR operator
              "postgresql" {:?column? "abc"}
              "sqlite" {(keyword "(? || ? || ?)") "abc"})]
           @(select db ['(|| "a" "b" "c")])))))

(deftest test-create-table
  (with-test-dbs [db]
    (let [table :test-create-table]
      (try (is (= @(create-table db table
                     (column :id :integer)
                     (column :nick :varchar :length 255)
                     (primary-key :nick))
                  [{:count 0}]))
           (finally
             ;; Cleanup for MySQL (non-transactional DDL)
             @(drop-table db [table] (if-exists true)))))))

(deftest test-drop-table-if-exists
  (with-test-dbs [db]
    (is (= @(drop-table db [:not-existing]
              (if-exists true))
           [{:count 0}]))))

(deftest test-delete
  (with-test-db [db]
    (is (= @(delete db :countries)
           [{:count 0}]))))

(deftest test-drop-table
  (with-test-db [db]
    (is (= @(drop-table db [:countries])
           [{:count 0}]))))

(deftest test-select
  (with-test-db [db]
    (are [stmt expected]
        (= (deref stmt) expected)
      (select db [(as 1 :x)])
      [{:x 1}]
      (select db [:*]
        (from (as (select db [(as 1 :x) (as 2 :y)]) :z)))
      [{:x 1 :y 2}]
      (select db [:name]
        (from :continents)
        (order-by :name))
      [{:name "Africa"}
       {:name "Antarctica"}
       {:name "Asia"}
       {:name "Europe"}
       {:name "North America"}
       {:name "Oceania"}
       {:name "South America"}])))

(deftest test-truncate
  (with-test-db [db]
    (is (= @(truncate db [:countries])
           [{:count 0}]))))

(deftest test-new-db
  (let [db (new-db "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true")]
    (is (instance? sqlingvo.db.Database db))
    (is (= "postgresql" (:subprotocol db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "localhost" (:host db)))
    (is (= 5432 (:port db)))
    (is (= "datumbazo" (:name db)))
    (is (= {:ssl "true"} (:params db)))
    (is (= db (new-db db)))))

(deftest test-with-db
  (with-db [db "postgresql://tiger:scotch@localhost:5432/datumbazo"]
    (is (instance? sqlingvo.db.Database db))
    (is (instance? java.sql.Connection (:connection db)))
    (is (= "postgresql" (:subprotocol db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "localhost" (:host db)))
    (is (= 5432 (:port db)))
    (is (= "datumbazo" (:name db)))
    (is (= db (new-db db)))))

(deftest test-select-1-as-a-2-as-b-3-as-c
  (with-test-db [db]
    (is (= [{:a 1 :b 2 :c 3}]
           @(select db [(as 1 :a)
                        (as 2 :b)
                        (as 3 :c)])))))


;; CAST

(deftest test-cast-int-as-text
  (with-test-db [db]
    (is (= [{:text "1"}]
           @(select db [(as `(cast 1 :text) :text)])))))

(deftest test-deref-select
  (with-test-db [db]
    (is (= @(select db [(as 1 :a)
                        (as 2 :b)
                        (as 3 :c)])
           [{:a 1 :b 2 :c 3}]))))

(deftest test-deref-create-table
  (with-test-db [db]
    (let [table :test-deref-create-table]
      (is (= @(create-table db table
                (column :a :integer)
                (column :b :integer))
             [{:count 0}]))
      @(drop-table db [table]))))

(deftest test-deref-drop-table
  (with-test-db [db]
    (let [table :test-deref-drop-table]
      @(create-table db table
         (column :a :integer))
      (is (= @(drop-table db [table])
             [{:count 0}]))
      (is (= @(drop-table db [table]
                (if-exists true))
             [{:count 0}])))))

(deftest test-deref-insert
  (with-test-db [db]
    (let [table :test-deref-insert]
      @(create-table db table
         (column :a :integer)
         (column :b :integer))
      (is (= @(insert db table [:a :b]
                (values {:a 1 :b 2}))
             [{:count 1}]))
      @(drop-table db [table]))))

(deftest test-insert-fixed-columns-mixed-values
  (with-test-db [db]
    @(drop-table db [:test] (if-exists true))
    @(create-table db :test
       (column :a :integer)
       (column :b :integer))
    (is (= @(insert db :test [:a :b]
              (values [{:a 1 :b 2} {:b 3} {:c 3}])
              (returning *))
           [{:a 1 :b 2}
            {:a nil :b 3}
            {:a nil :b nil}]))))

(defn create-companies-table [db]
  (create-table db :companies
    (column :id :serial :primary-key? true)))

(defn create-exchanges-table [db]
  (create-table db :exchanges
    (column :id :serial :primary-key? true)))

(defn create-quotes-table [db]
  (create-table db :quotes
    (column :id :serial :primary-key? true)
    (column :exchange-id :integer :not-null? true :references :exchanges/id)
    (column :company-id :integer :references :companies/id)
    (column :symbol :citext :not-null? true :unique? true)
    (column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
    (column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))))

(deftest test-insert-fixed-columns-mixed-values-2
  (with-test-db [db]
    @(create-companies-table db)
    @(insert db :companies [:id]
       (values [{:id 5}]))
    @(create-exchanges-table db)
    @(insert db :exchanges [:id]
       (values [{:id 2}]))
    @(create-quotes-table db)
    (is (= @(insert db :quotes [:id :exchange-id :company-id
                                :symbol :created-at :updated-at]
              (values [{:updated-at #inst "2012-11-02T18:22:59.688-00:00"
                        :created-at #inst "2012-11-02T18:22:59.688-00:00"
                        :symbol "MSFT"
                        :exchange-id 2
                        :company-id 5
                        :id 5}
                       {:updated-at #inst "2012-11-02T18:22:59.688-00:00"
                        :created-at #inst "2012-11-02T18:22:59.688-00:00"
                        :symbol "SPY"
                        :exchange-id 2
                        :id 6}])
              (returning *))
           [{:updated-at #inst "2012-11-02T18:22:59.688-00:00",
             :created-at #inst "2012-11-02T18:22:59.688-00:00",
             :symbol "MSFT",
             :company-id 5,
             :exchange-id 2,
             :id 5}
            {:updated-at #inst "2012-11-02T18:22:59.688-00:00",
             :created-at #inst "2012-11-02T18:22:59.688-00:00",
             :symbol "SPY",
             :company-id nil,
             :exchange-id 2,
             :id 6}]))))

(deftest test-create-test-table
  (with-test-db [db]
    (is (= @(create-test-table db :empsalary)
           [{:count 0}]))))

(deftest test-insert-test-table
  (with-test-db [db]
    @(create-test-table db :empsalary)
    (is (= @(insert-test-table db :empsalary)
           [{:count 10}]))))

;; Window functions: http://www.postgresql.org/docs/9.4/static/tutorial-window.html

(deftest test-compare-salaries
  (with-test-db [db]
    (with-test-table db :empsalary
      (is (= @(select db [:depname :empno :salary '(over (avg :salary) (partition-by :depname))]
                (from :empsalary))
             [{:avg 5020.0000000000000000M
               :salary 5200
               :empno 11
               :depname "develop"}
              {:avg 5020.0000000000000000M
               :salary 4200
               :empno 7
               :depname "develop"}
              {:avg 5020.0000000000000000M
               :salary 4500
               :empno 9
               :depname "develop"}
              {:avg 5020.0000000000000000M
               :salary 6000
               :empno 8
               :depname "develop"}
              {:avg 5020.0000000000000000M
               :salary 5200
               :empno 10
               :depname "develop"}
              {:avg 3700.0000000000000000M
               :salary 3500
               :empno 5
               :depname "personnel"}
              {:avg 3700.0000000000000000M
               :salary 3900
               :empno 2
               :depname "personnel"}
              {:avg 4866.6666666666666667M
               :salary 4800
               :empno 3
               :depname "sales"}
              {:avg 4866.6666666666666667M
               :salary 5000
               :empno 1
               :depname "sales"}
              {:avg 4866.6666666666666667M
               :salary 4800
               :empno 4
               :depname "sales"}])))))

(deftest test-rank-over-order-by
  (with-test-db [db]
    (with-test-table db :empsalary
      (is (= @(select db [:depname :empno :salary '(over (rank) (partition-by :depname (order-by (desc :salary))))]
                (from :empsalary))
             [{:rank 1 :salary 6000 :empno 8 :depname "develop"}
              {:rank 2 :salary 5200 :empno 10 :depname "develop"}
              {:rank 2 :salary 5200 :empno 11 :depname "develop"}
              {:rank 4 :salary 4500 :empno 9 :depname "develop"}
              {:rank 5 :salary 4200 :empno 7 :depname "develop"}
              {:rank 1 :salary 3900 :empno 2 :depname "personnel"}
              {:rank 2 :salary 3500 :empno 5 :depname "personnel"}
              {:rank 1 :salary 5000 :empno 1 :depname "sales"}
              {:rank 2 :salary 4800 :empno 4 :depname "sales"}
              {:rank 2 :salary 4800 :empno 3 :depname "sales"}])))))

(deftest test-window-over-empty
  (with-test-db [db]
    (with-test-table db :empsalary
      (is (= @(select db [:salary '(over (sum :salary))]
                (from :empsalary))
             [{:sum 47100 :salary 5200}
              {:sum 47100 :salary 5000}
              {:sum 47100 :salary 3500}
              {:sum 47100 :salary 4800}
              {:sum 47100 :salary 3900}
              {:sum 47100 :salary 4200}
              {:sum 47100 :salary 4500}
              {:sum 47100 :salary 4800}
              {:sum 47100 :salary 6000}
              {:sum 47100 :salary 5200}])))))

(deftest test-window-sum-over-order-by
  (with-test-db [db]
    (with-test-table db :empsalary
      (is (= @(select db [:salary '(over (sum :salary) (order-by :salary))]
                (from :empsalary))
             [{:sum 3500 :salary 3500}
              {:sum 7400 :salary 3900}
              {:sum 11600 :salary 4200}
              {:sum 16100 :salary 4500}
              {:sum 25700 :salary 4800}
              {:sum 25700 :salary 4800}
              {:sum 30700 :salary 5000}
              {:sum 41100 :salary 5200}
              {:sum 41100 :salary 5200}
              {:sum 47100 :salary 6000}])))))

(deftest test-window-rank-over-partition-by
  (with-test-db [db]
    (with-test-table db :empsalary
      (is (= @(select db [:depname :empno :salary :enroll-date]
                (from (as (select db [:depname :empno :salary :enroll-date
                                      (as '(over (rank) (partition-by :depname (order-by (desc :salary) :empno))) :pos)]
                            (from :empsalary))
                          :ss))
                (where '(< pos 3)))
             [{:enroll-date #inst "2006-09-30T22:00:00.000-00:00"
               :salary 6000
               :empno 8
               :depname "develop"}
              {:enroll-date #inst "2007-07-31T22:00:00.000-00:00"
               :salary 5200
               :empno 10
               :depname "develop"}
              {:enroll-date #inst "2006-12-22T23:00:00.000-00:00"
               :salary 3900
               :empno 2
               :depname "personnel"}
              {:enroll-date #inst "2007-12-09T23:00:00.000-00:00"
               :salary 3500
               :empno 5
               :depname "personnel"}
              {:enroll-date #inst "2006-09-30T22:00:00.000-00:00"
               :salary 5000
               :empno 1
               :depname "sales"}
              {:enroll-date #inst "2007-07-31T22:00:00.000-00:00"
               :salary 4800
               :empno 3
               :depname "sales"}])))))

(deftest test-window-alias
  (with-test-db [db]
    (with-test-table db :empsalary
      (is (= @(select db ['(over (sum :salary) :w)
                          '(over (avg :salary) :w)]
                (from :empsalary)
                (window (as '(partition-by :depname (order-by (desc salary))) :w)))
             [{:avg 6000.0000000000000000M :sum 6000}
              {:avg 5466.6666666666666667M :sum 16400}
              {:avg 5466.6666666666666667M :sum 16400}
              {:avg 5225.0000000000000000M :sum 20900}
              {:avg 5020.0000000000000000M :sum 25100}
              {:avg 3900.0000000000000000M :sum 3900}
              {:avg 3700.0000000000000000M :sum 7400}
              {:avg 5000.0000000000000000M :sum 5000}
              {:avg 4866.6666666666666667M :sum 14600}
              {:avg 4866.6666666666666667M :sum 14600}])))))

(deftest test-insert-array
  (with-test-db [db]
    @(drop-table db [:test] (if-exists true))
    @(create-table db :test
       (column :x :text :array? true))
    (is (= @(insert db :test [:x]
              (values [{:x [1 2]}
                       {:x [3 4]}])
              (returning :*))
           [{:x ["1" "2"]}
            {:x ["3" "4"]}]))))

(deftest test-create-table-array-column
  (with-test-db [db]
    @(drop-table db [:test] (if-exists true))
    @(create-table db :test
       (column :x :text :array? true))))

;; (deftest test-select
;;   (with-backends [db]
;;     (setup-countries db)
;;     (is (= @(select db [:*]
;;               (from :countries))
;;            countries))))

;; (deftest test-select-columns
;;   (with-backends [db]
;;     (setup-countries db)
;;     (is (= @(select db [:id]
;;               (from :countries))
;;            (map #(select-keys % [:id]) countries)))))

;; (deftest test-select-where-equals
;;   (with-backends [db]
;;     (setup-countries db)
;;     (is (= @(select db [:id :name]
;;               (from :countries)
;;               (where '(= :name "Spain")))
;;            [{:id 1 :name "Spain"}]))))

;; (deftest test-insert
;;   (with-backends [db]
;;     (drop-countries db)
;;     (create-countries db)
;;     (is (= @(insert db :countries []
;;               (values countries))
;;            [{:count 4}]))))

;; (deftest test-insert-returning
;;   (with-backends [db]
;;     (drop-countries db)
;;     (create-countries db)
;;     (is (= @(insert db :countries []
;;               (values countries)
;;               (returning :*))
;;            countries))))

;; (deftest test-insert-returning-column
;;   (with-backends [db]
;;     (drop-countries db)
;;     (create-countries db)
;;     (is (= @(insert db :countries []
;;               (values countries)
;;               (returning :id))
;;            (map #(select-keys % [:id]) countries)))))

(deftest test-except
  (with-backends [db]
    (is (= @(except
             (select db [(as '(generate_series 1 3) :x)])
             (select db [(as '(generate_series 3 4) :x)]))
           [{:x 1} {:x 2}]))))

(deftest test-union
  (with-backends [db]
    (is (= @(union
             (select db [(as 1 :x)])
             (select db [(as 1 :x)]))
           [{:x 1}]))))

(deftest test-union-all
  (with-backends [db]
    (is (= @(union
             {:all true}
             (select db [(as 1 :x)])
             (select db [(as 1 :x)]))
           [{:x 1} {:x 1}]))))

(deftest test-intersect
  (with-backends [db]
    (is (= @(intersect
             (select db [(as '(generate_series 1 2) :x)])
             (select db [(as '(generate_series 2 3) :x)]))
           [{:x 2}]))))


(comment

  (def my-db (new-db "postgresql://tiger:scotch@localhost/datumbazo"))

  @(select my-db [1 "2" '(+ 1 2) '(+ (now) (cast "1 day" :interval))])

  @(select db [[1 2]])

  @(create-table db :countries
     (column :name :text)
     (column :code :text))

  @(insert db :countries []
     (values [{:code "es" :name "Spain"}]))

  @(drop-table db [:countries])

  @(select my-db [:*]
     (from :countries))

  @(select db [(as '(+ :id 10) :id)
               (as (upper-case :code) :code)]
     (from :countries))

  @(select db [(as '(+ :id 10) :id)
               (as '(upper :code) :code)]
     (from :countries))
  select * from information_schema.tables

  @(select db [:*]
     (from :countries)
     (order-by (asc :name)))

  (select db [:*]
    (from :countries)
    (order-by (asc :name)))


  (first @(select db [:*]
            (from :information-schema.tables)
            (where '(= :table-name "pg_statistic"))))

  (->> @(select db [:*]
          (from :information-schema.tables)
          (where '(= :table-name "pg_statistic"))
          )))
