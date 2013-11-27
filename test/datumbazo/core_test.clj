(ns datumbazo.core-test
  (:refer-clojure :exclude [distinct group-by])
  (:require [clj-time.core :refer [now]]
            [clj-time.coerce :refer [to-timestamp]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [upper-case]]
            [clojure.pprint :refer [pprint]]
            [clojure.java.io :refer [file]]
            [environ.core :refer [env]]
            [validation.core :refer :all]
            [datumbazo.validation :refer [new-record? uniqueness-of]]
            [slingshot.slingshot :refer [try+]])
  (:use clojure.test
        datumbazo.core
        datumbazo.io
        datumbazo.test))

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
  (is (= ["SELECT \"continents\".\"id\", \"continents\".\"name\", \"continents\".\"code\" FROM \"continents\" WHERE (\"continents\".\"id\" = 1)"]
         (sql (continent-by-pk* nil {:id 1})))))

(database-test test-continent-by-pk
  (is (empty? (continent-by-pk db {:id 1}))))

(deftest test-rating-by-pk
  (let [time (now)]
    (is (= [(str "SELECT \"ratings\".\"user_id\", \"ratings\".\"spot_id\", \"ratings\".\"rating\", \"ratings\".\"created_at\", \"ratings\".\"updated_at\" "
                 "FROM \"ratings\" WHERE ((\"ratings\".\"user_id\" = 1) and (\"ratings\".\"spot_id\" = 2) and (\"ratings\".\"created_at\" = ?))")
            (to-timestamp time)]
           (sql (rating-by-pk* db {:user-id 1 :spot-id 2 :created-at time}))))))

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

(database-test test-drop-continents
  (is (= "Drop the continents database table."
         (:doc (meta #'drop-continents))))
  (drop-countries db)
  (is (= 0 (drop-continents db)))
  (is (= 0 (drop-continents db (if-exists true)))))

(database-test test-delete-continents
  (is (= "Delete all rows in the continents database table."
         (:doc (meta #'delete-continents))))
  (is (= 7 (delete-continents db)))
  (is (= 0 (delete-continents db)))
  (is (= 0 (count-all db :continents))))

(database-test test-delete-continent
  (is (= "Delete the continent from the database table."
         (:doc (meta #'delete-continent))))
  (let [europe (europe db)]
    (is (= 1 (delete-continent db europe)))
    (is (= 0 (delete-continent db europe)))))

(database-test test-delete-countries
  (is (= "Delete all rows in the countries database table."
         (:doc (meta #'delete-countries))))
  (is (= 0 (delete-countries db)))
  (is (= 0 (count-all db :countries))))

(database-test test-insert-continent
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
      (is (thrown? Exception (insert-continent db row))))))

(database-test test-insert-continents
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
        (is (= "EU" (:code row)))))))

(database-test test-update-columns-best-row-identifiers
  (is (= {:id 1} (update-columns-best-row-identifiers db continents-table {:id 1 :name "Europe"})))
  (is (= {:user-id 8841372 :tweet-id 285415218434154496}
         (update-columns-best-row-identifiers db tweets-users-table {:user-id 8841372 :tweet-id 285415218434154496}))))

(database-test test-update-columns-unique-columns
  (is (= {:id 1 :name "Europe"} (update-columns-unique-columns db continents-table {:id 1 :name "Europe"})))
  (is (= {:user-id 8841372 :tweet-id 285415218434154496}
         (update-columns-unique-columns db tweets-users-table {:user-id 8841372 :tweet-id 285415218434154496}))))

(database-test test-where-clause-columns
  (is (= {:id 1} (where-clause-columns db continents-table {:id 1})))
  (is (= {:name "Europe"} (where-clause-columns db continents-table {:name "Europe"})))
  (is (= {:user-id 8841372 :tweet-id 285415218434154496}
         (where-clause-columns db tweets-users-table {:user-id 8841372 :tweet-id 285415218434154496}))))

(database-test test-update-clause
  (let [[_ clause] ((update-clause db continents-table {:id 1}) nil)]
    (is (map? clause))))

;; ;; (database-test test-save-tweets-users
;; ;;   #_(save-tweets-user db {:tweet-id 1 :user-id 1}))

(database-test test-create-table-compound-primary-key
  (is (= (run db (create-table :ratings
                   (column :id :serial)
                   (column :user-id :integer :not-null? true :references :users/id)
                   (column :spot-id :integer :not-null? true :references :spots/id)
                   (column :rating :integer :not-null? true)
                   (column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
                   (column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))
                   (primary-key :user-id :spot-id :created-at)))
         [{:count 0}])))

(database-test test-save-continent
  (let [row (save-continent db (europe db))]
    (is (number? (:id row)))
    (is (= "Europe" (:name row)))
    (is (= "EU" (:code row)))
    (is (= row (save-continent db row)))
    ;; (is (= row (save-continent db (dissoc row :id))))
    ))

(database-test test-truncate-continents
  (is (= "Truncate the continents database table."
         (:doc (meta #'truncate-continents))))
  (is (= 0 (truncate-continents db (cascade true))))
  (is (= 0 (truncate-continents db (cascade true) (if-exists true) )))
  (is (= 0 (count-all db :continents))))

(database-test test-truncate-countries
  (is (= "Truncate the countries database table."
         (:doc (meta #'truncate-countries))))
  (is (= 0 (truncate-countries db)))
  (is (= 0 (count-all db :countries))))

(database-test test-update-continent
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
      (is (= "EU" (:code continent))))))

(database-test test-continents
  (is (= 7 (count (continents db))))
  (is (= [(africa db)] (continents db {:page 1 :per-page 1 :order-by :name})))
  (is (= [(antarctica db)] (continents db {:page 2 :per-page 1 :order-by :name}))))

(database-test test-countries
  (is (= 0 (count (countries db)))))

(database-test test-countries-by-continent-id
  (is (empty? (countries-by-continent-id db (:id (europe db))))))

(database-test test-continent-by-id
  (is (nil? (continent-by-id db nil)))
  (is (nil? (continent-by-id db -1)))
  (is (= (europe db) (continent-by-id db (:id (europe db)))))
  (is (= (europe db) (continent-by-id db (str (:id (europe db))))))
  (is (= (europe db) (continent-by-id db (str (:id (europe db)) "-europe")))))

(database-test test-continent-by-name
  (is (nil? (continent-by-name db nil)))
  (is (nil? (continent-by-name db "unknown")))
  (is (= (europe db) (continent-by-name db (:name (europe db))))))

(database-test test-continents-by-id
  (is (empty? (continents-by-id db -1)))
  (is (empty? (continents-by-id db "-1")))
  (is (= ["SELECT \"continents\".\"id\", \"continents\".\"name\", \"continents\".\"code\" FROM \"continents\" WHERE (\"continents\".\"id\" = ?)" -1]
         (sql (continents-by-id* db -1))))
  (is (= [(europe db)] (continents-by-id db (:id (europe db)))))
  (is (= [(europe db)] (continents-by-id db (str (:id (europe db)))))))

(database-test test-continents-by-name
  (is (empty? (continents-by-name db nil)))
  (is (= [(continent-by-name db "Europe")] (continents-by-name db "Europe")))
  (is (= ["SELECT \"continents\".\"id\", \"continents\".\"name\", \"continents\".\"code\" FROM \"continents\" WHERE (\"continents\".\"name\" = ?)" (citext "Europe")]
         (sql (continents-by-name* db "Europe"))))
  (is (= [(europe db)] (continents-by-name db (:name (europe db)))))
  (is (= (continents-by-name db (:name (europe db)))
         (continents-by-name db (upper-case (:name (europe db)))))))

(database-test test-twitter-users
  (is (= 33 (count (twitter-users db)))))

(database-test test-twitter-tweets
  (is (= 23 (count (twitter-tweets db)))))

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
  (let [user (->> {:created-at #inst "2011-02-22T06:29:06.000-00:00"
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
                   :verified false}
                  (save-twitter-user db))]
    (is (= (dissoc user :updated-at)
           (dissoc (save-twitter-user db user) :updated-at)))))

(database-test test-count-all
  (is (= 7 (count-all db :continents))))

(database-test test-insert-twitter-user
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
    (is (= 9 (:id user)))))

(database-test test-validation
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
       (is (= errors (:errors (meta record))))))))

(database-test test-array
  (is (= [{:array [1 2]}]
         (run db (select [[1 2]])))))

(database-test test-array-concat
  (is (= [{:?column? [1 2 3 4 5 6]}]
         (run db (select ['(|| [1 2] [3 4] [5 6])])))))

;; RUN

;; RAW SQL

(database-test test-sql-str
  (is (= "SELECT 1, 'a'" (sql-str db (select [1 "a"])))))

;; PostgreSQL JSON Support Functions

(database-test test-array-to-json
  (is (= [{:array-to-json [[1 5] [99 100]]}]
         (run db (select [`(array_to_json (cast "{{1,5},{99,100}}" ~(keyword "int[]")))])))))

(database-test test-row-to-json
  (is (= [{:row-to-json {:f1 1, :f2 "foo"}}]
         (run db (select ['(row_to_json (row 1 "foo"))])))))

(database-test test-to-json
  (is (= [{:to-json "Fred said \"Hi.\""}]
         (run db (select ['(to_json "Fred said \"Hi.\"")])))))

(database-test test-with
  (is (= (run db (with [:x (select [:*] (from :continents))]
                       (select [:*] (from :x))))
         (run db (select [:*] (from :continents))))))

;; VENDOR TESTS

(defvendor-test test-cast-string-to-int
  (is (= (run db (select [`(cast "1" :integer)]))
         [(case vendor
            :mysql {(keyword "CAST('1' AS integer)") 1}
            :postgresql {:int4 1}
            :sqlite {(keyword "CAST(? AS integer)") 1})])))

(defvendor-test test-run1
  (is (= (run1 db (select [1 2 3]))
         (first (run db (select [1 2 3]))))))

(defvendor-test test-select-1
  (is (= (run db (select [1]))
         [(case vendor
            :postgresql {:?column? 1}
            {:1 1})])))

(defvendor-test test-select-1-2-3
  (is (= (run db (select [1 2 3]))
         [(case vendor
            :postgresql {:?column? 1 :?column?-2 2 :?column?-3 3}
            {:1 1 :2 2 :3 3})])))

(defvendor-test test-select-1-as-n
  (is (= (run db (select [(as 1 :n)]))
         [{:n 1}])))

(defvendor-test test-select-x-as-n
  (is (= (run db (select [(as "x" :n)]))
         [{:n "x"}])))

(defvendor-test test-test-select-1-2-3-as
  (is (= (run db (select [(as 1 :a) (as 2 :b) (as 3 :c)]))
         [{:a 1, :b 2, :c 3}])))

(defvendor-test test-concat-strings
  (is (= [(case vendor
            :mysql {(keyword "('a' || 'b' || 'c')") 0} ;; not string concat, but OR operator
            :postgresql {:?column? "abc"}
            :sqlite {(keyword "(? || ? || ?)") "abc"})]
         (run db (select ['(|| "a" "b" "c")])))))

(defvendor-test test-create-table
  (let [table :test-create-table]
    (try (is (= (run db (create-table table
                          (column :id :integer)
                          (column :nick :varchar :length 255)
                          (primary-key :nick)))
                (if (= :sqlite vendor)
                  [] [{:count 0}])))
         ;; (is (empty? (run db (select [:*] (from table)))))
         (finally
           ;; Cleanup for MySQL (non-transactional DDL)
           (run db (drop-table [table] (if-exists true)))))))

(defvendor-test test-drop-table-if-exists
  (is (= (run db (drop-table [:not-existing]
                   (if-exists true)))
         (if (= :sqlite vendor)
           [] [{:count 0}]))))
