(ns datumbazo.core-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clj-time.coerce :refer [to-timestamp]]
            [clj-time.core :refer [now]]
            [clojure.java.io :refer [file]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [upper-case]]
            [clojure.test :refer :all]
            [geo.postgis :as geo]
            [datumbazo.core :as sql]
            [datumbazo.driver.core :as driver]
            [datumbazo.io :refer :all]
            [datumbazo.test :refer :all]
            [datumbazo.validation :refer [new-record? uniqueness-of]]
            [inflections.core :refer [hyphenate underscore]]
            [slingshot.slingshot :refer [try+]]
            [validation.core :refer :all])
  (:import datumbazo.driver.clojure.Driver))

(defvalidate continent
  (presence-of :name)
  ;; TODO: Provide db via state
  ;; (uniqueness-of :continents :name :if new-record?)
  (presence-of :code)
  (exact-length-of :code 2)
  ;; TODO: Provide db via state
  ;; (uniqueness-of :continents :code :if new-record?)
  )

(sql/deftable continents
  "The continents database table."
  (sql/column :id :serial :primary-key? true)
  (sql/column :name :text :unique? true)
  (sql/column :code :text :unique? true)
  (sql/column :geometry :geometry :hidden? true)
  (sql/prepare validate-continent!))

(sql/deftable countries
  "The countries database table."
  (sql/column :id :serial :primary-key? true)
  (sql/column :continent-id :integer :references :continents/id)
  (sql/column :name :text :unique? true)
  (sql/column :geometry :geometry :hidden? true))

(sql/deftable ratings
  (sql/column :id :serial)
  (sql/column :user-id :integer :not-null? true :references :users/id)
  (sql/column :spot-id :integer :not-null? true :references :spots/id)
  (sql/column :rating :integer :not-null? true)
  (sql/column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
  (sql/column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))
  (sql/primary-key :user-id :spot-id :created-at))

(sql/deftable users
  (sql/column :id :integer)
  (sql/column :nick :varchar :size 255)
  (sql/primary-key :nick))

(deftest test-continent-by-pk*
  (with-backends [db]
    (is (= ["SELECT \"continents\".\"id\", \"continents\".\"name\", \"continents\".\"code\" FROM \"continents\" WHERE (\"continents\".\"id\" = 1)"]
           (sql/sql (continent-by-pk* db {:id 1}))))))

(deftest test-continent-by-pk
  (with-backends [db]
    (is (empty? (continent-by-pk db {:id 1})))))

;; TODO: Create table
(deftest test-rating-by-pk
  (with-backends [db]
    (let [time (now)]
      ;; (is (= [(str "SELECT \"ratings\".\"user_id\", \"ratings\".\"spot_id\", \"ratings\".\"rating\", \"ratings\".\"created_at\", \"ratings\".\"updated_at\" "
      ;;              "FROM \"ratings\" WHERE ((\"ratings\".\"user_id\" = ?) and (\"ratings\".\"spot_id\" = ?) and (\"ratings\".\"created_at\" = ?))")
      ;;         1 2 (to-timestamp time)]
      ;;        (sql (rating-by-pk* db {:user-id 1 :spot-id 2 :created-at time}))))
      (is (= [(str "SELECT \"ratings\".\"user-id\", \"ratings\".\"spot-id\", \"ratings\".\"rating\", \"ratings\".\"created-at\", \"ratings\".\"updated-at\" "
                   "FROM \"ratings\" WHERE ((\"ratings\".\"user-id\" = NULL) and (\"ratings\".\"spot-id\" = NULL) and (\"ratings\".\"created-at\" = NULL))")]
             (sql/sql (rating-by-pk* db {:user-id 1 :spot-id 2 :created-at time})))))))

(sql/deftable twitter-users
  "The Twitter users database table."
  (sql/table :twitter.users)
  (sql/column :id :bigint :primary-key? true)
  (sql/column :screen-name :text :not-null? true)
  (sql/column :name :text :not-null? true)
  (sql/column :followers-count :integer :not-null? true :default 0)
  (sql/column :friends-count :integer :not-null? true :default 0)
  (sql/column :retweet-count :integer :not-null? true :default 0)
  (sql/column :statuses-count :integer :not-null? true :default 0)
  (sql/column :verified :boolean :not-null? true :default false)
  (sql/column :possibly-sensitive :boolean :not-null? true :default false)
  (sql/column :location :text)
  (sql/column :time-zone :text)
  (sql/column :lang :varchar :size 2)
  (sql/column :url :text)
  (sql/column :profile-image-url :text)
  (sql/column :created-at :timestamp-with-time-zone :not-null? true :default "now()")
  (sql/column :updated-at :timestamp-with-time-zone :not-null? true :default "now()"))

(sql/deftable twitter-tweets
  "The Twitter tweets database table."
  (sql/table :twitter.tweets)
  (sql/column :id :bigint :primary-key? true)
  (sql/column :user-id :integer :references :twitter.users/id)
  (sql/column :retweeted :boolean :not-null? true :default false)
  (sql/column :text :text :not-null? true)
  (sql/column :created-at :timestamp-with-time-zone :not-null? true :default "now()")
  (sql/column :updated-at :timestamp-with-time-zone :not-null? true :default "now()"))

(sql/deftable tweets-users
  "The join table between tweets and users."
  (sql/table :twitter.tweets-users)
  (sql/column :user-id :integer :not-null? true :references :users/id)
  (sql/column :tweet-id :integer :not-null? true :references :tweets/id))

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
  (let [columns (sql/columns twitter-tweets-table)]
    (is (= 6 (count columns)))
    (is (= [:id :user-id :retweeted :text :created-at :updated-at]
           (map :name columns)))))

(deftest test-prefix
  (are [x y expected]
      (= (sql/prefix x y) expected)
    :a :b :a.b
    :continents :id :continents.id))

(deftest test-column-keys
  (is (= (sql/column-keys continents-table)
         [:id :name :code :geometry]))
  (is (= (sql/column-keys continents-table :prefix? true)
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
         (sql/sql (continents* db {:page 2 :per-page 20})))))

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
  (with-backends [db]
    (when (= (:backend db) 'jdbc.core)
      (is (= "Drop the continents database table."
             (:doc (meta #'drop-continents))))
      (drop-countries db)
      (is (= 0 (drop-continents db)))
      (is (= 0 (drop-continents db (sql/if-exists true)))))))

(deftest test-delete-continents
  (with-backends [db]
    (is (= "Delete all rows in the continents database table."
           (:doc (meta #'delete-continents))))
    (is (= 7 (delete-continents db)))
    (is (= 0 (delete-continents db)))
    (is (= 0 (sql/count-all db :continents)))))

(deftest test-delete-continent
  (with-backends [db]
    (is (= "Delete the continent from the database table."
           (:doc (meta #'delete-continent))))
    (let [europe (europe db)]
      (is (= 1 (delete-continent db europe)))
      (is (= 0 (delete-continent db europe))))))

(deftest test-delete-countries
  (with-backends [db]
    (is (= "Delete all rows in the countries database table."
           (:doc (meta #'delete-countries))))
    (is (= 2 (delete-countries db)))
    (is (= 0 (sql/count-all db :countries)))))

(deftest test-insert-continent
  (with-backends [db]
    (let [europe (europe db)]
      (delete-continent db europe)
      (let [row (insert-continent db europe)]
        (is (number? (:id row)))
        (is (= "Europe" (:name row)))
        (is (= "EU" (:code row)))
        (is (thrown? Exception (insert-continent db row)))))))

(deftest test-insert-continent-error
  (with-backends [db]
    (try+
     (insert-continent db {})
     (catch [:type :validation.core/error] {:keys [errors record]}
       (is (= {} record))
       (is (= ["can't be blank."] (:name errors)))
       (is (= ["has the wrong length (should be 2 characters)." "can't be blank."] (:code errors)))))))

(deftest test-insert-continents
  (with-backends [db]
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
  (with-backends [db]
    (is (= {:id 1} (sql/update-columns-best-row-identifiers db continents-table {:id 1 :name "Europe"})))
    (is (= {:user-id 8841372 :tweet-id 285415218434154496}
           (sql/update-columns-best-row-identifiers db tweets-users-table {:user-id 8841372 :tweet-id 285415218434154496})))))

(deftest test-update-columns-unique-columns
  (with-backends [db]
    (is (= {:id 1 :name "Europe"} (sql/update-columns-unique-columns db continents-table {:id 1 :name "Europe"})))
    (is (= {:user-id 8841372 :tweet-id 285415218434154496}
           (sql/update-columns-unique-columns db tweets-users-table {:user-id 8841372 :tweet-id 285415218434154496})))))

(deftest test-where-clause-columns
  (with-backends [db]
    (is (= {:id 1} (sql/where-clause-columns db continents-table {:id 1})))
    (is (= {:name "Europe"} (sql/where-clause-columns db continents-table {:name "Europe"})))
    (is (= {:user-id 8841372 :tweet-id 285415218434154496}
           (sql/where-clause-columns db tweets-users-table {:user-id 8841372 :tweet-id 285415218434154496})))))

(deftest test-update-clause
  (with-backends [db]
    (let [[_ clause] ((sql/update-clause db continents-table {:id 1}) nil)]
      (is (map? clause)))))

(deftest test-save-continent
  (with-backends [db]
    (let [row (save-continent db (europe db))]
      (is (number? (:id row)))
      (is (= "Europe" (:name row)))
      (is (= "EU" (:code row)))
      (is (= row (save-continent db row)))
      ;; (is (= row (save-continent db (dissoc row :id))))
      )))

(deftest test-truncate-continents
  (with-backends [db]
    (is (= "Truncate the continents database table."
           (:doc (meta #'truncate-continents))))
    (is (= 0 (truncate-continents db (sql/cascade true))))
    (is (= 0 (truncate-continents db (sql/cascade true) (sql/if-exists true))))
    (is (= 0 (sql/count-all db :continents)))))

(deftest test-truncate-countries
  (with-backends [db]
    (is (= "Truncate the countries database table."
           (:doc (meta #'truncate-countries))))
    (is (= 0 (truncate-countries db)))
    (is (= 0 (sql/count-all db :countries)))))

(deftest test-update-continent
  (with-backends [db]
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
  (with-backends [db]
    (is (= 7 (count (continents db))))
    (is (= [(africa db)] (continents db {:page 1 :per-page 1 :order-by :name})))
    (is (= [(antarctica db)] (continents db {:page 2 :per-page 1 :order-by :name})))))

(deftest test-countries
  (with-backends [db]
    (is (= 2 (count (countries db))))))

(deftest test-countries-by-continent-id
  (with-backends [db]
    (is (= (countries-by-continent-id db (:id (europe db)))
           (countries-by-name db "Spain")))))

(deftest test-continent-by-id
  (with-backends [db]
    (is (nil? (continent-by-id db nil)))
    (is (nil? (continent-by-id db -1)))
    (is (= (europe db) (continent-by-id db (:id (europe db)))))
    (is (= (europe db) (continent-by-id db (str (:id (europe db))))))
    (is (= (europe db) (continent-by-id db (str (:id (europe db)) "-europe"))))))

(deftest test-continent-by-name
  (with-backends [db]
    (is (nil? (continent-by-name db nil)))
    (is (nil? (continent-by-name db "unknown")))
    (is (= (europe db) (continent-by-name db (:name (europe db)))))))

(deftest test-continents-by-id
  (with-backends [db]
    (is (empty? (continents-by-id db -1)))
    (is (empty? (continents-by-id db "-1")))
    (is (= ["SELECT \"continents\".\"id\", \"continents\".\"name\", \"continents\".\"code\" FROM \"continents\" WHERE (\"continents\".\"id\" = -1)"]
           (sql/sql (continents-by-id* db -1))))
    (is (= [(europe db)] (continents-by-id db (:id (europe db)))))
    (is (= [(europe db)] (continents-by-id db (str (:id (europe db))))))))

(deftest test-continents-by-name
  (with-backends [db]
    (is (empty? (continents-by-name db nil)))
    (is (= [(continent-by-name db "Europe")] (continents-by-name db "Europe")))
    (is (= ["SELECT \"continents\".\"id\", \"continents\".\"name\", \"continents\".\"code\" FROM \"continents\" WHERE (\"continents\".\"name\" = ?)" (citext "Europe")]
           (sql/sql (continents-by-name* db "Europe"))))
    (is (= [(europe db)] (continents-by-name db (:name (europe db)))))
    (is (= (continents-by-name db (:name (europe db)))
           (continents-by-name db (upper-case (:name (europe db))))))))

(deftest test-twitter-users
  (with-backends [db]
    (is (= 33 (count (twitter-users db))))))

(deftest test-twitter-tweets
  (with-backends [db]
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
  (with-backends [db]
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
  (with-backends [db]
    (is (= 7 (sql/count-all db :continents)))))

(deftest test-insert-twitter-user
  (with-backends [db]
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
  (with-backends [db]
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
    (is (instance? datumbazo.driver.clojure.Driver (:driver db)))
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
    (is (instance? datumbazo.driver.clojure.Driver (:driver db)))
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
    (is (= @(create-test-table db :empsalary)
           [{:count 0}]))))

(deftest test-insert-test-table
  (with-backends [db]
    @(create-test-table db :empsalary)
    (is (= @(insert-test-table db :empsalary)
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
    (is (= @(sql/except
             (sql/select db [(sql/as '(generate_series 1 3) :x)])
             (sql/select db [(sql/as '(generate_series 3 4) :x)]))
           [{:x 1} {:x 2}]))))

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
  (with-drivers [db db {:test? false}]
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
  (with-drivers [db db {:test? false}]
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
