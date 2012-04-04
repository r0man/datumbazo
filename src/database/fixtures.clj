(ns database.fixtures
  (:use [clojure.string :only (lower-case)]
        [migrate.core :only (defmigration)]
        database.core
        validation.core))

(defn language-url [language]
  (if (:id language)
    (format "http://example.com/languages/%s-%s" (:id language) (:name language))))

;; VALIDATIONS

(defvalidate country
  (presence-of :name)
  (uniqueness-of :countries :name :if new-record?))

(defvalidate language
  (presence-of :name)
  (min-length-of :name 2)
  (max-length-of :name 64)
  (presence-of :family)
  (min-length-of :family 2)
  (max-length-of :family 64)
  (presence-of :iso-639-1)
  (exact-length-of :iso-639-1 2)
  (presence-of :iso-639-2)
  (exact-length-of :iso-639-2 3))

(defvalidate photo
  (presence-of :title)
  (min-length-of :title 2)
  (max-length-of :title 64))

(defvalidate photo-thumbnail
  (presence-of :photo-id)
  (presence-of :url)
  (presence-of :width)
  (presence-of :heigth))

(defvalidate role
  (presence-of :name)
  (uniqueness-of :roles :name :if new-record?)
  (min-length-of :name 2)
  (max-length-of :name 32))

(defvalidate user
  (presence-of :nick)
  (min-length-of :nick 2)
  (max-length-of :nick 16)
  (uniqueness-of :users :nick :if new-record?)
  (presence-of :email)
  (is-email :email)
  (uniqueness-of :users :email :if new-record?))

;; TABLES

(deftable countries
  [[:id :serial :primary-key? true]
   [:name :text :not-null? true :unique? true]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]]
  :validate validate-country!)

(deftable languages
  [[:id :serial :primary-key? true]
   [:name :text :unique? true :not-null? true]
   [:family :text :not-null? true]
   [:iso-639-1 :varchar :length 2 :unique? true :not-null? true :serialize #'lower-case]
   [:iso-639-2 :varchar :length 3 :unique? true :not-null? true :serialize #'lower-case]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]]
  :transforms [#(assoc %1 :url (language-url %1))]
  :validate validate-language!)

(deftable photos
  [[:id :serial :primary-key? true]
   [:title :text :not-null? true]
   [:taken-at :timestamp-with-time-zone]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]]
  :validate validate-photo!)

(deftable photo-thumbnails
  [[:id :serial :primary-key? true]
   [:photo-id :integer :references :photos/id :not-null? true]
   [:url :text :not-null? true]
   [:width :integer :not-null? true]
   [:heigth :integer :not-null? true]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]]
  :validate validate-photo-thumbnail!)

(deftable roles
  [[:id :serial :primary-key? true]
   [:name :text :unique? true :not-null? true]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]]
  :validate validate-role!)

(deftable users
  [[:id :serial :primary-key? true]
   [:country-id :integer :references :countries/id :not-null? true]
   [:nick :text :not-null? true :unique? true]
   [:email :text :not-null? true :unique? true]
   [:crypted-password :text]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]]
  :validate validate-user!)

(deftable roles-users
  [[:role-id :integer :references :roles/id :not-null? true]
   [:user-id :integer :references :users/id :not-null? true]])

;; MIGRATIONS

(defmigration "2012-01-01T00:00:00"
  "Create the countries table."
  (create-table (table :countries))
  (drop-table (table :countries)))

(defmigration "2012-01-01T00:01:00"
  "Create the languages table."
  (create-table (table :languages))
  (drop-table (table :languages)))

(defmigration "2012-01-01T00:02:00"
  "Create the photos table."
  (create-table (table :photos))
  (drop-table (table :photos)))

(defmigration "2012-01-01T00:03:00"
  "Create the photo thumbnails table."
  (create-table (table :photo-thumbnails))
  (drop-table (table :photo-thumbnails)))

(defmigration "2012-01-01T00:04:00"
  "Create the roles table."
  (create-table (table :roles))
  (drop-table (table :roles)))

(defmigration "2012-01-01T00:05:00"
  "Create the users table."
  (create-table (table :users))
  (drop-table (table :users)))

(defmigration "2012-01-01T00:06:00"
  "Create the join table between roles and users."
  (create-table (table :roles-users))
  (drop-table (table :roles-users)))

;; COUNTRIES

(def usa
  (make-language
   :id 1
   :name "United States"))

;; LANGUAGES

(def german
  (make-language
   :id 1
   :name "German"
   :family "Indo-European"
   :iso-639-1 "DE"
   :iso-639-2 "DEU"))

(def spanish
  (make-language
   :id 2
   :name "Spanish"
   :family "Indo-European"
   :iso-639-1 "ES"
   :iso-639-2 "ESP"))

;; PHOTOS

(def street-art-berlin
  (make-photo
   :id 1
   :title "Street Art Berlin"))

;; ROLES

(def administrator
  (make-role :id 1 :name "Administrator"))

(def surfer
  (make-role :id 2 :name "Surfer"))

;; USERS

(def bodhi
  (make-user
   :id 1
   :country-id (:id usa)
   :nick "Bodhi"
   :email "bodhi@example.com"
   :crypted-password "$2a$10$ajTnLiS/sGn7XrK/FrhPE.xSBiUiu60wIksc3RxDpTcMPAovfZ.5q" ; secret
   ))

(def bodhi-is-surfer
  {:user-id (:id bodhi) :role-id (:id surfer)})

(def roach-is-surfer
  {:user-id (:id bodhi) :role-id (:id surfer)})

(def roach
  (make-user
   :id 2
   :country-id (:id usa)
   :nick "Roach"
   :email "roach@example.com"
   :crypted-password "$2a$10$ajTnLiS/sGn7XrK/FrhPE.xSBiUiu60wIksc3RxDpTcMPAovfZ.5q" ; secret
   ))

(defn insert-fixtures []
  (save-country usa)
  (save-role surfer)
  (save-user bodhi)
  (save-user roach)
  (save-roles-user {:role-id (:id surfer) :user-id (:id bodhi)})
  (save-roles-user {:role-id (:id surfer) :user-id (:id roach)}))

;; (insert-fixtures)