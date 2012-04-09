(ns database.test.core
  (:import org.joda.time.DateTime org.postgresql.util.PSQLException)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.internal :as internal])
  (:use [korma.sql.fns :only (pred-and pred-or)]
        clojure.test
        database.core
        database.columns
        database.tables
        database.postgis
        database.test
        database.fixtures))

(database-test test-add-column
  (let [column (make-column :x :integer)]
    (is (= column (add-column :languages column)))))

;; (deftest test-column-prefix
;;   (are [table column expected]
;;     (is (= expected (column-prefix table column)))
;;     :languages :name :languages-name))

(database-test test-drop-column
  (is (drop-column :languages :created-at)))

(database-test test-create-table-with-languages
  (drop-table :languages)
  (is (instance? database.tables.Table (create-table :languages)))
  (is (thrown? Exception (create-table :languages))))

(database-test test-create-table-with-photo-thumbnails
  (drop-table (table :photo-thumbnails))
  (is (instance? database.tables.Table (create-table (table :photo-thumbnails))))
  (is (thrown? Exception (create-table (table :photo-thumbnails)))))

(database-test test-delete-record
  (is (nil? (delete-record :languages nil)))
  (is (nil? (delete-record :languages {})))
  (let [record (insert-record :languages german)]
    (is (= record (delete-record :languages record)))
    (insert-record :languages german)))

(database-test test-delete-all
  (let [language (insert-record :languages german)]
    (is (= 1 (delete-all :languages)))
    (is (= 0 (delete-all :languages)))))

(database-test test-delete-where
  (let [language (insert-record :languages german)]
    (is (= 1 (delete-where :languages ["name = ?" (:name language)])))
    (is (= 0 (delete-where :languages ["name = ?" (:name language)])))
    (insert-record :languages german)))

(database-test test-drop-table
  (is (drop-table :languages))
  (is (drop-table :languages :if-exists true))
  (is (thrown? Exception (drop-table :languages))))

(deftest test-new-record?
  (is (new-record? {}))
  (is (new-record? {:id nil}))
  (is (new-record? {:id ""}))
  (is (not (new-record? {:id 1})))
  (is (not (new-record? {:id "1"}))))

(database-test test-text=
  (let [language (insert-record :languages german)]
    (is (= language (first (select (entity :languages) (where {:name [text= (:name language)]})))))))

(deftest test-entity
  (let [entity (entity :languages)]
    (is (= [:updated-at :created-at :iso-639-2 :iso-639-1 :family :name :id]
           (:fields entity)))))

(database-test test-record-exists?
  (is (not (record-exists? :languages {})))
  (is (not (record-exists? :languages german)))
  (let [german (insert-record :languages german)]
    (is (record-exists? :languages german))))

(database-test test-reload-record
  (is (nil? (reload-record :languages {})))
  (let [language (insert-record :languages german)]
    (is (= language (reload-record :languages language)))))

(database-test test-insert-record
  (is (thrown? Exception (insert-record :languages {})))
  (let [record (insert-record :languages german)]
    (is (number? (:id record)))
    (is (= "German" (:name record)))
    (is (= "Indo-European" (:family record)))
    (is (= "de" (:iso-639-1 record)))
    (is (= "deu" (:iso-639-2 record)))
    (is (= "http://example.com/languages/1-German" (:url record)))
    (is (instance? DateTime (:created-at record)))
    (is (instance? DateTime (:updated-at record))))
  ;; (is (thrown? Exception (insert-record :languages german)))
  )

(database-test test-update-record
  (is (thrown? Exception (update-record :languages {})))
  (is (nil? (update-record :languages german)))
  (let [record (update-record :languages (assoc (insert-record :languages german) :name "Deutsch"))]
    (is (number? (:id record)))
    (is (= "Deutsch" (:name record)))
    (is (= "Indo-European" (:family record)))
    (is (= "de" (:iso-639-1 record)))
    (is (= "deu" (:iso-639-2 record)))
    (is (= "http://example.com/languages/1-Deutsch" (:url record)))
    (is (instance? DateTime (:created-at record)))
    (is (instance? DateTime (:updated-at record)))
    (is (= record (update-record :languages record)))))

(database-test test-save-record
  (is (thrown? Exception (save-record :languages {})))
  (let [language (save-record :languages german)]
    (is (pos? (:id language)))
    (is (= language (save-record :languages language)))))

(deftest test-table
  (is (table? (table :languages)))
  (is (= :languages (:name (table :languages))))
  (is (= (table :languages) (table (table :languages)))))

(deftest test-all-clause
  (is (= (pred-and {:role-id 2} {:user-id 1})
         (all-clause :roles-users bodhi-is-surfer))))

(deftest test-unique-key-clause
  (are [record expected]
    (is (= expected (unique-key-clause :languages record)))
    {} (pred-or)
    {:id 1} (pred-or {:id 1})
    ;; TODO: How?
    ;; {:id 1 :iso-639-1 "de"} (pred-or {:id 1} {:iso-639-1 "de"})
    ;; {:id 1 :iso-639-1 "de" :undefined "column"} {:id 1 :iso-639-1 "de"}
    ))

(deftest test-where-clause
  (is (= (all-clause :roles-users bodhi-is-surfer)
         (where-clause :roles-users bodhi-is-surfer)))
  (is (= (unique-key-clause :users bodhi)
         (where-clause :users bodhi))))

(database-test test-uniqueness-of
  (let [validate-iso-639-1 (uniqueness-of :languages :iso-639-1)]
    (is (nil? (meta (validate-iso-639-1 german))))
    (insert-record :languages german)
    (is (= {:errors {:iso-639-1 ["has already been taken."]}}
           (meta (validate-iso-639-1 german))))))

(deftest test-where-text=
  (is (= (str "SELECT \"languages\".* FROM \"languages\" WHERE (TO_TSVECTOR(CAST(? AS regconfig), "
              "CAST(\"languages\".\"name\" AS text)) @@ PLAINTO_TSQUERY(CAST(? AS regconfig), CAST(? AS text)))")
         (sql-only (select :languages (where-text= :name "x"))))))

(database-test test-defquery
  (let [german (save-language german)
        spanish (save-language spanish)]
    (defquery example-query "Doc" [arg-1 & options]
      (-> (languages*) (order :name)))
    (is (= [german spanish] (example-query "arg-1")))
    (is (= [german] (example-query "arg-1" :page 1 :per-page 1)))
    (is (= [spanish] (example-query "arg-1" :page 2 :per-page 1)))))


(database-test test-shift-fields
  (insert-fixtures)
  (let [users (select (entity :users)
                      (korma.core/join :countries (= :countries.id :users.country-id))
                      (shift-fields :countries :country [:id :name :created-at :updated-at]))]
    (is (= 2 (count users)))
    (let [user (first users)]
      (is (= (:id bodhi) (:id user)))
      (is (= (:country-id bodhi) (:country-id user)))
      (is (= (:nick bodhi) (:nick user)))
      (is (= (:email bodhi) (:email user)))
      (is (= (:crypted-password bodhi) (:crypted-password user)))
      (is (instance? DateTime (:created-at user)))
      (is (instance? DateTime (:updated-at user)))
      (let [country (:country user)]
        (is (= (:id usa) (:id country)))
        (is (= (:name usa) (:name country)))
        (is (instance? DateTime (:created-at country)))
        (is (instance? DateTime (:updated-at country)))))))

(database-test test-join
  (insert-fixtures)
  (= (select (entity :users)
             (korma.core/join :countries (= :countries.id :users.country-id))
             (shift-fields :countries :country [:id :name :created-at :updated-at]))
     (select (entity :users) (join :countries (= :countries.id :users.country-id))))
  (let [result (select (entity :users) (join :left :countries (= :countries.id :users.country-id) [:id :name]))]
    (is (= {:id 1 :name "United States"} (:country (first result))))))
