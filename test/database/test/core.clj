(ns database.test.core
  (:import org.joda.time.DateTime org.postgresql.util.PSQLException)
  (:require [clojure.java.jdbc :as jdbc])
  (:use [korma.sql.fns :only (pred-and pred-or)]
        clojure.test
        database.core
        database.connection
        database.columns
        database.tables
        ;; database.postgis
        database.test
        database.fixtures
        database.ddl))

(deftest test-create-extension
  (with-redefs [jdbc/do-commands #(is (= "CREATE EXTENSION hstore" %1))]
    (create-extension :hstore)))

(deftest test-drop-extension
  (with-redefs [jdbc/do-commands #(is (= "DROP EXTENSION hstore" %1))]
    (drop-extension :hstore)))

;; (deftest test-index-name
;;   (are [table columns name]
;;     (is (= name (index-name table columns)))
;;     :continents [:name] "continents_name_idx"
;;     :oauth_nonces [:nonce :created_at] "oauth_nonces_nonce_created_at_idx"))

;; (database-test test-create-index
;;   (is (create-index :continents [:id :name])))

;; (database-test test-add-column
;;   (let [column (make-column :x :integer)]
;;     (is (= (assoc column :table (table :wikipedia.languages))
;;            (add-column :wikipedia.languages column)))))

;; (database-test test-drop-column
;;   (is (drop-column :wikipedia.languages :created-at)))

;; (database-test test-create-table-with-languages
;;   (drop-table :wikipedia.languages)
;;   (is (instance? database.tables.Table (create-table :wikipedia.languages)))
;;   (is (thrown? Exception (create-table :wikipedia.languages))))

;; (database-test test-create-table-with-photo-thumbnails
;;   (drop-table (table :photo-thumbnails))
;;   (is (instance? database.tables.Table (create-table (table :photo-thumbnails))))
;;   (is (thrown? Exception (create-table (table :photo-thumbnails)))))

;; (database-test test-delete-record
;;   (is (nil? (delete-record :wikipedia.languages nil)))
;;   (is (nil? (delete-record :wikipedia.languages {})))
;;   (let [record (insert-record :wikipedia.languages german)]
;;     (is (= record (delete-record :wikipedia.languages record)))
;;     (insert-record :wikipedia.languages german)))

(deftest test-delete-all
  (with-redefs [jdbc/do-commands #(do (is (= "DELETE FROM public.continents" %1)) [0])]
    (is (= 0 (delete-all :continents))))
  (with-redefs [jdbc/do-commands #(do (is (= "DELETE FROM wikipedia.languages" %1)) [0])]
    (is (= 0 (delete-all :wikipedia.languages))))
  (with-quoted-identifiers \"
    (with-redefs [jdbc/do-commands #(do (is (= "DELETE FROM \"public\".\"continents\"" %1)) [0])]
      (is (= 0 (delete-all :continents))))
    (with-redefs [jdbc/do-commands #(do (is (= "DELETE FROM \"wikipedia\".\"languages\"" %1)) [0])]
      (is (= 0 (delete-all :wikipedia.languages))))))

(deftest test-truncate-table
  (with-redefs [jdbc/do-commands #(is (= "TRUNCATE public.continents" %1))]
    (truncate-table :continents))
  (with-redefs [jdbc/do-commands #(is (= "TRUNCATE wikipedia.languages" %1))]
    (truncate-table :wikipedia.languages))
  (with-quoted-identifiers \"
    (with-redefs [jdbc/do-commands #(is (= "TRUNCATE \"public\".\"continents\"" %1))]
      (truncate-table :continents))
    (with-redefs [jdbc/do-commands #(is (= "TRUNCATE \"wikipedia\".\"languages\"" %1))]
      (truncate-table :wikipedia.languages))))

;; (database-test test-delete-where
;;   (let [language (insert-record :wikipedia.languages german)]
;;     (is (= 1 (delete-where :wikipedia.languages ["name = ?" (:name language)])))
;;     (is (= 0 (delete-where :wikipedia.languages ["name = ?" (:name language)])))
;;     (insert-record :wikipedia.languages german)))

;; (database-test test-drop-table
;;   (is (drop-table :wikipedia.languages))
;;   (is (drop-table :wikipedia.languages :if-exists true))
;;   (is (thrown? Exception (drop-table :wikipedia.languages))))

(deftest test-new-record?
  (is (new-record? {}))
  (is (new-record? {:id nil}))
  (is (new-record? {:id ""}))
  (is (not (new-record? {:id 1})))
  (is (not (new-record? {:id "1"}))))

;; (database-test test-text=
;;   (let [language (insert-record :wikipedia.languages german)]
;;     (is (= language (first (select (entity :wikipedia.languages) (where {:name [text= (:name language)]})))))))

;; (deftest test-entity
;;   (let [entity (entity :wikipedia.languages)]
;;     (is (= [:updated-at :created-at :iso-639-2 :iso-639-1 :family :name :id]
;;            (:fields entity)))))

;; (database-test test-record-exists?
;;   (is (not (record-exists? :wikipedia.languages {})))
;;   (is (not (record-exists? :wikipedia.languages german)))
;;   (let [german (insert-record :wikipedia.languages german)]
;;     (is (record-exists? :wikipedia.languages german))))

;; (database-test test-reload-record
;;   (is (nil? (reload-record :wikipedia.languages {})))
;;   (let [language (insert-record :wikipedia.languages german)]
;;     (is (= language (reload-record :wikipedia.languages language)))))

;; (database-test test-insert-record
;;   (is (thrown? Exception (insert-record :wikipedia.languages {})))
;;   (let [record (insert-record :wikipedia.languages german)]
;;     (is (number? (:id record)))
;;     (is (= "German" (:name record)))
;;     (is (= "Indo-European" (:family record)))
;;     (is (= "de" (:iso-639-1 record)))
;;     (is (= "deu" (:iso-639-2 record)))
;;     (is (= "http://example.com/languages/1-German" (:url record)))
;;     (is (instance? DateTime (:created-at record)))
;;     (is (instance? DateTime (:updated-at record))))
;;   ;; (is (thrown? Exception (insert-record :wikipedia.languages german)))
;;   )

;; (database-test test-update-record
;;   (is (thrown? Exception (update-record :wikipedia.languages {})))
;;   (is (nil? (update-record :wikipedia.languages german)))
;;   (let [record (update-record :wikipedia.languages (assoc (insert-record :wikipedia.languages german) :name "Deutsch"))]
;;     (is (number? (:id record)))
;;     (is (= "Deutsch" (:name record)))
;;     (is (= "Indo-European" (:family record)))
;;     (is (= "de" (:iso-639-1 record)))
;;     (is (= "deu" (:iso-639-2 record)))
;;     (is (= "http://example.com/languages/1-Deutsch" (:url record)))
;;     (is (instance? DateTime (:created-at record)))
;;     (is (instance? DateTime (:updated-at record)))
;;     (is (= record (update-record :wikipedia.languages record)))))

;; (database-test test-save-record
;;   (is (thrown? Exception (save-record :wikipedia.languages {})))
;;   (let [language (save-record :wikipedia.languages german)]
;;     (is (pos? (:id language)))
;;     (is (= language (save-record :wikipedia.languages language)))))

;; (deftest test-table
;;   (is (table? (table :wikipedia.languages)))
;;   (is (= "languages" (:name (table :wikipedia.languages))))
;;   (is (= (table :wikipedia.languages) (table (table :wikipedia.languages)))))

;; (deftest test-all-clause
;;   (is (= (pred-and {:role-id 2} {:user-id 1})
;;          (all-clause :roles-users bodhi-is-surfer))))

;; (deftest test-unique-key-clause
;;   (are [record expected]
;;     (is (= expected (unique-key-clause :wikipedia.languages record)))
;;     {} (pred-or)
;;     {:id 1} (pred-or {:id 1})
;;     ;; TODO: How?
;;     ;; {:id 1 :iso-639-1 "de"} (pred-or {:id 1} {:iso-639-1 "de"})
;;     ;; {:id 1 :iso-639-1 "de" :undefined "column"} {:id 1 :iso-639-1 "de"}
;;     ))

;; (deftest test-where-clause
;;   (is (= (all-clause :roles-users bodhi-is-surfer)
;;          (where-clause :roles-users bodhi-is-surfer)))
;;   (is (= (unique-key-clause :users bodhi)
;;          (where-clause :users bodhi))))

;; (database-test test-uniqueness-of
;;   (let [validate-iso-639-1 (uniqueness-of :wikipedia.languages :iso-639-1)]
;;     (is (nil? (meta (validate-iso-639-1 german))))
;;     (insert-record :wikipedia.languages german)
;;     (is (= {:errors {:iso-639-1 ["has already been taken."]}}
;;            (meta (validate-iso-639-1 german))))))

;; (database-test test-where-text=
;;   (is (= (str "SELECT \"wikipedia\".\"languages\".* FROM \"wikipedia\".\"languages\" WHERE (TO_TSVECTOR(CAST(? AS regconfig), "
;;               "CAST(\"wikipedia\".\"languages\".\"name\" AS text)) @@ PLAINTO_TSQUERY(CAST(? AS regconfig), CAST(? AS text)))")
;;          (sql-only (select :wikipedia.languages (where-text= :name "x"))))))

;; (database-test test-defquery
;;   (let [german (save-language german)
;;         spanish (save-language spanish)]
;;     (defquery example-query "Doc" [arg-1]
;;       (-> (languages*) (order :name)))
;;     (is (= [german spanish] (example-query "arg-1")))
;;     (is (= [german] (example-query "arg-1" {:page 1 :per-page 1})))
;;     (is (= [spanish] (example-query "arg-1" {:page 2 :per-page 1})))))

;; (deftest test-sql-slurp
;;   (is (= ["UPDATE wikipedia.languages SET name = 'x' WHERE 1 = 2;" "ANALYZE;"]
;;          (sql-slurp "test-resources/sql-slurp.sql"))))

;; (database-test test-exec-sql-batch-file
;;   (is (= [0 0] (exec-sql-batch-file "test-resources/sql-slurp.sql"))))

;; (database-test test-shift-fields
;;   (insert-fixtures)
;;   (let [users (select (entity :users)
;;                       (korma.core/join :countries (= :countries.id :users.country-id))
;;                       (shift-fields :countries :country [:id :name :created-at :updated-at]))]
;;     (is (= 2 (count users)))
;;     (let [user (first users)]
;;       (is (= (:id bodhi) (:id user)))
;;       (is (= (:country-id bodhi) (:country-id user)))
;;       (is (= (:nick bodhi) (:nick user)))
;;       (is (= (:email bodhi) (:email user)))
;;       (is (= (:crypted-password bodhi) (:crypted-password user)))
;;       (is (instance? DateTime (:created-at user)))
;;       (is (instance? DateTime (:updated-at user)))
;;       (let [country (:country user)]
;;         (is (= (:id usa) (:id country)))
;;         (is (= (:name usa) (:name country)))
;;         (is (instance? DateTime (:created-at country)))
;;         (is (instance? DateTime (:updated-at country)))))))

;; (database-test test-join
;;   (insert-fixtures)
;;   (= (select (entity :users)
;;              (korma.core/join :countries (= :countries.id :users.country-id))
;;              (shift-fields :countries :country [:id :name :created-at :updated-at]))
;;      (select (entity :users) (join :countries (= :countries.id :users.country-id))))
;;   (let [result (select (entity :users) (join :left :countries (= :countries.id :users.country-id) [:id :name]))]
;;     (is (= {:id 1 :name "United States"} (:country (first result))))))

;; (database-test test-with-tmp-table
;;   (with-tmp-table table
;;     [[:id :serial :primary-key? true]
;;      [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
;;      [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]]
;;     (is (string? (:name table)))
;;     (is (empty? (select (:name table))))))
