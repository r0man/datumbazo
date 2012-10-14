(ns database.sql.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace split]]
            [database.util :as util]))

(defprotocol ICompileSQL
  (compile-sql [arg] "Compile `arg` into an SQL statement."))

(defprotocol IMakeColumn
  (to-column [arg] "Convert `arg` into a Column."))

(defprotocol IMakeTable
  (to-table [arg] "Convert `arg` into a Table."))

(defrecord Column [schema table name type not-null? unique? primary-key?]

  ICompileSQL
  (compile-sql [t]
    [(str (if schema
            (str (jdbc/as-identifier schema) "."))
          (if table
            (str (jdbc/as-identifier table) "."))
          (jdbc/as-identifier name))])

  IMakeColumn
  (to-column [column]
    column))

(defrecord Table [schema name]

  ICompileSQL
  (compile-sql [t]
    [(str (if schema
            (str (jdbc/as-identifier schema) "."))
          (jdbc/as-identifier name))])

  IMakeTable
  (to-table [t] t))

(defrecord Select [columns]
  ICompileSQL
  (compile-sql [stmt]))

(defrecord DropTable [tables cascade? if-exists? restrict?]
  ICompileSQL
  (compile-sql [stmt]
    [(str "DROP TABLE "
          (if if-exists? "IF EXISTS ")
          (join ", " (map (comp first compile-sql) tables))
          (if cascade? " CASCADE")
          (if restrict? " RESTRICT"))]))

(defrecord TruncateTable [tables cascade? continue-identity? restart-identity? restrict?]
  ICompileSQL
  (compile-sql [stmt]
    [(str "TRUNCATE TABLE "
          (join ", " (map (comp first compile-sql) tables))
          (if restart-identity? " RESTART IDENTITY")
          (if continue-identity? " CONTINUE IDENTITY")
          (if cascade? " CASCADE")
          (if restrict? " RESTRICT"))]))

(defrecord Select [columns from]
  ICompileSQL
  (compile-sql [stmt]
    (let [columns (map compile-sql columns)
          from (map compile-sql from)]
      [(str "SELECT " (if (empty? columns)
                        "*" (join ", " (map first columns)))
            (if-not (empty? from)
              (str " FROM " (join ", " (map first from)))))])))

;; (jdbc/with-quoted-identifiers \"
;;   (prn (compile-sql
;;         (map->Select
;;          {:columns [(map->Column {:name :id}) (map->Column {:name :name})]
;;           :from [(map->Table {:name :continents})
;;                  (map->Table {:name :countries})]}))))

;; (jdbc/with-quoted-identifiers \"
;;   (prn (compile-sql
;;         (map->Select
;;          {:columns []
;;           :from [(map->Table {:name :continents})
;;                  (map->Table {:name :countries})]}))))

(defn column?
  "Returns true if `arg` is a Column, otherwise false."
  [arg] (instance? Column arg))

(defn table?
  "Returns true if `arg` is a Table, otherwise false."
  [arg] (instance? Table arg))

(defn make-column
  "Make a database column."
  [table name type & {:as options}]
  (assoc (map->Column (or options {}))
    :schema (:schema table)
    :table (:name table)
    :name name
    :type type))

(extend-type nil
  ICompileSQL
  (compile-sql [_] ["NULL"]))

(extend-type clojure.lang.Keyword

  ICompileSQL
  (compile-sql [k]
    (compile-sql (util/qualified-name k)))

  IMakeColumn
  (to-column [k]
    (to-column (util/qualified-name k)))

  IMakeTable
  (to-table [k]
    (to-table (util/qualified-name k))))

(extend-type clojure.lang.PersistentVector

  IMakeColumn
  (to-column [[t as alias]]
    (assoc (to-column t) :alias alias))

  IMakeTable
  (to-table [[t as alias]]
    (assoc (to-table t) :alias alias)))

(extend-type Number
  ICompileSQL
  (compile-sql [n]
    [(str n)]))

(extend-type Object
  ICompileSQL
  (compile-sql [o]
    ["?" o]))

(extend-type String

  ICompileSQL
  (compile-sql [s]
    [(->> (split s #"\.|/")
          (map (comp jdbc/as-identifier keyword))
          (join "."))])

  IMakeColumn
  (to-column [s]
    (map->Column (util/parse-column s)))

  IMakeTable
  (to-table [s]
    (map->Table (util/parse-table s))))
