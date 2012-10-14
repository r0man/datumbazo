(ns database.sql.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace split]]
            [database.util :as util]))

(defprotocol ICompileSQL
  (compile-sql [arg] "Compile `arg` into an SQL statement."))

(defprotocol IMakeTable
  (to-table [arg] "Convert `arg` into a Table."))

(defrecord Column [schema table name])

(defrecord Table [schema name]
  IMakeTable
  (to-table [t] t))

(defrecord Select [columns]
  ICompileSQL
  (compile-sql [stmt]))

(defrecord DropTable [table cascade? if-exists? restrict?]
  ICompileSQL
  (compile-sql [stmt]
    [(str "DROP TABLE "
          (if if-exists? "IF EXISTS ")
          (first (compile-sql table))
          (if cascade? " CASCADE")
          (if restrict? " RESTRICT"))]))

(extend-type nil
  ICompileSQL
  (compile-sql [_] ["NULL"]))

(extend-type clojure.lang.Keyword

  ICompileSQL
  (compile-sql [k]
    (compile-sql (util/qualified-name k)))

  IMakeTable
  (to-table [k]
    (to-table (util/qualified-name k))))

(extend-type String

  ICompileSQL
  (compile-sql [s]
    [(->> (split s #"\.|/")
          (map (comp jdbc/as-identifier keyword))
          (join "."))])

  IMakeTable
  (to-table [s]
    (map->Table (util/parse-table s))))
