(ns datumbazo.table
  (:require [datumbazo.record :as record]
            [datumbazo.util :as util]
            [sqlingvo.core :as sql]
            [sqlingvo.expr :as expr]))

(defn table
  "Make a new table."
  [name & body]
  (fn [table]
    [nil (merge
          table
          (second
           ((sql/chain-state body)
            (expr/parse-table name))))]))

(defn define-table-by-class
  "Define the `table-by-class` multi method for `table`."
  [table]
  (let [class (util/class-symbol table)]
    `(defmethod datumbazo.util/table-by-class ~class
       [~'class]
       ~table)))

(defn- define-truncate
  "Define a function that truncates `table`."
  [table]
  `(defn ~'truncate!
     ~(str "Truncate the " (-> table :name name) " table.")
     [~'db & [~'opts]]
     @(sql/truncate ~'db [~(util/table-keyword table)]
        (sql/cascade (:cascade ~'opts)))))

(defmacro deftable
  "Define a database table."
  [table-name doc & body]
  (let [table# (eval `(second ((table ~(keyword table-name) ~@body) {})))]
    `(do ~(record/define-record table#)
         ~(define-table-by-class table#)
         ~(define-truncate table#))))
