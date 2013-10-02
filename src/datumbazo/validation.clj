(ns datumbazo.validation
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :refer [blank?]]
            [datumbazo.core :refer [select from limit run1 where]]))

(defn new-record? [record]
  (blank? (str (:id record))))

(defn uniqueness-of
  "Validates that the record's attributes are unique."
  [db table columns & {:keys [error] :as opts}]
  (fn [record]
    (let [error (or error "has already been taken")
          columns (if (sequential? columns) columns [columns])
          vals (map record columns)
          condition `(and ~@(map (fn [c v] `(= ~c ~v)) columns vals))]
      (if (and (or (nil? (:if opts))
                   ((:if opts) record))
               (run1 db (select columns
                          (from table)
                          (where condition)
                          (limit 1))))
        (reduce
         (fn [record column]
           (with-meta record
             (assoc-in (meta record) [:errors column] error)))
         record columns)
        record))))
