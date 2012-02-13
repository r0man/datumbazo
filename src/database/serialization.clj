(ns database.serialization
  (:import java.sql.Timestamp)
  (:use [clj-time.coerce :only (to-date-time to-timestamp)]
        [inflections.core :only (camelize singular)]
        database.columns
        database.tables
        database.registry))

;; DESERIALIZATION

(defmulti deserialize-column (fn [column value] (:type column)))

(defmethod deserialize-column :default [column value]
  (if value ((or (:deserialize column) identity) value)))

(defmethod deserialize-column :timestamp-with-time-zone [column value]
  (to-date-time value))

(defn- assoc-url [record url-fn]
  (if url-fn
    (if-let [url (url-fn record)]
      (assoc record :url url) record)
    record))

(defn deserialize-record
  "Deserialize the database row."
  [table row]
  (if (not (nil? row))
    (with-ensure-table table
      (assoc-url
       (reduce #(assoc %1 (:name %2) (deserialize-column %2 (get row (:name %2))))
               {} (select-columns table (keys row)))
       (:url table)))))

;; SERIALIZATION

(defmulti serialize-column (fn [column value] (:type column)))

(defmethod serialize-column :default [column value]
  (if value ((or (:serialize column) identity) value)))

(defmethod serialize-column :timestamp-with-time-zone [column value]
  (to-timestamp value))

(defn serialize-record
  "Serialize the database row."
  [table row]
  (if (not (nil? row))
    (with-ensure-table table
      (let [row (or row {}) columns (select-columns table (keys row))]
        (reduce #(assoc %1 (column-keyword %2) (serialize-column %2 (get row (:name %2))))
                {} columns)))))

(defn define-serialization
  "Returns the serialization froms for the database table."
  [table]
  (let [record# (camelize (singular (table-symbol table)))
        entity# (singular (table-symbol table))
        constructor# (symbol (str "map->" record#))]
    `(do
       (defn ~(symbol (str "deserialize-" entity#))
         ~(str "Deserialize the " entity# " database row.")
         [~entity#] (deserialize-record ~(table-keyword table) ~entity#))
       (defn ~(symbol (str "serialize-" entity#))
         ~(str "Serialize the " entity# " database row.")
         [~entity#] (serialize-record ~(table-keyword table) ~entity#)))))
