(ns database.serialization
  (:import java.sql.Timestamp)
  (:use [clj-time.coerce :only (from-date)]
        [inflections.core :only (camelize singular)]
        database.columns
        database.tables
        database.registry))

(defprotocol IDeserialization
  (deserialize [obj] "Deserialize `obj`."))

;; (defprotocol ISerialization
;;   (serialize [obj] "Serialize `obj`."))

;; (defrecord Serializer [type serialize-fn deserialize-fn]
;;   IDeserialization
;;   (deserialize [value]
;;     (if (and value deserialize-fn)
;;       (deserialize-fn value)))
;;   ISerialization
;;   (serialize [value]
;;     (if (and value serialize-fn)
;;       (serialize-fn value))))

(defn- assoc-url [record url-fn]
  (if url-fn
    (if-let [url (url-fn record)]
      (assoc record :url url) record)
    record))

(defn deserialize-column
  "Deserialize the column of the database row."
  [column value] (if value ((or (:deserialize column) identity) value)))

(defn deserialize-record
  "Deserialize the database row."
  [table row]
  (if (not (nil? row))
    (with-ensure-table table
      (assoc-url
       (reduce #(assoc %1 (:name %2) (deserialize-column %2 (get row (:name %2))))
               {} (select-columns table (keys row)))
       (:url table)))))

(defn serialize-column
  "Serialize the `value` of column."
  [column value] (if value ((or (:serialize column) identity) value)))

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

(extend-protocol IDeserialization
  nil
  (deserialize [_]
    nil)
  Object
  (deserialize [object]
    object)
  Timestamp
  (deserialize [timestamp]
    (from-date timestamp)))
