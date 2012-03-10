(ns database.serialization
  (:import java.sql.Timestamp)
  (:use [clj-time.coerce :only (to-date-time to-timestamp)]
        [inflections.core :only (camelize singular)]
        database.columns
        database.registry
        database.tables
        database.util))

;; DESERIALIZATION

(defmulti deserialize-column (fn [column value] (:type column)))

(defmethod deserialize-column :default [column value]
  (if value ((or (:deserialize column) identity) value)))

(defmethod deserialize-column :timestamp-with-time-zone [column value]
  (to-date-time value))

(defn deserialize-record
  "Deserialize the database row."
  [table row]
  (if (not (nil? row))
    (with-ensure-table table
      (-> (reduce #(assoc %1 (:name %2) (deserialize-column %2 (get row (:name %2))))
                  (into {} row) (select-columns table (keys row)))
          (assoc-url (:url table))))))

;; SERIALIZATION

(defmulti serialize-column (fn [column value] (:type column)))

(defmethod serialize-column :default [column value]
  (if value ((or (:serialize column) identity) value)))

(defmethod serialize-column :integer [column value]
  (parse-integer value :junk-allowed true))

(defmethod serialize-column :serial [column value]
  (parse-integer value :junk-allowed true))

(defmethod serialize-column :timestamp-with-time-zone [column value]
  (to-timestamp value))

(defn serialize-record
  "Serialize the database row."
  [table row]
  (if (not (nil? row))
    (with-ensure-table table
      (let [row (or row {}) columns (select-columns table (keys row))]
        (reduce #(assoc %1 (keyword (column-name %2)) (serialize-column %2 (get row (:name %2))))
                {} columns)))))

(defn define-serialization
  "Returns the serialization froms for the database table."
  [table]
  (let [record# (symbol (camelize (singular (table-name table))))
        entity# (symbol (singular (table-name table)))
        constructor# (symbol (str "map->" record#))]
    `(do
       (defn ~(symbol (str "deserialize-" entity#))
         ~(str "Deserialize the " entity# " database row.")
         [~entity#] (deserialize-record ~(keyword (table-name table)) ~entity#))
       (defn ~(symbol (str "serialize-" entity#))
         ~(str "Serialize the " entity# " database row.")
         [~entity#] (serialize-record ~(keyword (table-name table)) ~entity#)))))
