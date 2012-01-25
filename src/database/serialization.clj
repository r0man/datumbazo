(ns database.serialization
  (:use [inflections.core :only (camelize singular)]
        database.columns
        database.tables))

(defn- transform-column [column row transform-fn]
  (let [attr-name (column-keyword column) attr-val (get row attr-name)]
    (if (and attr-val transform-fn)
      (assoc row attr-name (transform-fn attr-val))
      row)))

(defn deserialize-column
  "Deserialize the column of the database row."
  [column row] (transform-column column row (:deserialize column)))

(defn deserialize-column
  "Deserialize the column of the database row."
  [column value] (if value ((or (:deserialize column) identity) value)))

(defn deserialize-row
  "Deserialize the database row."
  [table row]
  (with-ensure-table table
    (reduce #(deserialize-column %2 %1) (or row {}) (vals (:columns table)))))

(defn deserialize-row
  "Deserialize the database row."
  [table row]
  (with-ensure-table table
    (reduce #(assoc %1 (:name %2) (deserialize-column %2 (get row (:name %2))))
            {} (select-columns table (keys row)))))

(defn serialize-column
  "Serialize the `value` of column."
  [column value] (if value ((or (:serialize column) identity) value)))

(defn serialize-row
  "Serialize the database row."
  [table row]
  (with-ensure-table table
    (let [row (or row {}) columns (select-columns table (keys row))]
      (reduce #(assoc %1 (column-keyword %2) (serialize-column %2 (get row (:name %2))))
              {} columns))))

(defn define-serialization
  "Returns the serialization froms for the database table."
  [table]
  (let [record# (camelize (singular (table-symbol table)))
        entity# (singular (table-symbol table))
        constructor# (symbol (str "map->" record#))]
    `(do
       (defn ~(symbol (str "deserialize-" entity#))
         ~(str "Deserialize the " entity# " database row.")
         [~entity#] (deserialize-row ~(table-keyword table) ~entity#))
       (defn ~(symbol (str "serialize-" entity#))
         ~(str "Serialize the " entity# " database row.")
         [~entity#] (serialize-row ~(table-keyword table) ~entity#)))))
