(ns database.serialization
  (:use [inflections.core :only (camelize singular)]
        database.columns
        database.tables))

(defn- select-columns [table columns]
  (let [columns (set columns)]
    (filter #(contains? columns (column-keyword %1)) (:columns table))))

(defn- transform-column [column row transform-fn]
  (let [attr-name (column-keyword column) attr-val (get row attr-name)]
    (if (and attr-val transform-fn)
      (assoc row attr-name (transform-fn attr-val))
      row)))

(defn deserialize-column
  "Deserialize the column of the database row."
  [column row] (transform-column column row (:deserialize column)))

(defn deserialize-row
  "Deserialize the database row."
  [table row] (reduce #(deserialize-column %2 %1) row (:columns table)))

(defn serialize-column
  "Serialize the column of the database row."
  [column row] (transform-column column row (:serialize column)))

(defn serialize-row
  "Serialize the database row."
  [table row]
  (let [columns (select-columns table (keys row))]
    (reduce #(serialize-column %2 %1) (into {} row) columns)))

(defn define-deserialization
  "Returns the serialization froms for the database table."
  [table]
  (let [record# (camelize (singular (table-symbol table)))
        entity# (singular (table-symbol table))
        constructor# (symbol (str "map->" record#))]
    `(do
       (defn ~(symbol (str "deserialize-" entity#))
         ~(str "Deserialize the " entity# " database row.")
         [~entity#] (deserialize-row (find-table ~(table-keyword table)) ~entity#))
       (defn ~(symbol (str "serialize-" entity#))
         ~(str "Serialize the " entity# " database row.")
         [~entity#] (serialize-row (find-table ~(table-keyword table)) ~entity#)))))
