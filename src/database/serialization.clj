(ns database.serialization
  (:use [inflections.core :only (camelize singular)]
        database.columns
        database.tables))

(defn- transform-column [column row transform-fn]
  (let [attribute (column-keyword column) value (get row attribute)]
    (if (and value transform-fn)
      (assoc row attribute (transform-fn value))
      row)))

(defn deserialize-column
  "Deserialize the column of the database row."
  [column row] (transform-column column row (:deserialize column)))

(defn serialize-column
  "Serialize the column of the database row."
  [column row] (transform-column column row (:serialize column)))

(defn deserialize-row
  "Deserialize the database row."
  [table row] (reduce #(deserialize-column %2 %1) row (:columns table)))

(defn serialize-row
  "Serialize the database row."
  [table row] (reduce #(serialize-column %2 %1) (into {} row) (:columns table)))

(defn define-deserialization
  "Returns the serialization froms for the database table."
  [table]
  (let [record# (camelize (singular (table-symbol table)))
        entity# (singular (table-symbol table))
        constructor# (symbol (str "map->" record#))]
    `(do
       (defn ~(symbol (str "deserialize-" entity#))
         ~(str "Deserialize the " entity# " database row.")
         [~entity#] (deserialize-row (find-table ~(table-keyword table)) (~constructor# ~entity#)))
       (defn ~(symbol (str "serialize-" entity#))
         ~(str "Serialize the " entity# " database row.")
         [~entity#] (serialize-row (find-table ~(table-keyword table)) (~constructor# ~entity#))))))
