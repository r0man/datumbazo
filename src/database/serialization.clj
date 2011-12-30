(ns database.serialization
  (:use [inflections.core :only (camelize singular)]
        database.columns
        database.tables))

(defprotocol IDeserialize
  (deserialize [row table] "Deserialize the table row."))

(defprotocol ISerialize
  (serialize [row table] "Serialize the table row."))

(defn- transform-column [row column transform-fn]
  (let [attribute (column-keyword column) value (get row attribute)]
    (assoc row attribute ((or transform-fn identity) (get row attribute)))))

(defn deserialize-column
  "Deserialize the column of the database row."
  [row column] (transform-column row column (:deserialize column)))

(defn serialize-column
  "Serialize the column of the database row."
  [row column] (transform-column row column (:serialize column)))

(defn deserialize-row
  "Deserialize the database row."
  [row table] (reduce deserialize-column row (:columns table)))

(defn serialize-row
  "Serialize the database row."
  [row table] (reduce serialize-column (into {} row) (:columns table)))

(defn define-deserialization
  "Returns the serialization froms for the database table."
  [table]
  (let [record# (camelize (singular (table-symbol table)))
        entity# (singular (table-symbol table))
        constructor# (symbol (str "map->" record#))]
    `(do
       (extend ~record# IDeserialize {:deserialize deserialize-row})
       (defn ~(symbol (str "deserialize-" entity#))
         ~(str "Deserialize the " entity# " database row.")
         [~entity#] (deserialize (find-table ~(table-keyword table)) (~constructor# ~entity#)))
       (extend ~record# ISerialize {:serialize serialize-row})
       (defn ~(symbol (str "serialize-" entity#))
         ~(str "Serialize the " entity# " database row.")
         [~entity#] (serialize (find-table ~(table-keyword table)) (~constructor# ~entity#))))))
