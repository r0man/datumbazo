(ns database.meta
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (upper-case)]
        database.connection))

(defrecord Schema [name catalog])

(defn schemas
  "Retrieves the schemas available to the database connection."
  [connection]
  (->> (.getSchemas (.getMetaData connection))
       (resultset-seq)
       (map #(Schema. (:table_schem %1) (:table_catalog %1)))))

(defn tables
  "Retrieves the schemas available to the database connection."
  [connection & {:keys [catalog schema-pattern table-pattern types]
                 :or {schema-pattern :public types [:table]}}]
  (->> (.getTables (.getMetaData connection)
                   (if catalog (name catalog))
                   (if schema-pattern (name schema-pattern))
                   (if table-pattern (name table-pattern))
                   (into-array String (map (comp upper-case name) (remove nil? types))))
       (resultset-seq)))

(defn version
  "Returns the product name, mayor and minor version of the database
  connection."
  [connection]
  (let [meta (.getMetaData connection)]
    {:product-name (.getDatabaseProductName meta)
     :major-version (.getDatabaseMajorVersion meta)
     :minor-version (.getDatabaseMinorVersion meta)}))
