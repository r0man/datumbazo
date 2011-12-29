(ns database.postgis
  (:require [clojure.java.jdbc :as jdbc])
  (:use database.columns
        database.core
        database.tables))

(defn add-geometry-column
  "Add a PostGIS geometry column to table with the AddGeometryColumn SQL fn."
  [table column code geometry dimension]
  (jdbc/with-query-results _
    ["SELECT AddGeometryColumn(?::text, ?::text, ?::integer, ?::text, ?::integer)"
     (table-identifier table)
     (column-identifier column)
     code geometry dimension])
  column)

(defmethod add-column :point-2d [table column]
  (add-geometry-column table column 4326 "POINT" 2))

(defmethod add-column :multipolygon-2d [table column]
  (add-geometry-column table column 4326 "MULTIPOLYGON" 2))
