(ns database.postgis
  (:import [org.postgis Geometry PGgeometry PGboxbase PGbox2d PGbox3d Point])
  (:require [clojure.java.jdbc :as jdbc])
  (:use database.columns
        database.core
        database.tables
        database.registry))

(def ^:dynamic *srid* 4326)

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

(defn geometry?
  "Returns true if `arg` is a geometry, otherwise false."
  [arg] (instance? PGgeometry arg))

(defn make-geometry
  "Make a org.postgis.PGgeometry from `geometry`."
  [geometry] (PGgeometry. geometry))

(defn make-point-2d
  "Make a 2-dimensional org.postgis.Point from `x`, `y` and `srid`."
  [x y & [srid]] (doto (Point. x y) (.setSrid (or srid *srid*))))

(defn make-point-3d
  "Make a 3-dimensional org.postgis.Point from `x`, `y`, `z` and `srid`."
  [x y z & [srid]] (doto (make-point-2d x y srid) (.setZ z)))

(defn make-box-2d
  "Make a 2-dimensional org.postgis.PGBox2d."
  ([south-west north-east]
     (PGbox2d. south-west north-east))
  ([sw-lat sw-lon ne-lat ne-lon]
     (make-box-2d
      (make-point-2d sw-lon sw-lat)
      (make-point-2d ne-lon ne-lat))))

(defmethod print-dup PGgeometry [geometry writer]
  (print-dup (str (.getGeometry geometry)) writer))

(defn read-geometry
  "Parse `s` and return a org.postgis.PGgeometry object. "
  [s] (org.postgis.PGgeometry. (str s)))
