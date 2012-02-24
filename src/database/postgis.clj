(ns database.postgis
  (:import [org.postgis Geometry PGgeometry PGboxbase PGbox2d PGbox3d Point])
  (:require [clojure.java.jdbc :as jdbc])
  (:use [geo.box :only (make-box north-east south-west to-box safe-boxes)]
        [geo.location :only (latitude longitude make-location to-location ILocation)]
        [korma.core :exclude (table)]
        [korma.sql.engine :only [infix]]
        database.columns
        database.core
        database.tables
        database.serialization
        database.registry))

(def ^:dynamic *srid* 4326)

(defprotocol IBox
  (to-box-2d [obj] "Convert `obj` to a 2-dimensional PostGIS box."))

(defprotocol IPoint
  (to-point-2d [obj] "Convert `obj` to a 2-dimensional PostGIS Point."))

(defn set-srid
  "Set the SRID of `geometry` with the ST_SetSRID PostGIS fn."
  [geometry & [srid]] (sqlfn ST_SetSRID geometry (Integer. (or srid *srid*))))

(defn && [geo-1 geo-2]
  (infix (set-srid geo-1) "&&" (set-srid geo-2)))

(defn geo= [geo-1 geo-2]
  (infix geo-1 "~=" geo-2))

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
  (add-geometry-column table column *srid* "POINT" 2))

(defmethod add-column :multipolygon-2d [table column]
  (add-geometry-column table column *srid* "MULTIPOLYGON" 2))

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
  ([ll-x ll-y ur-x ur-y]
     (make-box-2d
      (make-point-2d ll-x ll-y)
      (make-point-2d ur-x ur-y))))

(defmethod print-dup PGgeometry [geometry writer]
  (print-dup (str (.getGeometry geometry)) writer))

(defn read-geometry
  "Parse `s` and return a org.postgis.PGgeometry object. "
  [s] (org.postgis.PGgeometry. (str s)))

(defquery select-by-location
  "Select all rows of `table` at `location`."
  [query field location]
  (if-let [point (to-point-2d location)]
    (let [point (make-geometry (to-point-2d location))]
      (where query {field [geo= point]}))
    query))

(defmethod serialize-column :point-2d [column location]
  (make-geometry (to-point-2d location)))

(defmethod deserialize-column :point-2d [column point-2d]
  (to-location point-2d))

(extend-type nil
  IBox
  (to-box-2d [obj]
    nil)
  IPoint
  (to-point-2d [obj]
    nil))

(extend-type Object
  IBox
  (to-box-2d [obj]
    (if-let [box (to-box obj)]
      (make-box-2d
       (to-point-2d (south-west box))
       (to-point-2d (north-east box)))))
  IPoint
  (to-point-2d [obj]
    (if-let [location (to-location obj)]
      (make-point-2d (longitude location) (latitude location)))))

(extend-type PGbox2d
  IBox
  (to-box-2d [box]
    box)
  geo.box/IBox
  (north-east [box]
    (make-location
     (latitude (to-location (.getURT box)))
     (longitude (to-location (.getURT box)))))
  (south-west [box]
    (make-location
     (latitude (to-location (.getLLB box)))
     (longitude (to-location (.getLLB box)))))
  (to-box [box]
    (make-box (south-west box) (north-east box))))

(extend-type PGgeometry
  ILocation
  (latitude [geometry]
    (latitude (to-location geometry)))
  (longitude [geometry]
    (longitude (to-location geometry)))
  (to-location [geometry]
    (to-location (.getGeometry geometry))))

(extend-type Point
  ILocation
  (latitude [point]
    (.getY point))
  (longitude [point]
    (.getX point))
  (to-location [point]
    (make-location (latitude point) (longitude point)))
  IPoint
  (to-point-2d [point]
    point))
