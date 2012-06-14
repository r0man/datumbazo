(ns database.postgis
  (:import [org.postgis Geometry PGgeometry PGbox2d Point])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as s])
  (:use [geo.box :only (make-box north-east south-west to-box safe-boxes)]
        [geo.location :only (latitude longitude make-location to-location ILocation)]
        [clojure.java.shell :only (sh)]
        [korma.sql.engine :only [infix]]
        [korma.sql.fns :only [pred-or]]
        [slingshot.slingshot :only [throw+]]
        database.columns
        database.core
        database.serialization
        database.tables
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

(defn box2d
  "Returns a BOX2D representing the maximum extents of the geometry."
  [geometry] (sqlfn box2d geometry))

(defn raster2pgsql*
  "Returns the command to convert `source` with the PostGIS
  raster2pgsql command to `target` using `table` as the table name."
  [source target table & {:keys [band column copy filename mode no-data srid width height overview]
                          :or {mode :append}}]
  (->> [["raster2pgsql"]
        (if band ["-b" band])
        (if copy ["-Y"])
        (if filename ["-F"])
        (if no-data ["-N" no-data])
        (if overview ["-l" overview])
        (if srid ["-s" srid])
        (if (and width height) ["-t" (str width "x" height)])
        [(condp = mode
           :append "-a"
           :create "-c"
           :drop "-d"
           :prepare "-p")]
        (if column ["-f" column])
        [(format "%s \"%s\" > \"%s\"" source table target)]]
       (mapcat concat)
       (s/join " ")))

(defn raster2pgsql
  "Convert `source` with the PostGIS raster2pgsql command to `target`
  using `table` as the table name."
  [source target table & {:as options}]
  (let [result (sh "bash" "-c" (apply raster2pgsql* source target table (mapcat seq options)))]
    (if (zero? (:exit result))
      (assoc (merge options result)
        :source source
        :target target
        :table table)
      (throw+ (assoc result :type :raster2pgsql)))))

(defn st-centroid
  "Returns the geometric center of a geometry."
  [geometry] (sqlfn st_centroid geometry))

(defn st-translate
  "Translates the geometry to a new location using the numeric
  parameters as offsets."
  [geometry delta-x delta-y] (sqlfn st_translate geometry delta-x delta-y))

(defn st-x
  "Return the X coordinate of the point, or NULL if not
  available. Input must be a point."
  [geometry] (sqlfn st_x geometry))

(defn st-y
  "Return the Y coordinate of the point, or NULL if not
  available. Input must be a point."
  [geometry] (sqlfn st_y geometry))

(defquery select-by-location
  "Select all rows of `query` where `field` matches `location`."
  [query field location]
  (if-let [point (to-point-2d location)]
    (let [geometry (make-geometry (to-point-2d location))]
      (where query {field [geo= geometry]}))
    query))

(defquery select-in-box
  "Select all rows of `query` where `field` is in `box`."
  [query field box]
  (if-let [box (to-box box)]
    (let [geometries (map to-box-2d (safe-boxes box))]
      (where* query (apply pred-or (map #(hash-map field [&& %1]) geometries))))
    query))

(defquery sort-by-distance
  "Sort query by location."
  [query field location]
  (if-let [point (to-point-2d location)]
    (order query (sqlfn ST_Distance field (make-geometry point)) (or (:direction options) :asc))
    query))

;; SERIALIZATION

(defmethod deserialize-column :box-2d [column box-2d]
  (to-box box-2d))

(defmethod serialize-column :box-2d [column bounds]
  (to-box-2d bounds))

(defmethod deserialize-column :point-2d [column point-2d]
  (if point-2d (to-location point-2d)))

(defmethod serialize-column :point-2d [column location]
  (if-let [point-2d (to-point-2d location)]
    (make-geometry point-2d)))

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

;; (select :spots (limit 1)
;;         (fields [(raw "st_x(spots.location)") :x]))

;; (select :spots (limit 1)
;;         (fields [(st-x :location) :x]))

;; (select :spots (limit 1)
;;         (join :countries (= :countries.id :spots.country-id))
;;         (fields (st-translate
;;                  :countries.geometry
;;                  (infix (st-x :location) "-" (st-x (st-centroid :countries.geometry)))
;;                  (infix (st-y :location) "-" (st-y (st-centroid :countries.geometry))))))
