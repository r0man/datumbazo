(ns datumbazo.json
  (:import [org.postgis PGgeometry Point])
  (:require [clojure.data.json :refer [-write JSONWriter]]))

(extend-type PGgeometry
  JSONWriter
  (-write [geom out]
    (-write (.getGeometry geom) out)))

(extend-type Point
  JSONWriter
  (-write [point out]
    (-write {:type "Point" :coordinates [(.getX point) (.getY point) (.getZ point)]} out)))
