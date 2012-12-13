(ns datumbazo.postgis
  (:import org.postgis.PGgeometry))

;; (def ^:dynamic *readers*
;;   {'inst read-wkt})

(defn read-wkt
  "Read a geometry from `s` in WKT format."
  [s] (PGgeometry/geomFromString s))
