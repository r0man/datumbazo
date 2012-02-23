(ns database.test.postgis
  (:import [org.postgis Geometry PGgeometry PGboxbase PGbox2d PGbox3d Point])
  (:use [clojure.string :only (lower-case)]
        clojure.test
        database.columns
        database.core
        database.postgis
        database.registry
        database.tables
        database.test
        database.test.examples))

(deftable continents
  [[:id :serial :primary-key? true]
   [:name :text :not-null? true :unique? true]
   [:iso-3166-1-alpha-2 :varchar :length 2 :unique? true :serialize #'lower-case]
   [:location [:point-2d]]
   [:geometry [:multipolygon-2d]]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]])

(def continents (find-table :continents))
(def point-2d (make-column :point_2d [:point-2d]))
(def multipolygon-2d (make-column :multipolygon_2d [:multipolygon-2d]))

(database-test test-add-column
  (let [table (create-table continents)]
    (is (= point-2d (add-column table point-2d)))
    (is (= multipolygon-2d (add-column table multipolygon-2d)))))

(database-test test-add-geometry-column
  (let [table (create-table continents)]
    (is (= point-2d (add-geometry-column table point-2d 4326 "POINT" 2)))
    (is (= multipolygon-2d (add-geometry-column table multipolygon-2d 4326 "MULTIPOLYGON" 2)))))

(database-test test-create-with-continents
  (is (instance? database.tables.Table (create-table continents)))
  (is (thrown? Exception (create-table continents))))

(deftest test-geometry?
  (is (not (geometry? nil)))
  (is (not (geometry? "")))
  (is (not (geometry? (make-point-2d 1 2))))
  (is (geometry? (make-geometry (make-point-2d 1 2)))))

(deftest test-make-box-2d
  (let [box (make-box-2d 1 2 3 4)]
    (is (= 1.0 (.getY (.getLLB box))))
    (is (= 2.0 (.getX (.getLLB box))))
    (is (= 3.0 (.getY (.getURT box))))
    (is (= 4.0 (.getX (.getURT box)))))
  (let [box (make-box-2d (make-point-2d 2 1) (make-point-2d 4 3))]
    (is (= 1.0 (.getY (.getLLB box))))
    (is (= 2.0 (.getX (.getLLB box))))
    (is (= 3.0 (.getY (.getURT box))))
    (is (= 4.0 (.getX (.getURT box))))))

(deftest test-make-geometry
  (let [geometry (make-geometry (make-point-2d 1.0 2.0))]
    (is (instance? PGgeometry geometry))
    (is (instance? Point (.getGeometry geometry))))
  (let [geometry (make-geometry (make-point-2d 1.0 2.0 3.0))]
    (is (instance? PGgeometry geometry))
    (is (instance? Point (.getGeometry geometry)))))

(deftest test-make-point-2d
  (let [point (make-point-2d 1.0 2.0)]
    (is (instance? Point point))
    (is (= 4326 (.getSrid point)))
    (is (= 1.0 (.getX point)))
    (is (= 2.0 (.getY point)))
    (is (= 0.0 (.getZ point)))))

(deftest test-make-point-3d
  (let [point (make-point-3d 1.0 2.0 3.0)]
    (is (instance? Point point))
    (is (= 4326 (.getSrid point)))
    (is (= 1.0 (.getX point)))
    (is (= 2.0 (.getY point)))
    (is (= 3.0 (.getZ point)))))

(deftest test-read-geometry
  (let [point (make-point-2d 1 2)]
    (is (= point (.getGeometry (read-geometry point)))))
  (let [point (make-point-3d 1 2 3)]
    (is (= point (.getGeometry (read-geometry point))))))
