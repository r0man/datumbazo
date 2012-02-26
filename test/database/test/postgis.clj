(ns database.test.postgis
  (:import [org.postgis Geometry PGgeometry PGboxbase PGbox2d PGbox3d Point]
           org.joda.time.DateTime)
  (:use [clojure.string :only (lower-case)]
        [geo.box :only (north-east south-west to-box make-box)]
        [geo.location :only (make-location latitude longitude to-location)]
        [korma.core :exclude (table)]
        [migrate.core :only (defmigration)]
        clojure.test
        database.columns
        database.core
        database.postgis
        database.registry
        database.tables
        database.test
        database.test.examples))

(def world-box (make-box (make-location -90 -180) (make-location 90 180)))

(def world-box-wraped (make-box (make-location -90 0) (make-location 90 -180)))

(deftable continents
  [[:id :serial :primary-key? true]
   [:name :text :not-null? true :unique? true]
   [:iso-3166-1-alpha-2 :varchar :length 2 :unique? true :serialize #'lower-case]
   [:location [:point-2d]]
   [:geometry [:multipolygon-2d]]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]])

(defmigration "2012-02-24T13:35:00"
  "Create the continents table."
  (create-table (table :continents))
  (drop-table (table :continents)))

(def asia
  {:id 1
   :name "Asia"
   :iso-3166-1-alpha-2 "as"
   :freebase-guid "#9202a8c04000641f8000000000004011"
   :geonames-id 6255147
   :location (make-location 49.837982 105.820313)
   :countries 0
   :regions 0
   :spots 0})

(def europe
  {:id 2
   :name "Europe"
   :iso-3166-1-alpha-2 "eu"
   :freebase-guid "#9202a8c04000641f800000000001413e"
   :geonames-id 6255148
   :location (make-location 54.5259614 15.2551187)
   :countries 0
   :regions 0
   :spots 0})

(def continents (find-table :continents))
(def point-2d (make-column :point_2d [:point-2d]))
(def multipolygon-2d (make-column :multipolygon_2d [:multipolygon-2d]))

(database-test test-add-column
  (is (= point-2d (add-column :continents point-2d)))
  (is (= multipolygon-2d (add-column :continents multipolygon-2d))))

(database-test test-add-geometry-column
  (is (= point-2d (add-geometry-column :continents point-2d 4326 "POINT" 2)))
  (is (= multipolygon-2d (add-geometry-column :continents multipolygon-2d 4326 "MULTIPOLYGON" 2))))

(database-test test-create-with-continents
  (drop-table (table :continents))
  (is (instance? database.tables.Table (create-table continents)))
  (is (thrown? Exception (create-table continents))))

(deftest test-geometry?
  (is (not (geometry? nil)))
  (is (not (geometry? "")))
  (is (not (geometry? (make-point-2d 1 2))))
  (is (geometry? (make-geometry (make-point-2d 1 2)))))

(database-test test-insert-continent
  (let [continent (insert-continent europe)]
    (is (pos? (:id continent)))
    (is (= (:id europe) (:id continent)))
    (is (= (:name europe) (:name continent)))
    (is (= (:iso-3166-1-alpha-2 europe) (:iso-3166-1-alpha-2 continent)))
    (is (= (:location europe) (:location continent)))
    (is (instance? DateTime (:created-at continent)))
    (is (instance? DateTime (:updated-at continent)))))

(deftest test-latitude
  (is (= 43.4073349 (latitude (make-point-2d -2.6983217 43.4073349)))))

(deftest test-longitude
  (is (= -2.6983217 (longitude (make-point-2d -2.6983217 43.4073349)))))

(deftest test-make-box-2d
  (let [box (make-box-2d 2 1 4 3)]
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

(deftest test-north-east
  (let [location (north-east (make-box-2d 148.045733 -35.522452 153.242267 -33.256207))]
    (is (= -33.256207 (latitude location)))
    (is (= 153.242267 (longitude location)))))

(deftest test-read-geometry
  (let [point (make-point-2d 1 2)]
    (is (= point (.getGeometry (read-geometry point)))))
  (let [point (make-point-3d 1 2 3)]
    (is (= point (.getGeometry (read-geometry point))))))

(database-test test-save-continent
  (let [continent (save-continent europe)]
    (is (pos? (:id continent)))
    (is (= (:id europe) (:id continent)))
    (is (= (:name europe) (:name continent)))
    (is (= (:iso-3166-1-alpha-2 europe) (:iso-3166-1-alpha-2 continent)))
    (is (= (:location europe) (:location continent)))
    (is (instance? DateTime (:created-at continent)))
    (is (instance? DateTime (:updated-at continent)))
    (is (= continent (save-continent europe)))))

(database-test test-select-by-location
  (let [[continent] (select-by-location (continents*) :location (:location (save-continent europe)))]
    (is (= (:id europe) (:id continent)))
    (is (= (:name europe) (:name continent)))
    (is (= (:iso-3166-1-alpha-2 europe) (:iso-3166-1-alpha-2 continent)))
    (is (= (:location europe) (:location continent)))
    (is (instance? DateTime (:created-at continent)))
    (is (instance? DateTime (:updated-at continent)))))

(database-test test-select-in-box
  (let [asia (save-continent asia) europe (save-continent europe)]
    (is (= #{asia europe} (set (select-in-box (continents*) :location world-box))))
    (is (= #{asia europe} (set (select-in-box (continents*) :location world-box-wraped))))))

(deftest test-south-west
  (let [location (south-west (make-box-2d 148.045733 -35.522452 153.242267 -33.256207))]
    (is (= -35.522452 (latitude location)))
    (is (= 148.045733 (longitude location)))))

(database-test test-sort-by-location
  (let [asia (save-continent asia) europe (save-continent europe)]
    (is (= [asia europe] (sort-by-location (continents*) (:location asia))))
    (is (= [europe asia] (sort-by-location (continents*) (:location europe))))))

(deftest test-to-box
  (let [box (to-box (make-box-2d 148.045733 -35.522452 153.242267 -33.256207))]
    (is (= -35.522452 (latitude (south-west box))))
    (is (= 148.045733 (longitude (south-west box))))
    (is (= -33.256207 (latitude (north-east box))))
    (is (= 153.242267 (longitude (north-east box))))))

(deftest test-to-box-2d
  (let [point (to-point-2d (make-location 43.4073349 -2.6983217))]
    (is (= -2.6983217 (.getX point)))
    (is (= 43.4073349 (.getY point)))))

(deftest test-to-location
  (let [location (to-location (make-point-2d -2.6983217 43.4073349))]
    (is (= 43.4073349 (latitude location)))
    (is (= -2.6983217 (longitude location)))))

(deftest test-to-point-2d
  (let [box (to-box-2d (make-box-2d 148.045733 -35.522452 153.242267 -33.256207))]
    (is (= 148.045733 (.getX (.getLLB box))))
    (is (= -35.522452 (.getY (.getLLB box))))
    (is (= 153.242267 (.getX (.getURT box))))
    (is (= -33.256207 (.getY (.getURT box)))))
  (let [box (to-box-2d "-35.522452 148.045733 -33.256207 153.242267")]
    (is (= 148.045733 (.getX (.getLLB box))))
    (is (= -35.522452 (.getY (.getLLB box))))
    (is (= 153.242267 (.getX (.getURT box))))
    (is (= -33.256207 (.getY (.getURT box))))))

(database-test test-update-continent
  (let [continent (insert-continent europe)]
    (let [continent (update-continent (assoc europe :name "Europa"))]
      (is (pos? (:id continent)))
      (is (= (:id europe) (:id continent)))
      (is (= "Europa" (:name continent)))
      (is (= (:iso-3166-1-alpha-2 europe) (:iso-3166-1-alpha-2 continent)))
      (is (= (:location europe) (:location continent)))
      (is (instance? DateTime (:created-at continent)))
      (is (instance? DateTime (:updated-at continent)))
      (is (= continent (update-continent continent))))))