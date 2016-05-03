(ns datumbazo.vendor
  (:require [no.en.core :as noencore]))

(defmulti jdbc-url
  "Return the JDBC URL for `db`."
  (fn [db] (-> db :scheme keyword)))

(defmulti subname
  "Return the JDBC subname for `db`."
  (fn [db] (-> db :scheme keyword)))

(defn- format-server [db]
  (str (:server-name db)
       (when-let [port (:server-port db)]
         (str ":" port))))

(defmethod subname :oracle [db]
  (str ":" (:user db) "/" (:password db) "@"
       (format-server db)
       ":" (:name db)))

(defmethod subname :sqlserver [db]
  (str "//" (format-server db) ";"
       "database=" (:name db) ";"
       "user=" (:user db) ";"
       "password=" (:password db)))

(defmethod subname :default [db]
  (str "//" (format-server db) "/" (:name db)))

(defmethod jdbc-url :default [db]
  (str "jdbc:" (-> db :scheme name) ":" (subname db)
       (when-let [params (not-empty (:query-params db))]
         (str "?" (noencore/format-query-params params)))))
