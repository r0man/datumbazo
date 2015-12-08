(ns datumbazo.vendor
  (:require [clojure.string :as str]
            [sqlingvo.db :as db]))

(defn- format-server [db-spec]
  (str (:host db-spec)
       (if (:port db-spec)
         (str ":" (:port db-spec)))))

(defmulti subname
  "Return the JDBC subname for `db-spec`."
  (fn [db-spec]
    (or (keyword (:subprotocol db-spec))
        (:scheme db-spec))))

(defmethod subname :oracle [db-spec]
  (str ":" (:user db-spec) "/" (:password db-spec) "@"
       (format-server db-spec)
       ":" (:name db-spec)))

(defmethod subname :sqlserver [db-spec]
  (str "//" (format-server db-spec) ";"
       "database=" (:name db-spec) ";"
       "user=" (:user db-spec) ";"
       "password=" (:password db-spec)))

(defmethod subname :default [db-spec]
  (str "//" (:server-name db-spec)
       (if-let [port (:server-port db-spec)]
         (str ":" port))
       "/" (:name db-spec)
       (if-not (str/blank? (:query-string db-spec))
         (str "?" (:query-string db-spec)))))

(defmulti db-spec
  "Return the vendor specific `db-spec`."
  (fn [db-spec]
    (or (keyword (:subprotocol db-spec))
        (:scheme db-spec))))

(defmethod db-spec :mysql [spec]
  (assoc (db/mysql spec) :subname (subname spec)))

(defmethod db-spec :oracle [spec]
  (assoc (db/oracle spec)
         :subprotocol "oracle:thin"
         :subname (subname spec)))

(defmethod db-spec :postgresql [spec]
  (assoc (db/postgresql spec) :subname (subname spec)))

(defmethod db-spec :vertica [spec]
  (assoc (db/vertica spec) :subname (subname spec)))

(defmethod db-spec :sqlite [spec]
  (assoc (db/sqlite spec) :subname (subname spec)))

(defmethod db-spec :sqlserver [spec]
  (assoc (db/sqlserver spec) :subname (subname spec)))
