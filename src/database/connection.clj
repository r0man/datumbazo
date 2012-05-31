(ns database.connection
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (blank?)]
        [database.util :only (parse-url)]
        [environ.core :only (env)]
        [inflections.core :only (dasherize underscore)]
        [slingshot.slingshot :only [throw+]]
        korma.db))

(def ^:dynamic *connections* (atom {}))

(def ^:dynamic *naming-strategy*
  {:keys (comp keyword dasherize) :fields (comp name underscore)})

(defn korma-connection
  "Returns the Korma connection for the given name."
  [name]
  (let [url (env name)]
    (if (blank? url)
      (throw+ (format "Can't find database url for %s. Please configure environ!" name))
      (if-let [spec (parse-url url)]
        (if-let [connection (get @*connections* spec)]
          connection
          (do (swap! *connections* assoc spec (create-db (assoc spec :naming *naming-strategy*)))
              (get @*connections* spec)))
        (throw+  (str "Can't parse database url: " url))))))

(defn jdbc-connection
  "Returns the JDBC connection for the given name."
  [name] @(:pool (korma-connection name)))

(defmacro with-database
  "Evaluate `body` with the Korma and JDBC connections set to `connection`."
  [name & body]
  `(let [connection# @_default]
     (try
       (default-connection (korma-connection ~name))
       (jdbc/with-connection (jdbc-connection ~name)
         ~@body)
       (finally (default-connection connection#)))))
