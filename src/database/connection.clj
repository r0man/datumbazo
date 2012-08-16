(ns database.connection
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (blank? join)]
        [database.util :only (parse-url)]
        [environ.core :only (env)]
        [inflections.core :only (dasherize underscore)]
        [slingshot.slingshot :only [throw+]]
        [korma.config :as config]
        korma.db))

(def ^:dynamic *connections* (atom {}))

(def ^:dynamic *naming-strategy*
  {:keys identity
   :fields identity})

(def ^:dynamic *quote* \")

(defn- format-params
  "Format the `params` map as a query string."
  [params]
  (->> (remove #(or (nil? (first %1)) (nil? (last %1))) params)
       (map #(join "=" (map name %1)))
       (join "&" )))

(defn database-spec
  "Returns the database spec for `name`."
  [name] (parse-url (env name)))

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

(defn jdbc-url
  "Returns the JDBC database url for `name`."
  [name]
  (let [spec (database-spec name)]
    (str "jdbc:" (:subprotocol spec) ":" (:subname spec)
         (let [params (format-params (select-keys spec [:user :password]))]
           (if-not (blank? params)
             (str "?" params))) )))

(defmacro with-naming-strategy
  "Evaluates `body` in the context of a naming strategy compatible
  with clojure.java.jdbc and Korma."
  [naming-strategy & body]
  `(jdbc/with-naming-strategy ~naming-strategy
     ~@body))

(defmacro with-quoted-identifiers
  "Evaluates `body` in the context of a quoting strategy compatible
  with clojure.java.jdbc and Korma."
  [quote & body]
  (let [quote# quote]
    `(jdbc/with-quoted-identifiers ~quote#
       (let [delimiters# (:delimiters @config/options)]
         (try
           (set-delimiters ~quote#)
           ~@body
           (finally
            (set-delimiters delimiters#)))))))

(defmacro with-database
  "Evaluates `body` with the Korma and JDBC connections set to the
  named connection."
  [name & body]
  `(let [connection# @_default]
     (try
       (default-connection (korma-connection ~name))
       (jdbc/with-connection @(:pool (korma-connection ~name))
         (with-naming-strategy *naming-strategy*
           (jdbc/with-quoted-identifiers *quote*
             ~@body)))
       (finally (default-connection connection#)))))
