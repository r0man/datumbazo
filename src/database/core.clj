(ns database.core
  (:require [clojure.java.jdbc :as jdbc]))

;; (defmacro with-connection
;;   "Evaluates body in the context of a new connection to the `name`
;;   database. The connection spec for `name` is looked up via environ."
;;   [name & body]
;;   `(let [name# ~name]
;;      (jdbc/with-connection
;;        (if (keyword? name#)
;;          (or (env name#)
;;              (throw (IllegalArgumentException. (format "Can't resolve connection spec via environ: %s" name#))))
;;          name#)
;;        ~@body)))

;; (defn make-table
;;   "Make a database table."
;;   [name & {:as options}]
;;   (assoc options
;;     :name (keyword name)))

;; (defn make-column
;;   "Make a database column."
;;   [name type & {:as options}]
;;   (assoc options
;;     :name (keyword name)))

;; (defn column
;;   "Add a database column to `table`."
;;   [table name type & options]
;;   (let [column (apply make-column name type options)]
;;     (-> (update-in table [:columns] #(concat %1 [(:name column)]))
;;         (assoc-in [:column (:name column)] column))))

;; (defn schema
;;   "Assoc `schema` under the :schema key to `table`."
;;   [table schema] (assoc table :schema schema))

;; (defn register-table
;;   "Register a database table."
;;   [table] table)

;; (defmacro deftable
;;   "Define a database table."
;;   [name doc & body]
;;   `(def ~name
;;      (register-table
;;       (-> (make-table ~(keyword name) :doc ~doc)
;;           ~@body))))

;; ;; (deftable continents)
;; ;; (continents :x 1)

;; ;; (deftable countries
;; ;;   (column :id :serial)
;; ;;   (column :continent-id :integer :references :continents/id)
;; ;;   (column :name))
