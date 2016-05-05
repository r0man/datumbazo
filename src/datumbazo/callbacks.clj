(ns datumbazo.callbacks
  (:require [inflections.core :as infl]))

(defmacro defcallback
  "Define a record callback."
  [callback doc]
  (let [protocol (symbol (str "I" (infl/camel-case callback)))]
    `(do (defprotocol ~protocol
           (~callback [~'record] ~doc))
         (extend-type java.lang.Object
           ~protocol
           (~callback [~'record] ~'record))
         (defn ~(symbol (str "call-" callback))
           [~'records]
           (map #(~callback %) ~'records)))))

(defcallback after-create
  "Called after a record has been created in the database.")

(defcallback after-delete
  "Called after a record has been deleted from the database.")

(defcallback after-initialize
  "Called after initializing a record.")

(defcallback after-find
  "Called after a record has been found in the database.")

(defcallback after-save
  "Called after a record has been saved to the database.")

(defcallback after-update
  "Called after a record has been updated in the database.")

(defcallback before-create
  "Called before a record is created in the database.")

(defcallback before-delete
  "Called before a record is deleted from the database.")

(defcallback before-save
  "Called before a record is saved to the database.")

(defcallback before-update
  "Called before a record is updated in the database.")
