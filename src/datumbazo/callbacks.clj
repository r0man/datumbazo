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

(defcallback after-initialize
  "Called after initializing a record.")

(defcallback before-validation
  "Called before a record gets validated.")

(defcallback after-validation
  "Called after a record has been validated.")

(defcallback before-save
  "Called before a record is saved to the database.")

(defcallback before-create
  "Called before a record is created in the database.")

(defcallback after-create
  "Called after a record has been created in the database.")

(defcallback after-save
  "Called after a record has been saved to the database.")

(defcallback after-commit
  "Called after a record has been commited to the database.")

(defcallback after-find
  "Called after a record has been found in the database.")

(defcallback after-initialize
  "Called after a record has been initialized.")

;; TODO: Add after-find-batch callback
