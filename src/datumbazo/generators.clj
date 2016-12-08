(ns datumbazo.generators
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]))

(defn varchar [size]
  (gen/fmap #(apply str %) (gen/vector (gen/char-alpha) 2)))
