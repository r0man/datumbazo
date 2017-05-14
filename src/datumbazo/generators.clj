(ns datumbazo.generators
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]))

(defn varchar [size]
  (gen/fmap #(apply str %) (gen/vector (gen/char-alpha) size)))
