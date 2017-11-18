(ns datumbazo.sqlite.types
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]))

(s/def ::boolean boolean?)

(s/def ::integer int?)

(s/def ::real double?)

(s/def ::text string?)
