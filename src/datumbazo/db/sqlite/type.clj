(ns datumbazo.db.sqlite.type
  (:require [clojure.spec.alpha :as s]))

(s/def ::boolean boolean?)

(s/def ::integer int?)

(s/def ::real double?)

(s/def ::text string?)
