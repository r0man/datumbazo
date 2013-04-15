(defproject datumbazo "0.4.0"
  :description "Clojure Database Kung-Foo"
  :url "https://github.com/r0man/datumbazo"
  :min-lein-version "2.0.0"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clj-time "0.5.0"]
                 [environ "0.4.0"]
                 [geo-clj "0.2.0"]
                 [org.clojure/clojure "1.5.1"]
                 [org.clojure/algo.monads "0.1.4"]
                 [org.clojure/tools.logging "0.2.6"]
                 [com.palletops/stevedore "0.8.0-beta.2"]
                 [postgresql/postgresql "9.1-901.jdbc4"]
                 [slingshot "0.10.3"]
                 [sqlingvo "0.4.0"]]
  :profiles {:dev {:dependencies [[validation-clj "0.5.2-SNAPSHOT"]
                                  [org.slf4j/slf4j-log4j12 "1.6.6"]
                                  [c3p0/c3p0 "0.9.1.2"]
                                  [com.jolbox/bonecp "0.7.1.RELEASE"]]
                   :resource-paths ["test-resources"]
                   :env {:test-db "postgresql://tiger:scotch@localhost/datumbazo"}}}
  :plugins [[lein-environ "0.4.0"]]
  :db [{:name :test-db
        :fixtures "db/test-db/fixtures"
        :migrations "db/test-db/migrations"}])
