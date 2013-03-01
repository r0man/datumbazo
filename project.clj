(defproject datumbazo "0.2.1-SNAPSHOT"
  :description "Clojure Database Kung-Foo"
  :url "https://github.com/r0man/datumbazo"
  :min-lein-version "2.0.0"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clj-time "0.4.4"]
                 [environ "0.3.0"]
                 [geo-clj "0.2.0"]
                 [org.clojure/clojure "1.5.0"]
                 [org.clojure/algo.monads "0.1.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.cloudhoist/stevedore "0.7.3"]
                 [postgresql/postgresql "9.1-901.jdbc4"]
                 [slingshot "0.10.3"]
                 [sqlingvo "0.2.1-SNAPSHOT"]]
  :profiles {:dev {:dependencies [[validation-clj "0.5.1-SNAPSHOT"]
                                  [org.slf4j/slf4j-log4j12 "1.6.6"]
                                  [c3p0/c3p0 "0.9.1.2"]
                                  [com.jolbox/bonecp "0.7.1.RELEASE"]]
                   :resource-paths ["test-resources"]
                   :env {:test-db "postgresql://tiger:scotch@localhost/datumbazo"}}}
  :plugins [[environ/environ.lein "0.3.0"]]
  :hooks [environ.leiningen.hooks]
  :db [{:name :test-db
        :fixtures "db/test-db/fixtures"
        :migrations "db/test-db/migrations"}]
  :repositories {"sonatype" "http://oss.sonatype.org/content/repositories/releases"})
