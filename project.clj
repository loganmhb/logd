(defproject logd "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main logd.core
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [compojure "1.5.2"]
                 [ring "1.5.1"]
                 [clj-http "3.4.1"]
                 [ring/ring-defaults "0.2.2"]
                 [mount "0.1.11"]
                 [cheshire "5.7.0"]
                 [org.clojure/core.async "0.2.395"]
                 [org.clojure/core.async "0.3.441"]
                 [gloss "0.2.6"]
                 [aleph "0.4.3"]
                 [manifold "0.1.6"]])
