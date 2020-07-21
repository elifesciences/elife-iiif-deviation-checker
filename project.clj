(defproject devchk "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [clj-http "3.10.1"] ;; http handling
                 [org.clojure/core.async "1.2.603"] ;; async handling
                 [cheshire "5.10.0"] ;; json handling
                 [org.clojure/tools.namespace "1.0.0"] ;; reload code
                 [org.clojure/data.csv "1.0.0"] ;; csv handling
                 ]

  :main devchk.core

  )
