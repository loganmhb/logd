(ns logd.core
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.core.async :as async]
            [compojure.core :refer [defroutes GET POST]]
            [mount.core :refer [defstate]]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.defaults :refer [api-defaults wrap-defaults]]))

(defroutes public-routes
  (GET "/log" {body :body}
    {:status 200 :body body})
  (POST "/log" {body :body}
    {:status 200 :body body}))

(defroutes peer-routes
  (POST "/request-vote" [] {:status 200 :body "ok"})
  (POST "/append-entries" [entries] {:status 200 :body "ok"}))

(def public-app (wrap-defaults public-routes api-defaults))

(def peer-app (wrap-defaults peer-routes api-defaults))

(defstate peer-server
  :start (jetty/run-jetty peer-app {:port 9001 :join? false})
  :stop (.stop peer-server))

(defstate public-server
  :start (jetty/run-jetty public-app {:port 9000 :join? false})
  :stop (.stop public-server))

(defn -main [& peer-hosts]
  (mount.core/start))

(comment
  (http/post "http://localhost:9000/log" {:body (json/generate-string {:hi "there"})})
  ) 
