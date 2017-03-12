(ns logd.core
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [compojure.core :refer [defroutes GET POST]]
            [logd.tcp :as ltcp]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [mount.core :refer [defstate]]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.defaults :refer [api-defaults wrap-defaults]]))

;; Main bus for communicating between Raft executor and network
(defstate event-stream
  :start (s/stream)
  :stop (.close event-stream))

(defn enqueue-request [req]
  (let [result (d/deferred)]
    (s/put! event-stream (assoc req :result result))
    (-> result
        (d/chain (fn [res]
                   {:status 200
                    :body (json/generate-string res)}))
        (d/catch Exception
            #(do (log/error %)
                 {:status 500
                  :body %})))))

(defroutes public-routes
  (GET "/log" []
    (enqueue-request {:read :all}))
  (POST "/log" {body :body}
    (enqueue-request {:write body})))

(def public-app (wrap-defaults public-routes api-defaults))

(defstate public-server
  :start (jetty/run-jetty public-app {:port 3457 :join? false})
  :stop (.stop public-server))

(defstate peer-server
  :start (ltcp/start-server event-stream 3456)
  :stop (.close peer-server))

(defn -main [& peer-hosts]
  (mount.core/start))

(comment
  (d/chain (ltcp/call-rpc "localhost" 3456
                          {:type :append-entries-response
                           :success true
                           :term 0})
           println)
  @(s/take! event-stream)
  (def client *2)
  *2
  @(s/put! (:cb-stream *1) (:rpc *1))
  (mount.core/stop)
  (mount.core/start)
  )
