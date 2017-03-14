(ns logd.core
  (:gen-class)
  (:require [cheshire.core :as json]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [compojure.core :refer [defroutes GET POST]]
            [logd.raft :as raft]
            [logd.tcp :as ltcp]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [mount.core :refer [defstate]]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.defaults :refer [api-defaults wrap-defaults]]
            [mount.core :as mount]
            [clojure.string :as str]))

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


(def argspec
  [["-p" "--peer ADDR" "Peer host or IP"
    :default #{}
    :assoc-fn (fn [m k v] (update m k conj v))]
   ["-i" "--id NAME" "ID (hostname or IP of this server)"]
   ["-h" "--help"]])

(defn -main [& args]
  (let [opts (cli/parse-opts args argspec)]
    (mount.core/start)
    (log/info "Running Raft with peers" (str/join ", " (:peer (:options opts))))
    (raft/run-raft event-stream (raft/initial-raft-state (:id (:options opts))
                                                         (:peer (:options opts))))))

(comment
  (d/chain (ltcp/call-rpc "localhost" 3456
                          {:type :append-entries
                           :term 1
                           :leader-id "peer1"
                           :prev-log-index 0
                           :prev-log-term 0
                           :entries [{:term 1 :data "correct"}]
                           :leader-commit 1})
           println)
  (.close event-stream)
  (def client *2)
  *2
  @(s/put! (:cb-stream *1) (:rpc *1))
  (mount.core/stop)
  (mount.core/start)
  
  (future (raft/run-raft logd.core/event-stream (raft/initial-raft-state [])))
  )
