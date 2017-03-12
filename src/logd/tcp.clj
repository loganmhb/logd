(ns logd.tcp
  (:require [aleph.tcp :as tcp]
            [gloss.io :as glossio]
            [logd.codec :as codec]
            [manifold.deferred :as d]
            [manifold.stream :as s]))

(defn wrap-raft-rpc [s]
  (let [out (s/stream)]
    (s/connect (s/map #(glossio/encode codec/raft-rpc %) out)
               s)
    (s/splice out
              (glossio/decode-stream s codec/raft-rpc))))


;; Sketch: handle incoming TCP deliveries by deserializing them,
;; passing them to another stream for processing with a callback stream
;; that will serialize the result back out
(defn start-server [event-stream port]
  (tcp/start-server
   (fn [s info]
     (let [s (wrap-raft-rpc s)]
       (s/connect (s/map #(hash-map :rpc % :cb-stream s)
                         s)
                  event-stream)))
   {:port port}))


(defn call-rpc [host port rpc]
  (d/let-flow [tcp-client (tcp/client {:host host :port port})
               client (wrap-raft-rpc tcp-client)]
    (s/put! client rpc)
    (s/take! client)))
