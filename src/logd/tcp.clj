(ns logd.tcp
  "Networking code, handling TCP clients, servers and serialization.

  Currently one TCP connection per RPC is used -- it would be faster to
  reuse connections for subsequent requests, but care must be taken to handle
  timeouts and failed requests properly to avoid misinterpreting responses as
  belonging to the wrong request.

  (It might even be faster to just use HTTP/2 without writing custom
  networking logic, I'm not sure.)"
  (:require [aleph.tcp :as tcp]
            [gloss.io :as glossio]
            [logd.codec :as codec]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [clojure.tools.logging :as log]))

(defn wrap-raft-rpc
  "Wraps a Manifold stream to automatically serialize and deserialize
   Raft RPC calls (puts serialized, takes deserialized)."
  [s]
  (let [out (s/stream)]
    (s/connect (s/map #(try
                         (glossio/encode codec/raft-rpc %)
                         (catch Throwable t
                           (log/error t "Error encoding data:" %)))
                      out)
               s)
    (s/splice out
              (glossio/decode-stream s codec/raft-rpc))))

;; Handle incoming TCP deliveries by deserializing them,
;; passing them to another stream for processing with a callback stream
;; that will serialize the result back out
(defn start-server [event-stream port]
  (tcp/start-server
   (fn [s info]
     (log/info "Handling TCP connection...")
     (let [s (wrap-raft-rpc s)]
       (s/connect (s/map #(hash-map :rpc % :cb-stream s)
                         s)
                  event-stream)))
   {:port port}))

(defn rpc-client [host port]
  (d/chain (tcp/client {:host host :port port})
           wrap-raft-rpc))

(defn call-rpc [host port rpc]
  (d/let-flow [client (rpc-client host port)]
    (log/info "Making RPC call to host" host ":" rpc)
    (d/chain (s/put! client rpc)
             (fn [_] (s/try-take! client ::closed 50 ::timeout)))))
