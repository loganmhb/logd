(ns logd.raft
  (:require [clojure.core.async :as async]
            [logd.raft.follower :as follower]))

(defn initial-raft-state
  "Structure describing the state of the Raft system, given a list of
  peers' domains/IPs.

  The Raft protocol specifies a log of changes applied to a state
  machine, but since the desired state here is a log no actual state
  machine is necessary, only the log itself -- :last-applied simply
  indicates the portion of the log that could be considered
  canonical."
  [peers]
  {:current-term 0
   :voted-for nil
   :log []
   :commit-index 0
   :last-applied 0
   :peers #{peers}
   :state :follower})

(def rpc-chan (async/chan))

(defn run-follower
  "Given a Raft state and a set of channels from which to get input,
   returns a continuation (for use with trampoline, to get around
   stack overflow issues with mutual recursion in Clojure)"
  [raft-state channels]
  (async/alt!
    ;; Election timeout -- if no contact from leader, become candidate
    (:election-timeout channels)
    #(run-candidate raft-state channels)
    
    ;; Handle an append-entries RPC call and update the state appropriately
    (:append-entries channels)
    ([rpc _]
     (let [{:keys [response state]} (follower/append-entries (:data rpc))]
       (async/>! (:callback-chan rpc) response)
       #(run-follower state channels)))

    ;; Handle a request-vote RPC call and update the state appropriately
    (:request-vote channels)
    ([rpc _]
     (let [{:keys [response state]} (follower/request-vote (:data rpc))]
       (async/>! (:callback-chan rpc) response)
       #(run-follower state channels)))))


(defn run-leader [raft-state channels])

(defn run-candidate [])
