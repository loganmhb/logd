(ns logd.raft
  (:require [clojure.core.async :as async]
            [logd.raft.candidate :as candidate]
            [logd.raft.follower :as follower]))

(defn new-election-timeout
  []
  (async/timeout (+ 150 (rand-int 100))))

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
   :role :follower
   :election-timeout (new-election-timeout)
   ;; Candidate state for bookkeeping requesting votes
   :requested-votes? false
   :votes-received 0
   :vote-responses [] ;; place to store channels for receiving votes
   ;; Leader state for tracking replication
   :replication-status (into {}
                             (for [peer peers]
                               {peer {:match-index 0
                                      :next-index 1}}))})

(def rpc-chan (async/chan))

(defn run-follower
  "Waits for input or election timeout and returns the transitioned state."
  [raft-state channels]
  (async/alt!!
    ;; Election timeout -- if no contact from leader, become candidate
    (:election-timeout raft-state)
    (assoc raft-state
           :role :candidate
           :election-timeout (new-election-timeout)
           :requested-votes? false
           :votes-received 0)
    
    ;; Handle an append-entries RPC call and update the state appropriately,
    ;; resetting the election timer if the call was successful
    (:append-entries channels)
    ([rpc _]
     (let [{:keys [response state]} (follower/append-entries raft-state (:data rpc))]
       (async/>!! (:callback-chan rpc) response)
       (if (:success response)
         (assoc state :election-timeout (new-election-timeout))
         state)))

    ;; Handle a request-vote RPC call and update the state appropriately
    (:request-vote channels)
    ([rpc _]
     (let [{:keys [response state]} (follower/request-vote raft-state (:data rpc))]
       (async/>!! (:callback-chan rpc) response)
       state))))


(defn run-candidate [raft-state channels]
  (if-not (:requested-votes? raft-state)
    (candidate/request-votes raft-state)
    (async/alt!!
      ;;FIXME: poll for stuff
      )))

(defn run-leader [raft-state channels])

