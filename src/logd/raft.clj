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
   ;; Candidate state for bookkeeping requesting votes
   :requested-votes? false
   :votes-received 0
   ;; Leader state for tracking replication
   :replication-status (into {}
                             (for [peer peers]
                               {peer {:match-index 0
                                      :next-index 1}}))})


(defn run-candidate [raft-state channels]
  (cond
    (:requested-votes? raft-state) (candidate/request-votes raft-state)

    (> (:votes-received raft-state) (/ (count (:peers raft-state)) 2))
    (assoc raft-state
           :role :leader)

    :else
    (async/alt!!
      (:vote-response-chans raft-state)
      ([rpc _] (if (:vote-granted rpc)
                 (update raft-state :votes-received inc)
                 raft-state))

      (:election-timeout raft-state)
      (become-candidate raft-state)

      (:append-entries channels)
      ([rpc _]
       ;; Become a follower and handle the RPC
       (let [{:keys [response state]}
             (follower/append-entries (assoc raft-state :role :follower)
                                      (:data rpc))]
         (async/>!! (:callback-chan rpc) response)
         (if (:success response)
           (assoc state :election-timeout (new-election-timeout))
           state))))))
