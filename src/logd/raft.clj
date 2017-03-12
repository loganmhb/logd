(ns logd.raft
  (:require [clojure.core.async :as async]
            [logd.raft.candidate :as candidate]
            [logd.raft.follower :as follower]
            [manifold.stream :as s]))

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
   :election-timeout (+ (System/currentTimeMillis) 150)
   ;; Candidate state for bookkeeping requesting votes
   :requested-votes? false
   :votes-received 0
   ;; Leader state for tracking replication
   :replication-status (into {}
                             (for [peer peers]
                               {peer {:match-index 0
                                      :next-index 1}}))})

(defn time-remaining [raft-state]
  (- (:election-timeout raft-state)
     (System/currentTimeMillis)))

(defn run-raft [events peers]
  (loop [state (initial-raft-state peers)]
    (let [ev @(s/try-take! events (time-remaining state) ::timeout)]
      (cond
        (= ev ::timeout) (become-candidate)
        ;; Other conditions?
        ;; - read request
        ;; - write request
        ;; - AppendEntries rpc
        ;; - RequestVotes rpc
        ))))
