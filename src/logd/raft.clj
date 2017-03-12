(ns logd.raft
  (:require [clojure.core.async :as async]
            [logd.raft.candidate :as candidate]
            [logd.raft.follower :as follower]
            [manifold.stream :as s]
            [manifold.deferred :as d]))

(defn new-election-timeout
  []
  (+ (System/currentTimeMillis) 150 (rand-int 100)))

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
   :votes-received 0
   ;; Leader state for tracking replication
   :replication-status (into {}
                             (for [peer peers]
                               {peer {:match-index 0
                                      :next-index 1}}))})

(defn time-remaining [raft-state]
  (- (:election-timeout raft-state)
     (System/currentTimeMillis)))


(defn reset-timeout [raft-state]
  (assoc raft-state :election-timeout (new-election-timeout)))

(defn become-candidate [raft-state]
  (-> raft-state
      (reset-timeout)
      (update :current-term inc)
      (assoc :role :candidate
             :votes-received 1)))

(defn await-majority [& deferreds]
  (let [results (s/stream)]
    (doseq [d deferreds]
      (d/chain d #(s/put! s %)))))

(defn handle-read
  "If leader, send AppendEntries to confirm leadership, then reply with log.
   Otherwise, redirect to the leader."
  [raft-state event])

(defn handle-write
  "If leader, send AppendEntries and wait for majority success, then reply success.
   Otherwise, redirect to the leader."
  [raft-state event])

(defn handle-event
  "Applies an event to a Raft state, performing side effects if necessary
  (e.g. making RPC calls to request votes or append entries)"
  [raft-state event]
  (cond
    (:rpc event)
    (let [{:keys [response new-state]} (handle-rpc (:rpc event))]
      (s/put! (:cb-stream event) response)
      new-state)

    (:read event)
    (handle-read raft-state event)

    (:write event)
    (handle-write raft-state event)))

(defn run-raft [events initial-state]
  (loop [state initial-state]
    (let [ev @(s/try-take! events ::closed
                           (time-remaining state) ::timeout)]
      (cond
        (= ev ::timeout) (-> state
                             become-candidate
                             request-votes
                             recur)
        (= ev ::closed) nil ; just stop?
        :else (recur (handle-event raft-state ev))))))

