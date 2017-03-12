(ns logd.raft
  (:require [clojure.core.async :as async]
            [logd.raft.candidate :as candidate]
            [logd.raft.follower :as follower]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.tools.logging :as log]))

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


(defn reset-timeout
  "Resets the election timeout monotonically (i.e. it won't reduce the
  timeout, though because of the random factor if the timeout is large
  enough it might not increase it)."
  [raft-state]
  (let [new-timeout (new-election-timeout)
        old-timeout (:election-timeout raft-state)]
    (assoc raft-state :election-timeout (if (< new-timeout old-timeout)
                                          old-timeout
                                          new-timeout))))

(defn become-candidate [raft-state]
  (log/info "Election timeout reached. Becoming :candidate")
  (-> raft-state
      (reset-timeout)
      (update :current-term inc)
      (assoc :role :candidate
             :votes-received 1)))

(defn get-log-index
  "Provides 1-indexed log access to comply with Raft semantics"
  [state n]
  (when (> n 1)
    (get-in state [:log (dec n)])))

(defn prev-log-index-missing? [state prev-log-index]
  (and (> prev-log-index 0)
       (nil? (get-log-index state prev-log-index))))

(defn prev-log-entry-from-wrong-term? [state data]
  (and (< 0 (:prev-log-index data))
       (not= (:term (get-log-index state
                                   (:prev-log-index data)))
             (:prev-log-term data))))

(defn become-follower [raft-state]
  (when (not= (:role raft-state) :follower)
    (log/info "Converting role from" (:role raft-state) "to :follower."))
  (assoc raft-state :role :follower))

(defn handle-append-entries
  "Implements Raft AppendEntries. Given a Raft state an an
  AppendEntries RPC call, returns an RPC response and an updated Raft
  state applying the call (or an unchanged state if the call was
  rejected)."
  [state data]
  (cond
    (< (:term data) (:current-term state))
    {:response {:term (:current-term state)
                :type :append-entries-response
                :success false}
     :state state}

    (or (prev-log-index-missing? state (:prev-log-index data))
        (prev-log-entry-from-wrong-term? state data))
    {:response {:term (:current-term state)
                :type :append-entries-response
                :success false}
     :state (reset-timeout (become-follower state))}

    ;; If none of those conditions are met, the RPC is acceptable and
    ;; we need to update the state.
    :else
    {:response {:success true
                :type :append-entries-response
                :term (:term data)}
     :state (-> state
                become-follower
                (update :log (fn [log]
                               (into (vec (take (:prev-log-index data) log))
                                     (:entries data))))
                (assoc :current-term (:term data))
                (assoc :commit-index (:leader-commit data))
                reset-timeout)}))

(defn handle-request-vote
  "Implements the RequestVote RPC."
  [state data]
  (if (or (< (:term data) (:current-term state))
          (< (:last-log-index data) (count (:log state)))
          (:voted-for state))
    {:response {:type :request-vote-response
                :vote-granted false
                :term (:current-term state)}
     :state state}
    {:response {:type :request-vote-response
                :vote-granted true
                :term (:current-term state)}
     :state (-> state
                (assoc :voted-for (:candidate-id data))
                reset-timeout)}))

(defn await-majority [& deferreds]
  (let [results (s/stream)]
    (doseq [d deferreds]
      (d/chain d #(s/put! results %)))))

(defn handle-read
  "If leader, send AppendEntries to confirm leadership, then reply with log.
   Otherwise, redirect to the leader."
  [raft-state event])

(defn handle-write
  "If leader, send AppendEntries and wait for majority success, then reply success.
   Otherwise, redirect to the leader."
  [raft-state event])

(defn handle-rpc [raft-state event]
  (let [{:keys [response state]}
        (case (:type (:rpc event))
          :append-entries (handle-append-entries raft-state (:rpc event))
          :request-vote (handle-request-vote raft-state (:rpc event)))]
    (s/put! (:cb-stream event) response)
    state))

(defn handle-event
  "Applies an event to a Raft state, performing side effects if necessary
  (e.g. making RPC calls to request votes or append entries)"
  [raft-state event]
  (cond
    (:rpc event)
    (handle-rpc raft-state event)

    (:read event)
    (handle-read raft-state event)

    (:write event)
    (handle-write raft-state event)))

(defn send-request-votes [raft-state]
  raft-state)

(defn run-raft [events initial-state]
  (loop [state initial-state]
    (let [ev @(s/try-take! events ::closed
                           (time-remaining state) ::timeout)]
      (cond
        (= ev ::timeout) (-> state
                             become-candidate
                             send-request-votes
                             recur)
        (= ev ::closed) nil ; just stop?
        :else (recur (handle-event state ev))))))

