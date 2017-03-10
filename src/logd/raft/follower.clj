(ns logd.raft.follower)

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

(defn append-entries
  "Implements Raft AppendEntries for a follower. Given a Raft state an
  an AppendEntries RPC call, returns an RPC response and an updated
  Raft state applying the call (or an unchanged state if the call was
  rejected)."
  [state data]
  (if (or (< (:term data) (:current-term state))
          (prev-log-index-missing? state (:prev-log-index data))
          (prev-log-entry-from-wrong-term? state data))
    {:response {:term (:current-term state)
                :success false}
     :state state}
    ;; If none of those conditions are met, the RPC is acceptable and
    ;; we need to update the state.
    {:response {:success true
                :term (:term data)}
     :state (-> state
                (update :log (fn [log]
                               (into (vec (take (:prev-log-index data) log))
                                     (:entries data))))
                (assoc :current-term (:term data))
                (assoc :commit-index (:leader-commit data)))}))


(defn request-vote
  "Implements the RequestVote RPC."
  [state data]
  (if (or (< (:term data) (:current-term state))
          (< (:last-log-index data) (count (:log state)))
          (:voted-for state))
    {:response {:vote-granted false
                :term (:current-term state)}
     :state state}
    {:response {:vote-granted true
                :term (:current-term state)}}))
