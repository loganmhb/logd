(ns logd.raft.follower)

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
  (cond
    (< (:term data) (:current-term state))
    {:actions [[:rpc-respond {:term (:current-term state)
                              :success false}]]
     :state state}

    (or (prev-log-index-missing? state (:prev-log-index data))
        (prev-log-entry-from-wrong-term? state data))
    {:actions [[:reset-election-timeout]
               [:rpc-respond {:term (:current-term state)
                              :success false}]]
     :state state}

    ;; If none of those conditions are met, the RPC is acceptable and
    ;; we need to update the state as well as resetting the election
    ;; timer.
    :else
    {:actions [[:reset-election-timeout]
               [:rpc-respond {:success true
                              :term (:term data)}]]
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
    {:actions [[:rpc-respond {:vote-granted false
                              :term (:current-term state)}]] 
     :state state}
    {:actions [[:reset-election-timeout]
               [:rpc-respond {:vote-granted true
                              :term (:current-term state)}]]
     :state (assoc state :voted-for (:candidate-id data))}))

