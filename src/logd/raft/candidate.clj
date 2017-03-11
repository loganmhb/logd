(ns logd.raft.candidate)

(defn nominate-self [raft-state]
  {:actions (for [peer peers]
              [:request-vote peer])
   :state (assoc raft-state
                 :role :candidate
                 :election-timeout (new-election-timeout)
                 :current-term (inc (:current-term raft-state))
                 :requested-votes? false
                 :votes-received 1)}) ; vote for self automatically
