(ns logd.raft.candidate)

(defn nominate-self [raft-state]
  {:actions (into [[:reset-election-timeout]]
                  (for [peer (:peers raft-state)]
                    [:request-vote peer]))
   :state (assoc raft-state
                 :role :candidate
                 :current-term (inc (:current-term raft-state))
                 :requested-votes? false
                 :votes-received 1)}) ; vote for self automatically
