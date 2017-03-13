(ns logd.raft
  (:require [clojure.tools.logging :as log]
            [logd.tcp :as ltcp]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [mount.core :as mount :refer [defstate]]
            [aleph.tcp :as tcp]))

(defstate config
  :start (:options (mount/args)))

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
  (assoc raft-state
         :role :follower))

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
  (let [not-granted-resp {:response {:type :request-vote-response
                                     :vote-granted false
                                     :term (:current-term state)}
                          :state state}]
    (cond
      (< (:term data) (:current-term state))
      (do (log/info "Not granting vote -- term out of date.")
          not-granted-resp)

      (some-> (:term (:voted-for state))
              (>= (:term data)))
      (do (log/info "Not granting vote -- voted for" (:voted-for state))
          not-granted-resp)

      (< (:last-log-index data) (count (:log state)))
      (do (log/info "Not granting vote -- index out of date.")
          not-granted-resp)
      
      :else
      (do
        (log/info "Voting for" (:candidate-id data))
        {:response {:type :request-vote-response
                    :vote-granted true
                    :term (:current-term state)}
         :state (-> state
                    (assoc :voted-for {:candidate (:candidate-id data)
                                       :term (:term data)})
                    reset-timeout)}))))

(defn handle-read
  "If leader, send AppendEntries to confirm leadership, then reply with log.
   Otherwise, redirect to the leader."
  [raft-state event]
  ;;FIXME: don't handle reads as a follower, and send a round of AppendEntries
  ;;before replying to a read as a leader
  (log/info "Handling read:" event)
  (d/success! (:result event) (:log raft-state))
  raft-state)

(defn handle-write
  "If leader, send AppendEntries and wait for majority success, then reply success.
   Otherwise, redirect to the leader."
  [raft-state event]
  raft-state)

(defn handle-rpc [raft-state event]
  (let [{:keys [response state]}
        (case (:type (:rpc event))
          :append-entries (handle-append-entries raft-state (:rpc event))
          :request-vote (handle-request-vote raft-state (:rpc event)))]
    (s/put! (:cb-stream event) response)
    state))

(defn become-leader-if-elected [raft-state]
  (if (> (:votes-received raft-state) (/ (count (:peer config))
                                         2))
    (do (log/info "Received" (:votes-received raft-state) "votes -- becoming :leader.")
        (assoc raft-state
               :role :leader
               :voted-for nil))
    raft-state))

(defn handle-vote [raft-state event]
  (if (< (:term event) (:current-term raft-state))
    ;; Out of date -- ignore.
    raft-state
    (if (:vote-granted event)
      (do (log/info "Vote granted for term" (:term event))          
          (-> raft-state
              (update :votes-received inc)
              become-leader-if-elected))
      (do (log/info "Vote not granted for term" (:term event))
          (assoc raft-state
                 :current-term (:term event))))))

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
    (handle-write raft-state event)

    (contains? event :vote-granted)
    (handle-vote raft-state event)

    :else (log/warn "Unrecognized event:" event)))

(defn send-request-votes
  "Asynchronously request votes from peers."
  [raft-state event-stream]  
  (log/info "Requesting votes from peers" (:peer config)) 
  (doseq [peer (:peer config)]
    (-> (d/let-flow [req (ltcp/call-rpc peer 3456
                                        {:type :request-vote
                                         :last-log-index (count (:log raft-state))
                                         :last-log-term (or (:term (last (:log raft-state)))
                                                            0)
                                         :term (:current-term raft-state)
                                         :candidate-id (:id config)})]
          (s/put! event-stream req))
        (d/catch #(log/error "Caught error making request:" %))))  
  raft-state)

(defn run-raft [events initial-state]
  (loop [state initial-state]
    (let [ev @(s/try-take! events ::closed
                           (time-remaining state) ::timeout)]
      (log/info "Got event:" ev)
      (cond
        (= ev ::timeout) (-> state
                             become-candidate
                             (send-request-votes events)
                             recur)
        (= ev ::closed) nil ; just stop?
        :else (recur (handle-event state ev))))))
