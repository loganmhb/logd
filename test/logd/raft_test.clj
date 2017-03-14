(ns logd.raft-test
  (:require [logd.raft :as sut]
            [clojure.test :as t]
            [gloss.io :as glossio]
            [logd.codec :as codec]))


(t/deftest append-entries-test
  (t/testing "append-entries returns false when :term < :current-term"
    (let [raft-state (assoc (sut/initial-raft-state "server" []) :current-term 2)]
      (t/is (= {:success false
                :type :append-entries-response
                :term 2}
               (:response (sut/handle-append-entries raft-state {:term 1
                                                                 :leader-id "peer1"
                                                                 :prev-log-index 0
                                                                 :prev-log-term 0
                                                                 :entries []
                                                                 :leader-commit 0}))))))
  (t/testing "append-entries returns false when log doesn't contain entry for prev-log-index, but resets the election timeout"
    (let [raft-state (sut/initial-raft-state "server" [])]
      (t/is (= {:success false
                :type :append-entries-response
                :term 0}
               (:response (sut/handle-append-entries raft-state {:term 0
                                                                 :leader-id "peer1"
                                                                 :prev-log-index 1
                                                                 :prev-log-term 0
                                                                 :entries []
                                                                 :leader-commit 0}))))))
  (t/testing "append-entries returns false when log entry at
  prev-log-index exists but is for a different term"
    (let [raft-state (assoc (sut/initial-raft-state "server" [])
                            :log [{:term 0 :data "good day"}])]
      (t/is (= {:success false
                :type :append-entries-response
                :term 0}
               (:response (sut/handle-append-entries raft-state {:term 1
                                                                 :leader-id "peer1"
                                                                 :prev-log-index 1
                                                                 :prev-log-term 1
                                                                 :entries []
                                                                 :leader-commit 0}))))))
  (t/testing "appends new log entries and resets the election timeout"
    (let [raft-state (sut/initial-raft-state "server" [])
          initial-timeout (:election-timeout raft-state)
          result (sut/handle-append-entries raft-state {:term 1
                                                        :leader-id "peer1"
                                                        :prev-log-index 0
                                                        :prev-log-term 0
                                                        :entries [{:term 1 :data :something}]
                                                        :leader-commit 0})]
      (t/is (= {:success true
                :type :append-entries-response
                :term 1}
               (:response result)))
      (t/is (= [{:term 1 :data :something}]
               (:log (:state result))))
      (t/is (>= (:election-timeout (:state result)) initial-timeout))))
  (t/testing "overwrites incorrect entries when the leader overrides them"
    (let [raft-state (assoc (sut/initial-raft-state "server" [])
                            :log [{:term 0 :data "wrong"}])
          result (sut/handle-append-entries raft-state {:term 1
                                                        :leader-id "peer1"
                                                        :prev-log-index 0
                                                        :prev-log-term 0
                                                        :entries [{:term 1 :data "correct"}]
                                                        :leader-commit 0})]
      (t/is (= {:success true
                :type :append-entries-response
                :term 1}
               (:response result)))
      (t/is (= (-> raft-state
                   (assoc :log [{:term 1 :data "correct"}]
                          :current-term 1)
                   (dissoc :election-timeout))
               (dissoc (:state result) :election-timeout)))))
  (t/testing "updates local commit index when leader's is greater"
    (let [raft-state (assoc (sut/initial-raft-state "server" [])
                            :log [{:term 0 :data "correct"}])
          result (sut/handle-append-entries raft-state {:term 1
                                                        :leader-id "peer1"
                                                        :prev-log-index 0
                                                        :prev-log-term 0
                                                        :entries [{:term 1 :data "correct"}]
                                                        :leader-commit 1})]
      (t/is (= {:success true
                :type :append-entries-response
                :term 1}
               (:response result)))
      (t/is (= 1 (:commit-index (:state result))))))
  (t/testing "a leader receiving an AppendEntries with higher term converts to follower"
    (let [raft-state (assoc (sut/initial-raft-state "server" []) :role :leader)]
      (t/is (= :follower (-> raft-state
                             (sut/handle-append-entries {:term 1
                                                         :leader-id "peer1"
                                                         :prev-log-index 0
                                                         :prev-log-term 0
                                                         :entries []
                                                         :leader-commit 0})
                             :state
                             :role))))))


(t/deftest request-vote-test
  (t/testing "rejects vote if requester term < current-term"
    (let [raft-state (assoc (sut/initial-raft-state "server" [])
                            :current-term 2)
          result (sut/handle-request-vote raft-state {:term 1
                                                      :candidate-id "peer1"
                                                      :last-log-index 0
                                                      :last-log-term 0})]
      (t/is (= raft-state (:state result)))
      (t/is (= {:vote-granted false
                :type :request-vote-response
                :term 2}
               (:response result)))))
  (t/testing "rejects vote if candidate's log has fewer entries"
    (let [raft-state (assoc (sut/initial-raft-state "server" [])
                            :log [{:term 0}])
          result (sut/handle-request-vote raft-state {:term 0
                                                      :candidate-id "peer1"
                                                      :last-log-index 0
                                                      :last-log-term 0})]
      (t/is (= {:vote-granted false
                :type :request-vote-response
                :term 0}
               (:response result)))))
  (t/testing "rejects vote if already voted for another candidate"
    (let [raft-state (assoc (sut/initial-raft-state "server" [])
                            :voted-for {:candidate "peer2"
                                        :term 0})
          result (sut/handle-request-vote raft-state {:term 0
                                                      :candidate-id "peer1"
                                                      :last-log-index 0
                                                      :last-log-term 0})]
      (t/is (= {:vote-granted false
                :type :request-vote-response
                :term 0}
               (:response result)))))
  (t/testing "otherwise, grants vote and records it"
    (let [raft-state (sut/initial-raft-state "server" [])
          result (sut/handle-request-vote raft-state {:term 0
                                                      :candidate-id "peer1"
                                                      :last-log-index 0
                                                      :last-log-term 0})]
      (t/is (= {:vote-granted true
                :type :request-vote-response
                :term 0}
               (:response result)))
      (t/is (= {:candidate "peer1"
                :term 0}
               (:voted-for (:state result)))))))

(t/deftest replication-needed-test
  (t/testing "when everything is up to date, returns false"
    (t/is (not (true? (sut/replication-needed?
                       {:heartbeat-timeout (+ (System/currentTimeMillis) 50)
                        :log []
                        :commit-index 0
                        :replication-state {"peer1" {:match-index 0
                                                     :rpc-sent? false
                                                     :next-index 1}}})))))
  (t/testing "when heartbeat timeout has expired returns true"
    (t/is (true? (sut/replication-needed?
                  {:heartbeat-timeout (System/currentTimeMillis)
                   :log []
                   :commit-index 0
                   :replication-state {"peer1" {:match-index 0
                                                :rpc-sent? false
                                                :next-index 1}}}))))
  (t/testing "when a peer is missing uncommitted entries returns true"
    (t/is (true? (sut/replication-needed?
                  {:heartbeat-timeout (+ (System/currentTimeMillis) 50)
                   :log [{:term 0
                          :data "my first log entry"}]
                   :commit-index 0
                   :replication-state {"peer1" {:match-index 0
                                                :rpc-sent? false
                                                :next-index 1}}}))))
  (t/testing "when a peer is missing uncommitted entries but a
  response has already been sent, returns false"
    (t/is (not (true? (sut/replication-needed?
                       {:heartbeat-timeout (+ (System/currentTimeMillis) 50)
                        :log [{:term 0
                               :data "my first log entry"}]
                        :commit-index 0
                        :replication-state {"peer1" {:match-index 0
                                                     :rpc-sent? true
                                                     :next-index 1}}}))))))

(t/deftest create-append-entries-test
  (t/testing "format is correct"
    (let [raft-state (sut/initial-raft-state "server" ["peer1"])
          req (sut/append-entries-request raft-state "peer1")]
      (t/is (seq (glossio/encode codec/raft-rpc req)))
      (t/is (not (seq (:entries (:log req)))))))
  (t/testing "sends unreplicated entries"))
