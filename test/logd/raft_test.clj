(ns logd.raft-test
  (:require [logd.raft :as sut]
            [clojure.test :as t]))


(t/deftest append-entries-test
  (t/testing "append-entries returns false when :term < :current-term"
    (let [raft-state (assoc (sut/initial-raft-state []) :current-term 2)]
      (t/is (= {:success false
                :term 2}
               (:response (sut/handle-append-entries raft-state {:term 1
                                                                 :leader-id "peer1"
                                                                 :prev-log-index 0
                                                                 :prev-log-term 0
                                                                 :entries []
                                                                 :leader-commit 0}))))))
  (t/testing "append-entries returns false when log doesn't contain entry for prev-log-index, but resets the election timeout"
    (let [raft-state (sut/initial-raft-state [])]
      (t/is (= {:success false
                :term 0}
               (:response (sut/handle-append-entries raft-state {:term 0
                                                                 :leader-id "peer1"
                                                                 :prev-log-index 1
                                                                 :prev-log-term 0
                                                                 :entries []
                                                                 :leader-commit 0}))))))
  (t/testing "append-entries returns false when log entry at
  prev-log-index exists but is for a different term"
    (let [raft-state (assoc (sut/initial-raft-state [])
                            :log [{:term 0 :data "good day"}])]
      (t/is (= {:success false
                :term 0}
               (:response (sut/handle-append-entries raft-state {:term 1
                                                                 :leader-id "peer1"
                                                                 :prev-log-index 1
                                                                 :prev-log-term 1
                                                                 :entries []
                                                                 :leader-commit 0}))))))
  (t/testing "appends new log entries and resets the election timeout"
    (let [raft-state (sut/initial-raft-state [])
          initial-timeout (:election-timeout raft-state)
          result (sut/handle-append-entries raft-state {:term 1
                                                        :leader-id "peer1"
                                                        :prev-log-index 0
                                                        :prev-log-term 0
                                                        :entries [{:term 1 :data :something}]
                                                        :leader-commit 0})]
      (t/is (= {:success true :term 1} (:response result)))
      (t/is (= [{:term 1 :data :something}]
               (:log (:state result))))
      (t/is (>= (:election-timeout (:state result)) initial-timeout))))
  (t/testing "overwrites incorrect entries when the leader overrides them"
    (let [raft-state (assoc (sut/initial-raft-state [])
                            :log [{:term 0 :data "wrong"}])
          result (sut/handle-append-entries raft-state {:term 1
                                                        :leader-id "peer1"
                                                        :prev-log-index 0
                                                        :prev-log-term 0
                                                        :entries [{:term 1 :data "correct"}]
                                                        :leader-commit 0})]
      (t/is (= {:success true :term 1} (:response result)))
      (t/is (= (-> raft-state
                   (assoc :log [{:term 1 :data "correct"}]
                          :current-term 1)
                   (dissoc :election-timeout))
               (dissoc (:state result) :election-timeout)))))
  (t/testing "updates local commit index when leader's is greater"
    (let [raft-state (assoc (sut/initial-raft-state [])
                            :log [{:term 0 :data "correct"}])
          result (sut/handle-append-entries raft-state {:term 1
                                                        :leader-id "peer1"
                                                        :prev-log-index 0
                                                        :prev-log-term 0
                                                        :entries [{:term 1 :data "correct"}]
                                                        :leader-commit 1})]
      (t/is (= {:success true :term 1} (:response result)))
      (t/is (= 1 (:commit-index (:state result))))))
  (t/testing "a leader receiving an AppendEntries with higher term converts to follower"
    (let [raft-state (assoc (sut/initial-raft-state []) :role :leader)]
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
    (let [raft-state (assoc (sut/initial-raft-state [])
                            :current-term 2)
          result (sut/handle-request-vote raft-state {:term 1
                                                      :candidate-id "peer1"
                                                      :last-log-index 0
                                                      :last-log-term 0})]
      (t/is (= raft-state (:state result)))
      (t/is (= {:vote-granted false :term 2}
               (:response result)))))
  (t/testing "rejects vote if candidate's log has fewer entries"
    (let [raft-state (assoc (sut/initial-raft-state [])
                            :log [{:term 0}])
          result (sut/handle-request-vote raft-state {:term 0
                                                      :candidate-id "peer1"
                                                      :last-log-index 0
                                                      :last-log-term 0})]
      (t/is (= {:vote-granted false :term 0}
               (:response result)))))
  (t/testing "rejects vote if already voted for another candidate"
    (let [raft-state (assoc (sut/initial-raft-state [])
                            :voted-for "peer2")
          result (sut/handle-request-vote raft-state {:term 0
                                                      :candidate-id "peer1"
                                                      :last-log-index 0
                                                      :last-log-term 0})]
      (t/is (= {:vote-granted false :term 0}
               (:response result)))))
  (t/testing "otherwise, grants vote and records it"
    (let [raft-state (sut/initial-raft-state [])
          result (sut/handle-request-vote raft-state {:term 0
                                                      :candidate-id "peer1"
                                                      :last-log-index 0
                                                      :last-log-term 0})]
      (t/is (= {:vote-granted true :term 0}
               (:response result)))
      (t/is (= "peer1" (:voted-for (:state result)))))))

(t/run-tests)
