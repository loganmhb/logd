(ns logd.raft.follower-test
  (:require [logd.raft :as raft]
            [logd.raft.follower :as sut]
            [clojure.test :as t]))

(t/deftest append-entries-test
  (t/testing "append-entries returns false when :term < :current-term"
    (let [raft-state (assoc (raft/initial-raft-state []) :current-term 2)
          result (sut/append-entries raft-state {:term 1
                                                 :leader-id "peer1"
                                                 :prev-log-index 0
                                                 :prev-log-term 0
                                                 :entries []
                                                 :leader-commit 0})]
      (t/is (contains? (set (:actions result)) [:rpc-respond {:success false
                                                              :term 2}]))
      ;; Does NOT reset the election timeout -- since the AppendEntries term is
      ;; not up to date, this is not a valid leader. (FIXME: not 100%
      ;; sure this is correct behavior)
      (t/is (not (contains? (set (:actions result))
                            [:reset-election-timeout])))))
  (t/testing "append-entries returns false when log doesn't contain entry for prev-log-index"
    (let [raft-state (raft/initial-raft-state [])
          result (sut/append-entries raft-state {:term 0
                                                 :leader-id "peer1"
                                                 :prev-log-index 1
                                                 :prev-log-term 0
                                                 :entries []
                                                 :leader-commit 0})]
      (t/is (contains? (set (:actions result))
                       [:rpc-respond {:success false :term 0}]))
      ;; The call failed but the leader is valid, so we should still reset the election
      ;; timeout.
      (t/is (contains? (set (:actions result)) [:reset-election-timeout]))))
  (t/testing "append-entries returns false when log entry at prev-log-index exists but is for a different term"
    (let [raft-state (assoc (raft/initial-raft-state [])
                            :log [{:term 0 :data "good day"}])]
      (t/is (contains? (set (:actions (sut/append-entries raft-state {:term 1
                                                                      :leader-id "peer1"
                                                                      :prev-log-index 1
                                                                      :prev-log-term 1
                                                                      :entries []
                                                                      :leader-commit 0})))
                       [:rpc-respond {:success false
                                      :term 0}]))))
  (t/testing "appends new log entries and resets the election timeout"
    (let [raft-state (raft/initial-raft-state [])
          result (sut/append-entries raft-state {:term 1
                                                 :leader-id "peer1"
                                                 :prev-log-index 0
                                                 :prev-log-term 0
                                                 :entries [{:term 1 :data :something}]
                                                 :leader-commit 0})]
      (t/is (contains? (set (:actions result)) [:rpc-respond {:success true :term 1}]))
      (t/is (= [{:term 1 :data :something}]
               (:log (:state result))))
      (t/is (contains? (set (:actions result))
                       [:reset-election-timeout]))))
  (t/testing "overwrites incorrect entries when the leader overrides them"
    (let [raft-state (assoc (raft/initial-raft-state [])
                            :log [{:term 0 :data "wrong"}])
          result (sut/append-entries raft-state {:term 1
                                                 :leader-id "peer1"
                                                 :prev-log-index 0
                                                 :prev-log-term 0
                                                 :entries [{:term 1 :data "correct"}]
                                                 :leader-commit 0})]
      (t/is (contains? (set (:actions result))
                       [:rpc-respond {:success true :term 1}]))
      (t/is (= (assoc raft-state
                      :log [{:term 1 :data "correct"}]
                      :current-term 1)
               (:state result)))))
  (t/testing "updates local commit index when leader's is greater"
    (let [raft-state (assoc (raft/initial-raft-state [])
                            :log [{:term 0 :data "correct"}])
          result (sut/append-entries raft-state {:term 1
                                                 :leader-id "peer1"
                                                 :prev-log-index 0
                                                 :prev-log-term 0
                                                 :entries [{:term 1 :data "correct"}]
                                                 :leader-commit 1})]
      (t/is (contains? (set (:actions result)) [:rpc-respond {:success true :term 1}]))
      (t/is (= 1 (:commit-index (:state result)))))))


(t/deftest request-vote-test
  (t/testing "rejects vote if requester term < current-term"
    (let [raft-state (assoc (raft/initial-raft-state [])
                            :current-term 2)
          result (sut/request-vote raft-state {:term 1
                                               :candidate-id "peer1"
                                               :last-log-index 0
                                               :last-log-term 0})]
      (t/is (= raft-state (:state result)))
      (t/is (contains? (set (:actions result))
                       [:rpc-respond {:vote-granted false :term 2}]))))
  (t/testing "rejects vote if candidate's log has fewer entries"
    (let [raft-state (assoc (raft/initial-raft-state [])
                            :log [{:term 0}])
          result (sut/request-vote raft-state {:term 0
                                               :candidate-id "peer1"
                                               :last-log-index 0
                                               :last-log-term 0})]
      (t/is (contains? (set (:actions result))
                       [:rpc-respond {:vote-granted false :term 0}]))))
  (t/testing "rejects vote if already voted for another candidate"
    (let [raft-state (assoc (raft/initial-raft-state [])
                            :voted-for "peer2")
          result (sut/request-vote raft-state {:term 0
                                               :candidate-id "peer1"
                                               :last-log-index 0
                                               :last-log-term 0})]
      (t/is (contains? (set (:actions result))
                       [:rpc-respond {:vote-granted false :term 0}]))))
  (t/testing "otherwise, grants vote and records it"
    (let [raft-state (raft/initial-raft-state [])
          result (sut/request-vote raft-state {:term 0
                                               :candidate-id "peer1"
                                               :last-log-index 0
                                               :last-log-term 0})]
      (t/is (contains? (set (:actions result))
                       [:rpc-respond {:vote-granted true :term 0}]))
      (t/is (contains? (set (:actions result))
                       [:reset-election-timeout]))
      (t/is (= "peer1" (:voted-for (:state result)))))))
