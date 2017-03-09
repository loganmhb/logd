(ns logd.raft-test
  (:require [logd.raft :as sut]
            [clojure.test :as t]))

(t/deftest append-entries-test
  (t/testing "append-entries returns false when :term < :current-term"
    (let [raft-state (assoc (sut/initial-raft-state []) :current-term 2)]
      (t/is (= {:success false
                :term 2}
               (:response (sut/append-entries raft-state {:term 1
                                                          :leader-id "peer1"
                                                          :prev-log-index 0
                                                          :prev-log-term 0
                                                          :entries []
                                                          :leader-commit 0}))))))
  (t/testing "append-entries returns false when log doesn't contain entry for prev-log-index"
    (let [raft-state (sut/initial-raft-state [])]
      (t/is (= {:success false
                :term 0}
               (:response (sut/append-entries raft-state {:term 0
                                                          :leader-id "peer1"
                                                          :prev-log-index 1
                                                          :prev-log-term 0
                                                          :entries []
                                                          :leader-commit 0}))))))
  (t/testing "append-entries returns false when log entry at prev-log-index exists but is for a different term"
    (let [raft-state (assoc (sut/initial-raft-state [])
                            :log [{:term 0 :data "good day"}])]
      (t/is (= {:success false
                :term 0}
               (:response (sut/append-entries raft-state {:term 1
                                                          :leader-id "peer1"
                                                          :prev-log-index 1
                                                          :prev-log-term 1
                                                          :entries []
                                                          :leader-commit 0}))))))
  (t/testing "appends new log entries"
    (let [raft-state (sut/initial-raft-state [])
          result (sut/append-entries raft-state {:term 1
                                                 :leader-id "peer1"
                                                 :prev-log-index 0
                                                 :prev-log-term 0
                                                 :entries [{:term 1 :data :something}]
                                                 :leader-commit 0})]
      (t/is (= {:success true :term 1} (:response result)))
      (t/is (= [{:term 1 :data :something}]
               (:log (:state result))))))
  (t/testing "overwrites incorrect entries when the leader overrides them"
    (let [raft-state (assoc (sut/initial-raft-state [])
                            :log [{:term 0 :data "wrong"}])
          result (sut/append-entries raft-state {:term 1
                                                 :leader-id "peer1"
                                                 :prev-log-index 0
                                                 :prev-log-term 0
                                                 :entries [{:term 1 :data "correct"}]
                                                 :leader-commit 0})]
      (t/is (= {:success true :term 1} (:response result)))
      (t/is (= (assoc raft-state
                      :log [{:term 1 :data "correct"}]
                      :current-term 1)
               (:state result))))))


(t/deftest request-vote-test
  (t/testing "rejects vote if requester term < current-term"
    (let [raft-state (assoc (sut/initial-raft-state [])
                            :current-term 2)
          result (sut/request-vote raft-state {:term 1
                                               :candidate-id "peer1"
                                               :last-log-index 0
                                               :last-log-term 0})]
      (t/is (= raft-state (:state result)))
      (t/is (= {:vote-granted false :term 2}
               (:response result)))))
  (t/testing "rejects vote if candidate's log has fewer entries"
    (let [raft-state (assoc (sut/initial-raft-state [])
                            :log [{:term 0}])
          result (sut/request-vote raft-state {:term 0
                                               :candidate-id "peer1"
                                               :last-log-index 0
                                               :last-log-term 0})]
      (t/is (= {:vote-granted false :term 0}
               (:response result)))))
  (t/testing "rejects vote if already voted for another candidate"
    (let [raft-state (assoc (sut/initial-raft-state [])
                            :voted-for "peer2")
          result (sut/request-vote raft-state {:term 0
                                               :candidate-id "peer1"
                                               :last-log-index 0
                                               :last-log-term 0})]
      (t/is (= {:vote-granted false :term 0}
               (:response result))))))
