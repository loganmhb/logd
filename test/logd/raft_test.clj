(ns logd.raft-test
  (:require [logd.raft :as sut]
            [clojure.test :as t]
            [clojure.core.async :as async]))


(t/deftest run-follower-test
  (t/testing "after election timeout, run-follower returns a candidate transition"
    (let [initial-timeout (async/timeout 10)
          raft-state (assoc (sut/initial-raft-state [])
                            :election-timeout initial-timeout)
          channels {:append-entries (async/chan)
                    :request-vote (async/chan)}
          next-state (sut/run-follower raft-state channels)]
      (t/is (= :candidate (:role next-state)))
      (t/is (false? (:requested-votes? next-state)))
      (t/is (zero? (:votes-received next-state)))
      (t/is (not= initial-timeout (:election-timeout next-state)))))
  (t/testing "receiving an append-entries call resets the election timeout"
    (let [initial-timeout (async/timeout 10)
          raft-state (assoc (sut/initial-raft-state [])
                            :election-timeout initial-timeout)
          channels {:append-entries (async/chan)
                    :request-vote (async/chan)}
          callback-chan (async/chan)
          _ (async/put! (:append-entries channels)
                        {:data {:term 0
                                :leader-id "peer1"
                                :prev-log-index 0
                                :prev-log-term 0
                                :entries []
                                :leader-commit 0}
                         :callback-chan callback-chan})
          next-state (future (sut/run-follower raft-state channels))]
      (t/is (= {:success true :term 0}
               (async/<!! callback-chan)))
      (t/is (= :follower (:role @next-state)))
      ;; check that a new channel has been created
      (t/is (not= (:election-timeout @next-state)
                  initial-timeout)))))


(t/deftest run-candidate-test
  (t/testing "when the candidate hasn't requested votes, it requests them"
    ))
