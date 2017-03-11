(ns logd.codec
  "Provides codecs for executing RPC requests over TCP."
  (:require [gloss.core :as gloss]))

(gloss/defcodec log-entry
  {:term :int32
   :data (gloss/finite-frame :int32
                             (gloss/string :utf-8))})

(gloss/defcodec rpc-type
  (gloss/enum :byte
              :append-entries
              :request-votes
              :append-entries-response
              :request-votes-response))

(gloss/defcodec server-id
  (gloss/finite-frame :int16
                      (gloss/string :utf-8)))

(gloss/defcodec append-entries
  {:type :append-entries
   :term :int32
   :leader-id server-id
   :prev-log-index :int32
   :entries (gloss/repeated log-entry)
   :leader-commit :int32})

(gloss/defcodec append-entries-response
  {:type :append-entries-response
   :success (gloss/enum :byte true false)
   :term :int32})

(gloss/defcodec request-votes
  {:type :request-votes
   :term :int32
   :candidate-id server-id
   :last-log-index :int32
   :last-log-term :int32})

(gloss/defcodec request-votes-response
  {:type :request-votes-response
   :vote-granted (gloss/enum :byte true false)
   :term :int32})

(gloss/defcodec raft-rpc
  (gloss/header rpc-type
                {:append-entries append-entries
                 :append-entries-response append-entries-response
                 :request-votes request-votes
                 :request-votes-response request-votes-response}
                :type))
