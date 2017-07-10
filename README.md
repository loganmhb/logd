# logd

A toy implementation of the Raft protocol, backing a consistent, persistent distributed log.

To run a three-node cluster with Docker:

    make run

which does:

    lein uberjar
    docker-compose build
    docker-compose up

The nodes will elect a leader (though sometimes it takes a little
while). After a leader has been elected, if you stop the leader with
`docker-compose stop peerX`, the remaining two nodes should elect a
new leader, and if you restart the dead node with `docker-compose
start peerX` it should be able to rejoin the cluster.

Once the cluster is running, you can fetch the log or append an entry
via HTTP. The provided `docker-compose.yml` does not expose ports to
the host, so the easiest way is to run a shell in one of the
containers:

    $ docker-compose exec peer1 /bin/bash
    # curl peer2:3457/log -d "my first log entry"
    "logd.raft/ok"
    # curl peer2:3457/log
    [{"term":8,"data":"my first log entry"}]

# Tests

Run the unit tests with:

    lein test
