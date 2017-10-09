#   newredis

a new high performance redis server written in golang.

# Usage

    go get -u github.com/widaT/newredis
    cd cmd && go build -o newredis server.go
    ./newredis

# benchmark

    redis-benchmark  -p 6380 -n 1000000 -q -t set,get,incr,lpush,lpop

    SET: 93023.25 requests per second
    GET: 104275.29 requests per second
    INCR: 90744.10 requests per second
    LPUSH: 89766.61 requests per second
    LPOP: 86206.90 requests per second

enjoy it !

#   Thanks

[goleveldb](https://github.com/syndtr/goleveldb)

[redcon](https://github.com/tidwall/redcon)


