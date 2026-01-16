# Razpravljanica

Avtorja: Niko Kralj in Fedja Močnik

Project for the course Distributed Systems. Distributed chat room with a implemented chain replication on the data plane and the raft consensus protocol on the control plane.

Struktura:
* bin - binaries
* cmd - TUI in zaganjanje kode.
* internal - komponente serveja
* pkg/ - definicije protobuffers grpc:
    * public (client <—> server)
    * private (server <—> server)
    * control (control <—> client / server)

## CLI (Cobra)

Za upravljanje CLI-ja uporabljamo **Cobra** (help, podukazi, flagi).


### Raft servers:
```bash
go run ./cmd/control --raft --node-id control1 --naslov localhost:9999 --raft-addr localhost:10000 --data-dir ./data/control1 --bootstrap=true --hb-timeout 5s

go run ./cmd/control --raft --node-id control2 --naslov localhost:9998 --raft-addr localhost:10001 --data-dir ./data/control2 --peers "control1=localhost:10000=localhost:9999" --hb-timeout 5s

go run ./cmd/control --raft --node-id control3 --naslov localhost:9997 --raft-addr localhost:10002 --data-dir ./data/control3 --peers "control1=localhost:10000=localhost:9999" --hb-timeout 5s

go run tools/raftstate.go localhost:9999
```

### Data nodes:
```bash
go run ./cmd/server --naslov localhost:9876 --node-id node1 --token-secret devsecret --control localhost:9999

go run ./cmd/server --naslov localhost:9877 --node-id node2 --token-secret devsecret --control localhost:9999

go run ./cmd/server --naslov localhost:9878 --node-id node3 --token-secret devsecret --control localhost:9999

go run ./cmd/client --naslov localhost:9999 cluster-state
```

### Ukazi:
```bash
# Users
go run ./cmd/client --naslov localhost:9999 create-user "Niko"
go run ./cmd/client --naslov localhost:9999 create-user "Fedja"

# Themes
go run ./cmd/client --naslov localhost:9999 create-topic "Zagovor"
go run ./cmd/client --naslov localhost:9999 create-topic "PS"
go run ./cmd/client --naslov localhost:9999 list-topics

# Post
go run ./cmd/client --naslov localhost:9999 post --tema 1 --uporabnik 1 --besedilo "msg1"
go run ./cmd/client --naslov localhost:9999 post --tema 1 --uporabnik 2 --besedilo "msg2"
go run ./cmd/client --naslov localhost:9999 post --tema 2 --uporabnik 1 --besedilo "msg3"

# Get msg
go run ./cmd/client --naslov localhost:9999 get-messages --tema 1 --od 0 --limit 0

# Like
go run ./cmd/client --naslov localhost:9999 like --tema 1 --uporabnik 1 --sporocilo 2

#Update / delete:
go run ./cmd/client --naslov localhost:9999 update --tema 1 --uporabnik 1 --sporocilo 1 --besedilo "EDIT: posodobljen prvi post"
go run ./cmd/client --naslov localhost:9999 delete --tema 1 --uporabnik 2 --sporocilo 2

# Sub gorutine:
go run ./cmd/client --naslov localhost:9999 subscribe --uporabnik 1 --teme 1 --od 0
```

### Help za ukaze:
```bash
go run ./cmd/client --help
go run ./cmd/client create-user --help
go run ./cmd/server --help
```
