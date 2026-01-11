# Razpravljanica

Avtorja: Niko Kralj in Fedja Močnik

Struktura:
* cmd/ - TUI in zaganjanje kode.
* internal/ - mogoče, če bo preveč kode za v cmd (client, server, control_unit) + storage, ki drži trenutni state.
* pkg/ interni packages (torej implementacij protobufers):
    * public (client <——> server): proto že generiran.
    * private (server <——> control_unit): proto še ni generiran.

## CLI (Cobra)

Za upravljanje CLI-ja uporabljamo **Cobra** (help, podukazi, flagi).

### Strežnik

```bash
go run ./cmd/server --naslov localhost:9876
```

### Control unit (1. del – dinamična verižna replikacija)

Najprej zaženemo dedicated control unit:

```bash
go run ./cmd/control --naslov localhost:9999
```

Nato zaženemo več nodes, ki se priključijo control unitu (chain se sestavi dinamično):

```bash
go run ./cmd/server --naslov localhost:9876 --node-id node1 --control localhost:9999
go run ./cmd/server --naslov localhost:9877 --node-id node2 --control localhost:9999
go run ./cmd/server --naslov localhost:9878 --node-id node3 --control localhost:9999
```

### Control unit (2. del – več nadzornih vozlišč z Raft)

Za 2. del naloge control plane teče kot Raft cluster.

1) Zaženi bootstrap node:

```bash
go run ./cmd/control --raft \
  --node-id control1 \
  --naslov localhost:9999 \
  --raft-addr localhost:10000 \
  --data-dir ./data/control1 \
  --bootstrap \
  --hb-timeout 30s
```

2) Zaženi še dva followerja (se samodejno pridružita prek seed peerjev):

```bash
go run ./cmd/control --raft \
  --node-id control2 \
  --naslov localhost:9998 \
  --raft-addr localhost:10001 \
  --data-dir ./data/control2 \
  --peers "control1=localhost:10000=localhost:9999" \
  --hb-timeout 30s

go run ./cmd/control --raft \
  --node-id control3 \
  --naslov localhost:9997 \
  --raft-addr localhost:10002 \
  --data-dir ./data/control3 \
  --peers "control1=localhost:10000=localhost:9999" \
  --hb-timeout 30s
```

3) Data-plane serverji lahko kot `--control` dobijo več naslovov (failover):

```bash
go run ./cmd/server --naslov localhost:9876 --node-id node1 --control localhost:9999,localhost:9998,localhost:9997
go run ./cmd/server --naslov localhost:9877 --node-id node2 --control localhost:9999,localhost:9998,localhost:9997
go run ./cmd/server --naslov localhost:9878 --node-id node3 --control localhost:9999,localhost:9998,localhost:9997
```

Če bootstrap control node odpove, se bo izvolil nov leader; data-plane node-i bodo avtomatsko preklopili na drugega control node-a.

Odjemalec se poveže na control unit (ker ta vedno vrača aktualen head/tail):

```bash
go run ./cmd/client --naslov localhost:9999 cluster-state
```

### Odjemalec

```bash
go run ./cmd/client --naslov localhost:9876 list-topics
go run ./cmd/client --naslov localhost:9876 create-user "Ana"
go run ./cmd/client --naslov localhost:9876 create-topic "Splošno"
go run ./cmd/client --naslov localhost:9876 post --tema 1 --uporabnik 1 --besedilo "Živjo!"
go run ./cmd/client --naslov localhost:9876 get-messages --tema 1 --od 0 --limit 10
go run ./cmd/client --naslov localhost:9876 subscribe --uporabnik 1 --teme "1,2,3"
go run ./cmd/client --naslov localhost:9876 cluster-state
```

Help za ukaze:

```bash
go run ./cmd/client --help
go run ./cmd/client create-user --help
go run ./cmd/server --help
```
