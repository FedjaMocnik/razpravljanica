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

### Control unit (2. del – dinamična verižna replikacija)

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
