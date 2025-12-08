# Razpravljanica

Avtorja: Niko Kralj in Fedja Močnik

Struktura:
* cmd/ - TUI in zaganjanje kode.
* internal/ - mogoče, če bo preveč kode za v cmd (client, server, control_unit) + storage, ki drži trenutni state.
* pkg/ interni packages (torej implementacij protobufers):
    * public (client <——> server): proto že generiran.
    * private (server <——> control_unit): proto še ni generiran.
