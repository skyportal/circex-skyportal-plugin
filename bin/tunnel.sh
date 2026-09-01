#!/usr/bin/env bash
# Forward the MSI llama.cpp servers to localhost. Needs the UMN VPN up.
#
# The servers run on the compute node agc03, which is only reachable through the
# mangi login node, so this jumps through mangi and forwards from agc03 itself —
# that works whether llama-server binds to 0.0.0.0 or only to localhost.
#
#   ./bin/tunnel.sh              # forward 8080 and 8081
#   ./bin/tunnel.sh 8081         # just the 8081 server
set -euo pipefail

USER_AT_MSI="${MSI_USER:-cough052}"
LOGIN="mangi.msi.umn.edu"
NODE="${MSI_NODE:-agc03}"
PORTS=("${@:-8080 8081}")
read -ra PORTS <<< "${PORTS[*]}"

FORWARDS=()
for port in "${PORTS[@]}"; do
  FORWARDS+=(-L "${port}:localhost:${port}")
done

echo "forwarding ${PORTS[*]} from ${NODE} via ${LOGIN} (ctrl-c to stop)"
exec ssh -N "${FORWARDS[@]}" -J "${USER_AT_MSI}@${LOGIN}" "${USER_AT_MSI}@${NODE}"
