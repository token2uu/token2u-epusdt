#!/usr/bin/env bash
set -euo pipefail

gateway_root="$(cd "$(dirname "$0")/../.." && pwd)"
merchant_root="${1:?Usage: bash integration/new-api/run.sh /absolute/path/to/token2u-new-api}"
merchant_root="$(cd "$merchant_root" && pwd)"
test_dir="$(mktemp -d "${TMPDIR:-/tmp}/epusdt-merchant-e2e.XXXXXX")"
trap 'rm -rf "$test_dir"' EXIT

# Add only a test bridge. Both applications' business code comes from their
# current working trees, including uncommitted fixes. No production DB/config.
python3 - "$merchant_root" "$gateway_root" "$test_dir" <<'PY'
import json, pathlib, sys
merchant, gateway, temporary = map(pathlib.Path, sys.argv[1:])
overlay = {"Replace": {str(merchant / "controller/epusdt_bridge_test.go"):
                       str(gateway / "integration/new-api/merchant_bridge_test.go.txt")}}
(temporary / "overlay.json").write_text(json.dumps(overlay))
gateway_overlay = {"Replace": {str(gateway / "src/mq/merchant_bridge.go"):
                              str(gateway / "integration/new-api/mq_bridge.go.txt"),
                              str(gateway / "src/task/merchant_bridge.go"):
                              str(gateway / "integration/new-api/task_bridge.go.txt")}}
(temporary / "gateway-overlay.json").write_text(json.dumps(gateway_overlay))
PY

(cd "$merchant_root" && go test ${EPUSDT_E2E_RACE:+-race} -overlay "$test_dir/overlay.json" -c -o "$test_dir/merchant.test" ./controller)
(cd "$gateway_root/src" && EPUSDT_MERCHANT_TEST_BINARY="$test_dir/merchant.test" go test -overlay "$test_dir/gateway-overlay.json" -tags merchant_e2e ${EPUSDT_E2E_RACE:+-race} ./mq ./task -run "${EPUSDT_E2E_RUN:-^TestMerchant(EndToEnd|NetworkEndToEnd)$}" -count=1 -v)
