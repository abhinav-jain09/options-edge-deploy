# Smoke auth account — authenticated `/api/config` deep verification

The web smoke (`scripts/smoke/check-options-edge-web.sh`) verifies the option-chain app's posture without a
token by default (`/` + `/option-chain` == 200; `/api/config` == 401 once login is on). To additionally
prove the protected API *serves valid config behind auth*, the smoke can obtain a Keycloak token and call
`/api/config` with it. That needs a dedicated CI identity in each realm (dev + prod).

> These steps **create a Keycloak client and a secret** and **store a Jenkins credential** — provisioning
> auth identities and handling their secrets is an operator action. Run them yourself; nothing here is done
> by the pipeline or by Claude. The pipeline only *consumes* the resulting Jenkins credential.

## Design

A confidential client **`options-edge-smoke`** with:
- **service accounts enabled** → `client_credentials` grant (no user, no password, no browser flow),
- **standard flow + direct access OFF** (it is not an interactive login client),
- an **audience mapper** adding `aud: options-edge-web` so the web app's JWT validator (`AUTH_AUDIENCE`)
  accepts the token,
- **no realm roles** → least privilege: the token authenticates `/api/**` (so `/api/config` returns 200)
  but cannot stage orders (`/api/orders/spread` requires `AUTH_ORDER_ROLE`, which this client is not granted).

The client lives in the `optionsedge` realm of **each** environment's Keycloak; dev and prod get **separate
clients/secrets**.

## 1. Create the client (run per environment)

### Dev (local Keycloak Docker container `oe-keycloak`, :8089)

```sh
KC_ADMIN_PW='<dev master-realm admin password>'   # see keycloak-dev-setup notes
docker exec oe-keycloak /opt/keycloak/bin/kcadm.sh config credentials \
  --server http://localhost:8080 --realm master --user admin --password "$KC_ADMIN_PW"

docker exec oe-keycloak /opt/keycloak/bin/kcadm.sh create clients -r optionsedge \
  -s clientId=options-edge-smoke -s enabled=true \
  -s publicClient=false -s serviceAccountsEnabled=true \
  -s standardFlowEnabled=false -s directAccessGrantsEnabled=false -s 'redirectUris=[]'

CID=$(docker exec oe-keycloak /opt/keycloak/bin/kcadm.sh get clients -r optionsedge \
  -q clientId=options-edge-smoke --fields id --format csv --noquotes)

docker exec oe-keycloak /opt/keycloak/bin/kcadm.sh create clients/$CID/protocol-mappers/models \
  -r optionsedge -s name=aud-options-edge-web -s protocol=openid-connect \
  -s protocolMapper=oidc-audience-mapper \
  -s 'config."included.client.audience"=options-edge-web' \
  -s 'config."access.token.claim"=true' -s 'config."id.token.claim"=false'

# Print the generated secret (store it in Jenkins; do NOT commit it):
docker exec oe-keycloak /opt/keycloak/bin/kcadm.sh get clients/$CID/client-secret -r optionsedge
```

### Prod (Keycloak pod in the `options-edge` namespace)

```sh
POD=$(kubectl -n options-edge get pod -l app.kubernetes.io/name=oe-keycloak -o name | head -1)
ADMIN_PW=$(kubectl -n options-edge get secret oe-keycloak-secrets \
  -o jsonpath='{.data.KC_BOOTSTRAP_ADMIN_PASSWORD}' | base64 -d)

kubectl -n options-edge exec "$POD" -- /opt/keycloak/bin/kcadm.sh config credentials \
  --server http://localhost:8080 --realm master --user admin --password "$ADMIN_PW"

kubectl -n options-edge exec "$POD" -- /opt/keycloak/bin/kcadm.sh create clients -r optionsedge \
  -s clientId=options-edge-smoke -s enabled=true \
  -s publicClient=false -s serviceAccountsEnabled=true \
  -s standardFlowEnabled=false -s directAccessGrantsEnabled=false -s 'redirectUris=[]'

CID=$(kubectl -n options-edge exec "$POD" -- /opt/keycloak/bin/kcadm.sh get clients -r optionsedge \
  -q clientId=options-edge-smoke --fields id --format csv --noquotes)

kubectl -n options-edge exec "$POD" -- /opt/keycloak/bin/kcadm.sh create clients/$CID/protocol-mappers/models \
  -r optionsedge -s name=aud-options-edge-web -s protocol=openid-connect \
  -s protocolMapper=oidc-audience-mapper \
  -s 'config."included.client.audience"=options-edge-web' \
  -s 'config."access.token.claim"=true' -s 'config."id.token.claim"=false'

kubectl -n options-edge exec "$POD" -- /opt/keycloak/bin/kcadm.sh get clients/$CID/client-secret -r optionsedge
```

> Durability: a `kcadm`-created client persists in Keycloak's Postgres (survives pod restarts). For full
> config-as-code, also add the client to `k8s/keycloak/keycloak-realm-configmap.yaml` (secret externalised),
> but that is optional — the realm import only seeds a *new* realm.

## 2. Store the secret as a Jenkins credential (per controller)

On **each** Jenkins (dev and prod controllers), create a **Secret text** credential:
- **ID:** `oe-smoke-client-secret`
- **Secret:** the value printed by the `client-secret` command above (each env its own value)

## 3. Turn on deep verification

Run the `options-edge-deploy` job with:
- **`SMOKE_AUTH_CREDENTIAL_ID = oe-smoke-client-secret`**

That's it. The pipeline binds the secret only inside the "Verify OptionsEdge Web App" stage, sets the
per-environment issuer automatically (dev `http://192.168.100.102:8089/realms/optionsedge`,
prod `https://auth.fullfunding.nl/realms/optionsedge`), and the smoke then fetches a token and asserts
authenticated `/api/config` returns `200` with a `provider` body. Leave the param empty to keep posture-only.

## 4. (Optional) verify locally before wiring CI

```sh
WEB_PUBLIC_URL=http://192.168.100.252:8094 \
OPTIONS_EDGE_SMOKE_ISSUER=https://auth.fullfunding.nl/realms/optionsedge \
OPTIONS_EDGE_SMOKE_CLIENT_SECRET='<secret>' \
  scripts/smoke/check-options-edge-web.sh
# expect: "... authenticated /api/config returned valid config"
```

## Rotation / revocation

Regenerate the secret in Keycloak (`kcadm ... get clients/$CID/client-secret` after a regenerate), update
the Jenkins credential. To disable deep verify, clear `SMOKE_AUTH_CREDENTIAL_ID` (falls back to posture).
