# The t50 demo box

A private, single-tenant OxidDB serving the 50K tier. Nothing reaches it from the network: the
server is published on the host's loopback only and you connect over an SSH tunnel. No TLS, no
domain, no public endpoint.

The box **never loads data**. Loading is CPU-bound on HNSW build (103 s across a laptop's cores
for t50) and peaks at 747 MiB — 1.8x steady state. It receives a finished database instead.

## Sizing

Measured on the restored t50 database:

| | |
| --- | --- |
| Bundle to ship | 397 MB |
| Data dir on disk | ~400 MB |
| Resident (anon) | **312-402 MiB** |
| Host | 2 GB RAM / 2 vCPU / 20 GB disk is comfortable |

t180 is a different conversation — 1.9 GiB on disk and ~5 GiB resident. It buys a demo nothing.

## 1. The instance

Debian or Ubuntu, 2 GB / 2 vCPU / 20 GB. Then:

```bash
# Docker engine + compose plugin
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "$USER"   # log out and back in

# 2 GB swap. OxidDB's memory governor misreports badly (38 MB reported while the
# process held 773 MiB; vectors_index reported 0 with 50,000 vectors loaded), so
# its HTTP 507 admission control will NOT save this box. The container's
# mem_limit and this swap file are the real backstop.
sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# Firewall: SSH only.
sudo ufw allow OpenSSH && sudo ufw --force enable
```

`docker-compose.yml` publishes to `127.0.0.1`, not `0.0.0.0`. That matters more than the firewall
rule: a container published on `0.0.0.0` writes its own iptables rule that bypasses ufw entirely,
which is the usual way a "firewalled" Docker host ends up open to the internet.

## 2. Get the data onto the box — two ways

**A. The published demo image (fastest).** The dataset is already inside it; there is nothing to
ship and no restore step.

```bash
# on the box, in a dir holding docker-compose.yml and .env
docker login rg.nl-ams.scw.cloud -u nologin --password-stdin   # Scaleway secret key
docker compose up -d
```

With `OXD_IMAGE=rg.nl-ams.scw.cloud/oxid-db/oxid-db:0.9.9-t50demo` in `.env`, Compose
seeds the empty `oxid-t50-data` volume from the image on first start and leaves it alone
afterwards. Healthy in about 3 s, 50,000 vectors. linux/amd64 only.

Rebuild it after a new load with `./deploy/demo-image/build.sh --push` (bump `VERSION` — the
script refuses to overwrite an existing tag).

**B. Ship a bundle to a stock engine.** Set `OXD_IMAGE=moonlightarray/oxid-db:0.9.9` and follow
the steps below. Slower, but it is how you move a tier the demo image does not carry (t100, t180)
and how you refresh a box in place.

## 2b. Build the bundle (on your laptop)

With the tier loaded into a local container:

```bash
./deploy/make-bundle.sh                    # defaults to the oxid-dbpedia-ns container
# → dist/t50.oxdb-bundle + .sha256
```

It stops the container, compacts (folds the live deltas into a fresh base — 45 deltas / 111 MiB
last time), writes a full-fidelity `--bundle`, verifies it, and restarts the container.

`--bundle` is not optional. A single-file backup drops cold vectors, and this collection is
`cold_f32: true` with a ~192 MiB `.vecs` file beside the checkpoint.

## 3. Ship and restore (path B only)

```bash
scp dist/t50.oxdb-bundle* deploy/docker-compose.yml deploy/restore.sh <user>@<host>:~/oxid-t50/
ssh <user>@<host>
cd ~/oxid-t50
cp /dev/null .env && printf 'OXD_API_TOKEN=%s\n' "$(openssl rand -base64 24 | tr -d /=+)" >> .env
./restore.sh t50.oxdb-bundle
```

`restore.sh` checks the SHA-256, verifies the bundle *before* it touches the volume, restores,
starts the server, waits for `/health`, and prints the collection and the resident memory.

## 4. Reach it

```bash
ssh -N -L 7878:127.0.0.1:7878 -L 7880:127.0.0.1:7880 <user>@<host>
```

Everything then works against `http://127.0.0.1:7878` unchanged — the setup block in
[`../docs/queries.html`](../docs/queries.html), the loader, `oxd_oracle.py`. The OxidDB web UI is
on 7880 through the same tunnel if you would rather drive the demo from a browser.

## 5. Prove it survived the trip

```bash
# reasoning: three classes nobody asserts
curl -s -X POST http://127.0.0.1:7878/query \
  -H "Authorization: Bearer $OXD_API_TOKEN" -H "Content-Type: application/json" \
  -d '{"query":"FIND ?x WHERE ?x IS-A <http://dbpedia.org/ontology/BodyOfWater> LIMIT 8"}'

# vectors + the neurosymbolic oracle, without rewriting anything
OXIDDB_URL=http://127.0.0.1:7878 OXIDDB_API_KEY=... \
  python loader/oxiddb_load.py out/t50 --smoke --skip-tbox --skip-entities

# strongest: the offline reasoner must reproduce the pipeline's subsumptions exactly
python loader/oxd_oracle.py out/t50 --docker oxid-t50
```

The smoke test must report `"ok": true` with `"outside_scope": []`.

Check memory from the cgroup, not `docker stats` — the latter counts page cache and overstates by
roughly 2x:

```bash
docker exec oxid-t50 grep '^anon ' /sys/fs/cgroup/memory.stat
```

## If this ever needs to be public

Three additions, none of which this setup blocks:

1. `[server.tls]` with `cert_file` / `key_file` — the server refuses a non-loopback bind without
   TLS unless `--insecure` is set, and the published image sets it.
2. Users mode instead of the shared token. The token in `.env` is *legacy* auth: whoever holds it
   is treated as an admin. Public means `oxd user create --username demo --role reader` (reader can
   run queries and read individuals, nothing more) plus
   `oxd api-key create --label demo --expires-days 7 demo`.
3. `[server.cors]` allowlist, if a browser app on another origin is to call the API.
