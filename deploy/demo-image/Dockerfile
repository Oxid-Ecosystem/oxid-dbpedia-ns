# OxidDB 0.9.9 with the oxid-dbpedia-ns t50 tier already loaded and classified.
#
# Build context is dist/demo-context, produced by deploy/demo-image/build.sh —
# not this directory. The seed is a restored, verified backup bundle, so the
# image ships a database that has already passed `oxd verify`.
#
# The base declares VOLUME /app/data: Docker seeds an EMPTY volume from this
# image's content on first start and leaves a non-empty one alone, so the
# dataset appears automatically without ever clobbering an existing volume.
FROM moonlightarray/oxid-db:0.9.9

# uid 10001 / gid 999 is the base image's unprivileged `oxd` user.
COPY --chown=10001:999 seed/    /app/data/
COPY --chown=10001:999 dataset/ /app/dataset/

LABEL org.opencontainers.image.title="oxid-db-demo" \
      org.opencontainers.image.description="OxidDB 0.9.9 preloaded with the oxid-dbpedia-ns t50 tier: 50,000 DBpedia entities, 1536-dim text-embedding-3-small vectors, OWL 2 EL TBox (797 classes), classified and ready to query." \
      org.opencontainers.image.version="0.9.9-t50demo" \
      org.opencontainers.image.source="https://github.com/Oxid-Ecosystem/oxid-dbpedia-ns" \
      org.opencontainers.image.licenses="CC-BY-SA-4.0" \
      org.opencontainers.image.base.name="docker.io/moonlightarray/oxid-db:0.9.9" \
      io.oxid.dataset.tier="t50" \
      io.oxid.dataset.entities="50000" \
      io.oxid.dataset.vectors="50000" \
      io.oxid.dataset.dimensions="1536" \
      io.oxid.dataset.embedding="text-embedding-3-small" \
      io.oxid.engine.version="0.9.9"

# Entrypoint, CMD, ports and healthcheck are inherited unchanged from the base.
