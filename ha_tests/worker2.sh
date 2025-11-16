#!/bin/bash

ansible-rulebook -r example_ha_rulebook.yml -i /dev/null \
    --ha-postgres-dsn "postgresql://eda_user:eda_password@localhost:5432/eda_ha_db" \
    --ha-uuid aeee9a33-194d-4131-b730-168260a6f240 \
    --ha-worker-id worker-2 \
    -S ../tests/sources -v -e vars2.yml
