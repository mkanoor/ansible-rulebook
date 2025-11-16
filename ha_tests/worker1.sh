#!/bin/bash
ansible-rulebook -r example_ha_rulebook.yml -i /dev/null \
    --ha-postgres-dsn "postgresql://eda_user:eda_password@localhost:5432/eda_ha_db" \
    --ha-uuid aeee9a33-194d-4131-b730-168260a6f240 \
    --ha-worker-id worker-1 \
    -S ../tests/sources -v -e vars1.yml
#    --controller-url https://xx.xx.xx.xx/api/controller/ \
#    --controller-user admin \
#   --controller-password xxxxxxxxxxxxxxxxx \
#   --controller-ssl-verify false
# if you dont have access to controller remove all the controller
# parameters

