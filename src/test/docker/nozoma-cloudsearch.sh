#!/bin/sh

# Used to create a local test environment for unit testing.
# When successfully started, set the following as JVM argument when 
# running JUnit:
#
#   -Dcloudsearch.endpoint="http://localhost:15808"
#
# Source material:
#
#   Docker image: https://hub.docker.com/r/oisinmulvihill/nozama-cloudsearch
#   Source:       https://github.com/oisinmulvihill/nozama-cloudsearch#docker

# download the docker compose configuration:
#curl -O https://raw.githubusercontent.com/oisinmulvihill/nozama-cloudsearch/master/nozama-cloudsearch.yaml

echo "Starting Nozoma CloudSearch..."

cd $(cd -P -- "$(dirname -- "$0")" && pwd -P)

# Run in the background:
docker-compose -f nozama-cloudsearch.yaml up -d

# Check everything is up and running
docker-compose -f nozama-cloudsearch.yaml ps

curl http://localhost:15808/ping

echo ""
echo "Nozoma CloudSearch was started properly if everything is up/OK."

read -p "Press ENTER key to shutdown Nozoma CloudSearch..."

# To shutdown and stop all parts:
docker-compose -f nozama-cloudsearch.yaml down
