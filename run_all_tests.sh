#!/bin/sh

set -euf

podman=$(command -v podman || command -v docker)

for version in 3.6 3.7 3.8 3.9 3.10 3.11; do
  echo $version
  "$podman" container run \
    --rm \
    -e PYTHONPATH=/app/src \
    --mount type=bind,source=.,destination=/app,ro=true \
    --security-opt label=disable \
    --userns keep-id \
    --user "$(id -u):$(id -g)" \
    "python:$version" \
    python -munittest "/app/test.py"
done
