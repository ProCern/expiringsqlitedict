#!/bin/sh

set -euf

for version in 3.6 3.7 3.8 3.9 3.10 3.11; do
  echo $version
  podman container run \
    --rm \
    -e PYTHONPATH=/app/src \
    --mount type=bind,source=.,destination=/app,ro=true \
    --security-opt label=disable \
    --userns keep-id \
    --user "$(id -u):$(id -g)" \
    "python:$version" \
    python3 -munittest "/app/test.py"
done

echo centos:7
podman container run \
  --rm \
  -e PYTHONPATH=/app/src \
  --mount type=bind,source=.,destination=/app,ro=true \
  --security-opt label=disable \
  centos:7 \
  /bin/sh -c '
  set -euvf

  yum update -y
  yum install -y python3 sqlite3
  python3 -munittest "/app/test.py"
'

echo fedora:38
podman container run \
  --rm \
  -e PYTHONPATH=/app/src \
  --mount type=bind,source=.,destination=/app,ro=true \
  --security-opt label=disable \
  --userns keep-id \
  --user "$(id -u):$(id -g)" \
  fedora:38 \
  python3 -munittest "/app/test.py"
