#!/bin/sh

set -euf

runtest='
  python3 -mpip install --user --upgrade pip
  /mnt/home/.local/bin/pip install --user "/mnt/app[test]"
  python3 -munittest "/mnt/app/test.py"
'

for version in 3.6 3.7 3.8 3.9 3.10 3.11; do
  echo $version
  podman container run \
    --rm \
    -e PYTHONPATH=/mnt/app/src \
    -e HOME=/mnt/home \
    --mount type=volume,destination=/mnt/home \
    --mount type=bind,source=.,destination=/mnt/app,ro=true \
    --security-opt label=disable \
    --userns keep-id \
    --user "$(id -u):$(id -g)" \
    "python:$version" \
    /bin/sh -c "
      set -euvf

      $runtest
    "
done

echo centos:7
podman container run \
  --rm \
  -e PYTHONPATH=/mnt/app/src \
  -e HOME=/mnt/home \
  --mount type=volume,destination=/mnt/home \
  --mount type=bind,source=.,destination=/mnt/app,ro=true \
  --security-opt label=disable \
  centos:7 \
  /bin/sh -c "
  set -euvf

  yum update -y
  yum install -y python3 sqlite3 python3-pip
  $runtest
"

echo fedora:38
podman container run \
  --rm \
  -e PYTHONPATH=/mnt/app/src \
  -e HOME=/mnt/home \
  --mount type=volume,destination=/mnt/home \
  --mount type=bind,source=.,destination=/mnt/app,ro=true \
  --security-opt label=disable \
  fedora:38 \
  /bin/sh -c "
  set -euvf

  dnf update -y
  dnf install -y python3 sqlite3 python3-pip
  $runtest
"
