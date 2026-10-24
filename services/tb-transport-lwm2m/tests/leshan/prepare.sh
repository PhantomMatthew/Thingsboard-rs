#!/usr/bin/env bash
set -euo pipefail

fixture_dir=$(cd "$(dirname "$0")" && pwd)

docker run --rm \
  -v "$fixture_dir:/work" \
  -w /work \
  docker.m.daocloud.io/library/maven:3.9-eclipse-temurin-21 \
  mvn -q -Dmaven.repo.local=.m2/repository \
  dependency:copy-dependencies -DoutputDirectory=lib compile
