#! /bin/bash

p=$(pwd)

# setup prometheus metrics extension
if [ ! -d "hivemq-prometheus-extension/" ]; then
  echo "Setting up HiveMQ Prometheus extension"
    curl -L https://github.com/hivemq/hivemq-prometheus-extension/releases/download/4.0.11/hivemq-prometheus-extension-4.0.11.zip \
  -o $p/hivemq-prometheus-extension.zip
  unzip $p/hivemq-prometheus-extension.zip -d $p/../monitoring/
  rm $p/hivemq-prometheus-extension.zip
fi

# setup authentication extension
if [ ! -d "hivemq-file-rbac-extension/" ]; then
  echo "Setting up HiveMQ RBAC extension"
    curl -L https://github.com/hivemq/hivemq-file-rbac-extension/releases/download/4.6.2/hivemq-file-rbac-extension-4.6.2.zip \
  -o $p/hivemq-file-rbac-extension.zip
  unzip $p/hivemq-file-rbac-extension.zip -d $p/../monitoring/
  rm $p/hivemq-file-rbac-extension.zip
fi
