This is the repository that contains Jaeger plugin to interface with Tempo.

You are free to use this software under a permissive open-source MIT license.

To fund further work and maintenance on this plugin, work will be done by flitnetics.

If you require additional support for your infrastructure, you can contact [Sales](mailto:sales@flitnetics.com)

## About

Works with [Kiali](https://kiali.io).

## Build/Compile
In order to compile the plugin from source code you can use `go build`:

```
cd /path/to/jaeger-tempo
go build ./cmd/jaeger-tempo
```

config.yml (or any name you want)
```
backend: tempo.host:3200 # no http:// here
```

## Start
In order to start plugin just tell jaeger the path to a config compiled plugin.

```
GRPC_STORAGE_PLUGIN_BINARY="./jaeger-tempo" GRPC_STORAGE_PLUGIN_CONFIGURATION_FILE=./config.yaml SPAN_STORAGE_TYPE=grpc-plugin  GRPC_STORAGE_PLUGIN_LOG_LEVEL=DEBUG ./all-in-one
```

Example with docker:

Make sure you run this in a folder with `jaeger-tempo` and `config.yml` present.
```
docker run --name jaeger -e SPAN_STORAGE_TYPE=grpc-plugin \
  -e GRPC_STORAGE_PLUGIN_BINARY="/app/jaeger-tempo" \
  -e GRPC_STORAGE_PLUGIN_CONFIGURATION_FILE=/app/config.yml \
  -e GRPC_STORAGE_PLUGIN_LOG_LEVEL=DEBUG --mount type=bind,source="$(pwd)",target=/app \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 -e ASSUME_NO_MOVING_GC_UNSAFE_RISK_IT_WITH=go1.19\
  -p 5775:5775/udp \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 14268:14268 \
  -p 14250:14250 \
  -p 9411:9411  \
  jaegertracing/all-in-one:1.22
```

For Jaeger Operator on Kubernetes for testing/demo **!!NOT PRODUCTION!!**, sample manifest:

```
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: observability

commonLabels:
  app.kubernetes.io/instance: observability

resources:
  - https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/crds/jaegertracing.io_jaegers_crd.yaml
  - https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/service_account.yaml
  - https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/role.yaml
  - https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/role_binding.yaml
  - https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/operator.yaml
  - https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/cluster_role.yaml
  - https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/cluster_role_binding.yaml
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jaeger-operator
  app: jaeger
spec:
  template:
    spec:
      containers:
      - name: jaeger-operator
        image: jaegertracing/jaeger-operator:master
        args: ["start"]
        env:
        - name: LOG-LEVEL
          value: debug
---
apiVersion: jaegertracing.io/v1
kind: Jaeger
metadata:
  name: jaeger-tempo
spec:
  strategy: allInOne
  allInOne:
    image: jaegertracing/all-in-one:latest
    options:
      log-level: debug
  storage:
    type: grpc-plugin
    grpcPlugin:
      image: ghcr.io/flitnetics/jaeger-tempo:latest
    options:
      grpc-storage-plugin:
        binary: /plugin/jaeger-tempo
        configuration-file: /plugin-config/config.yaml
        log-level: debug
  volumeMounts:
    - name: config-volume
      mountPath: /plugin-config
  volumes:
    - name: config-volume
      configMap:
        name: jaeger-tempo-config
---
apiVersion: v1
data:
  config.yaml: |-
    backend: your.tempo.host:3200
kind: ConfigMap
metadata:
  name: jaeger-tempo-config
```
