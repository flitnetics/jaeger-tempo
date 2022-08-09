FROM golang:1.17.3-alpine3.14 as BUILD
WORKDIR /build
COPY . .
RUN go build ./cmd/jaeger-tempo

FROM alpine:3.14.2 as FINAL
COPY --from=BUILD /build/jaeger-tempo /go/bin/jaeger-tempo
RUN mkdir /plugin
# /plugin/ location is defined in jaeger-operator
CMD ["cp", "-r", "/go/bin/jaeger-tempo", "/plugin/jaeger-tempo"]
