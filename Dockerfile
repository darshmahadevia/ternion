FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ternion ./cmd/ternion && \
    CGO_ENABLED=0 go build -o /out/ternionctl ./cmd/ternionctl && \
    mkdir /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ternion /usr/local/bin/ternion
COPY --from=build /out/ternionctl /usr/local/bin/ternionctl
COPY --from=build --chown=65532:65532 /out/data /var/lib/ternion
COPY demo/config /etc/ternion
ENTRYPOINT ["/usr/local/bin/ternion"]
