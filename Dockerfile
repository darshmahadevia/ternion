FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/quorumkv ./cmd/quorumkv && \
    CGO_ENABLED=0 go build -o /out/quorumkvctl ./cmd/quorumkvctl && \
    mkdir /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/quorumkv /usr/local/bin/quorumkv
COPY --from=build /out/quorumkvctl /usr/local/bin/quorumkvctl
COPY --from=build --chown=65532:65532 /out/data /var/lib/quorumkv
COPY demo/config /etc/quorumkv
ENTRYPOINT ["/usr/local/bin/quorumkv"]
