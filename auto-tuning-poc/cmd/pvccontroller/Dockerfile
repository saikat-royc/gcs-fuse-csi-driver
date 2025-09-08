# --- Build Stage ---
FROM golang:1.24 AS builder
WORKDIR /pvccontroller
ADD . .

RUN make pvccontroller-image BINDIR=/bin
FROM gcr.io/distroless/static-debian11 AS final

COPY --from=builder /bin/pvccontroller /pvccontroller
ENTRYPOINT ["/pvccontroller"]