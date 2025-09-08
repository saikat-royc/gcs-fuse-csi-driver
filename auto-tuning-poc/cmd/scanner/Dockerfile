
FROM golang:1.23 AS scanner-builder
WORKDIR /scanner
ADD . .

RUN make scanner-image BINDIR=/bin
FROM python:3.10-slim-bullseye AS debian
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y vim git sysstat mpich gcc-10 g++-10 libc6 libhwloc-dev

RUN python3 -m pip install --upgrade pip
RUN python3 -m venv /workspace/venv
ENV PATH="/workspace/venv/bin:$PATH"
RUN git clone --recurse-submodules https://github.com/GoogleCloudPlatform/dataflux-client-python
WORKDIR /dataflux-client-python
# RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir google-cloud-storage
ENV PYTHONPATH="${PYTHONPATH}:/dataflux-client-python/"
COPY ./cmd/scanner/scan-bucket.py /dataflux-client-python
COPY --from=scanner-builder /bin/scanner /scanner
ENTRYPOINT ["/scanner"]
