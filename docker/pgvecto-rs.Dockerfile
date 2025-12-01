ARG FROM_TAG
ARG POSTGRES_VERSION
FROM scratch as nothing
ARG TARGETARCH
FROM geonholee/pgvecto-rs-binary:${FROM_TAG}-${TARGETARCH} AS binary

# CUDA 12.3.2 + cuDNN 9 이미지
FROM nvidia/cuda:12.3.2-cudnn9-runtime-ubuntu22.04 AS cuda-source

FROM postgres:$POSTGRES_VERSION
ARG TARGETARCH
COPY --from=binary /pgvecto-rs-binary-release.deb /tmp/vectors.deb
RUN apt-get update && \
    apt-get install -y /tmp/vectors.deb wget ca-certificates && \
    rm -f /tmp/vectors.deb

# ONNX Runtime 설치 (GPU 또는 CPU)
RUN set -eux; \
    if [ "$TARGETARCH" = "amd64" ]; then \
        ORT_URL="https://github.com/microsoft/onnxruntime/releases/download/v1.21.0/onnxruntime-linux-x64-gpu-1.21.0.tgz"; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        ORT_URL="https://github.com/microsoft/onnxruntime/releases/download/v1.21.0/onnxruntime-linux-aarch64-1.21.0.tgz"; \
    else \
        echo "Unsupported architecture: $TARGETARCH"; exit 1; \
    fi; \
    wget "$ORT_URL" -O onnxruntime.tgz; \
    tar -xvf onnxruntime.tgz; \
    rm onnxruntime.tgz; \
    mv onnxruntime-* /opt/onnxruntime

# CUDA + cuDNN 9 복사
COPY --from=cuda-source /usr/local/cuda/lib64 /usr/local/cuda/lib64
COPY --from=cuda-source /usr/local/cuda/include /usr/local/cuda/include
COPY --from=cuda-source /usr/lib/x86_64-linux-gnu/libcudnn* /usr/lib/x86_64-linux-gnu/
RUN ln -s /usr/local/cuda /usr/local/cuda-12.3

# CUDA 라이브러리 경로 설정
ENV LD_LIBRARY_PATH=/usr/local/cuda-12.3/lib64:/usr/local/cuda/lib64:/usr/lib/x86_64-linux-gnu:/opt/onnxruntime/lib:${LD_LIBRARY_PATH}
ENV ORT_DYLIB_PATH=/opt/onnxruntime/lib/libonnxruntime.so

CMD ["postgres", "-c" ,"shared_preload_libraries=vectors.so", "-c", "search_path=\"$user\", public, vectors", "-c", "logging_collector=on"]