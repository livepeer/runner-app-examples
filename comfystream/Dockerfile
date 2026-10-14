# ComfyStream live-runner example.
#
# The ComfyUI workspace stays in the published base image. The comfystream
# package is replaced with a pinned checkout so Pipeline and custom nodes match
# runner.py. This image is built locally; it is not published by CI.
ARG BASE_IMAGE=livepeer/comfystream:latest
FROM ${BASE_IMAGE}

# livepeer/comfystream feat/comfypeer-liverunner
ARG COMFYSTREAM_REF=687f90b2f6023d8a31b548809abac02989878a2e
# eliteprox/livepeer-python-gateway pin used by that comfystream commit.
ARG GATEWAY_REF=796ca2dc833ef3c5981260c50d948639640f2895

# Leave the tree we are about to delete. ComfyUI's custom_nodes symlink and the
# base image's editable install both point at /workspace/comfystream.
WORKDIR /tmp

RUN rm -rf /workspace/comfystream \
    && git init /workspace/comfystream \
    && git -C /workspace/comfystream remote add origin https://github.com/livepeer/comfystream.git \
    && git -C /workspace/comfystream fetch --depth 1 origin "${COMFYSTREAM_REF}" \
    && git -C /workspace/comfystream checkout --detach FETCH_HEAD \
    && rm -rf /workspace/comfystream/.git

WORKDIR /workspace/comfystream

# livepeer-gateway's generated pb2 needs protobuf >= 6.31.1. ComfyUI pins
# protobuf<5; the live-runner path still runs with protobuf 6.
# The base image's --opencv-cuda step copies Ubuntu's libstdc++ over conda-forge's.
RUN /bin/bash -lc 'source /workspace/miniconda3/etc/profile.d/conda.sh \
    && conda activate comfystream \
    && conda install -n comfystream -y -c conda-forge --force-reinstall \
         "libstdcxx-ng>=14" "libstdcxx>=14" "libgcc-ng>=14" \
    && strings "${CONDA_PREFIX}/lib/libstdc++.so.6" | grep -q CXXABI_1.3.15 \
    && pip install --no-cache-dir uv \
    && uv pip install --python "$CONDA_PREFIX/bin/python" \
         "protobuf>=6.31.1" \
         "livepeer-gateway @ git+https://github.com/eliteprox/livepeer-python-gateway.git@${GATEWAY_REF}" \
         av \
         nvidia-ml-py \
    && python docker/patch_live_runner_payment_url.py \
    && pip install --no-cache-dir --no-deps -e . \
    && python -c "import av, livepeer_gateway, comfystream; from comfystream.pipeline import Pipeline; from comfystream.realtime.gpu import pinned_gpu"'

COPY runner.py /app/runner.py
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 8991

ENTRYPOINT ["/app/entrypoint.sh"]
