FROM python:3.11-slim

WORKDIR /opt/11writer-local-intelligence
COPY local_intelligence_worker/entrypoint.py ./entrypoint.py
COPY local_intelligence/onnx_image_runner/onnx_image_runner.py /opt/runner/onnx_image_runner.py

# Setup-time dependency acquisition is separate from the sealed runtime.  These
# versions match the reviewed local provisioning environment; the running compose
# service has network_mode:none and cannot acquire anything.
RUN python -m pip install --no-cache-dir \
    numpy==2.2.6 \
    onnxruntime==1.23.2 \
    Pillow==12.2.0

# The runtime filesystem is read-only, and this unprivileged numeric identity
# remains valid even when the image is deployed without an /etc/passwd entry.
USER 65532:65532
ENV PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["python", "/opt/11writer-local-intelligence/entrypoint.py"]
