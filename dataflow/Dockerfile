FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/transform_data.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/template/requirements.txt

COPY . /template/

RUN pip install --no-cache-dir -r /template/requirements.txt