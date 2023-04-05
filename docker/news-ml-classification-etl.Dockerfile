FROM public.ecr.aws/amazonlinux/amazonlinux:2
RUN yum install gcc openssl-devel bzip2-devel libffi-devel wget tar gzip make curl -y
RUN cd /opt  && \
    wget https://www.python.org/ftp/python/3.9.16/Python-3.9.16.tgz && \
    tar xzf Python-3.9.16.tgz

WORKDIR /opt/Python-3.9.16
RUN ./configure --enable-optimizations
RUN make altinstall
RUN python -m ensurepip --upgrade
RUN python -m pip install --upgrade pip
RUN curl -sSL https://install.python-poetry.org | python3.9 -
RUN yum update -y
RUN yum install zip -y
WORKDIR /app
ADD . /app/
COPY pyproject.toml .
RUN mkdir -p dist/lambda
CMD python3.9 -m pip install -t dist/lambda . && cd dist/lambda && zip -x '*.pyc' -r ../lambda.zip .
