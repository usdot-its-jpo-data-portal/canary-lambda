ARG BASE_IMAGE
FROM $BASE_IMAGE

WORKDIR /home

COPY template.yaml template.yaml
COPY src src
RUN pip install -r src/requirements.txt --upgrade --target src

CMD ["/home/create-stack.sh"]
