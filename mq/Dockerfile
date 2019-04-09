#
# Demo container with rabbitmq, node.js,
# and a JS script to produce and consume messages
#

FROM rabbitmq:alpine

RUN apk update && apk add nodejs

COPY messenger.js /
