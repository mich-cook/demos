#
# Demo container with rabbitmq, node.js,
# and a JS script to produce and consume messages
#
# To run the demo:
#   In one terminal, start a message-demo container running:
#   $ docker run -ti --rm message-demo
#   This terminal will now have rabbitmq logs
#
#   In another terminal, exec bash in the container:
#   $ docker exec -ti <container> bash
#	Then in the container's terminal, run the messenger.js file:
#   $ ./messenger.js
#   This will now create a message queue consumer
#   and will randomly create/destroy producers every 3 seconds.
#   What's happening will output to the screen
#

FROM rabbitmq:alpine

RUN apk update && apk add nodejs npm
RUN npm install amqplib

COPY messenger.js /
