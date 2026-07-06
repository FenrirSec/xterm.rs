FROM alpine:3.18.11

WORKDIR /app

RUN apk add --no-cache bash nano libc6-compat libgcc python3

RUN addgroup -S demo && adduser -S demo -G demo

COPY packages.txt /tmp/packages.txt

COPY edgepackages.txt /tmp/edgepackages.txt

#RUN apk add --no-cache $(cat /tmp/packages.txt)

#RUN apk add --no-cache --repository=http://dl-cdn.alpinelinux.org/alpine/edge/testing/ $(cat /tmp/edgepackages.txt)

COPY xterm_rs /bin/xterm_rs

COPY bashrc /home/demo/.bashrc

USER demo

ENTRYPOINT [ "/bin/xterm_rs", "--host", "0.0.0.0" ]
