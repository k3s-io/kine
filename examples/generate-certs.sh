#!/bin/sh

# Generate self signed root CA cert
openssl req -nodes -x509 -newkey rsa:2048 -keyout ca.key -out ca.crt -subj "/C=AU/ST=VIC/L=Melbourne/O=Ranch/OU=root/CN=root/emailAddress=sample@sample.com"


# Generate server cert to be signed
openssl req -nodes -newkey rsa:2048 -keyout server.key -out server.csr -subj "/C=AU/ST=VIC/L=Melbourne/O=Ranch/OU=root/CN=localhost/emailAddress=sample@sample.com"

# Sign the server cert
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt

