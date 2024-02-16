FROM mysql:latest

COPY server.crt /etc/mysql/conf.d/server-cert.pem
COPY server.key /etc/mysql/conf.d/server-key.pem
COPY ca.crt /etc/mysql/conf.d/server-ca.pem
COPY mysql-ssl.cnf /etc/mysql/conf.d/my.cnf

ENV MYSQL_DATABASE=kine

# Here we will need to change the permission for the ssl certs
RUN chown mysql /etc/mysql/conf.d/server-cert.pem /etc/mysql/conf.d/server-key.pem /etc/mysql/conf.d/server-ca.pem

EXPOSE 3306
