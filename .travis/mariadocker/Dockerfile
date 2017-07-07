FROM mariadb:10.2

COPY mariadocker/cmd.sh /usr/bin/

RUN chmod 777 /usr/bin/cmd.sh

EXPOSE 3306

CMD ["/usr/bin/cmd.sh"]