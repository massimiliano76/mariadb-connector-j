#!/usr/bin/env bash

ls -lrt /etc/ssl
mysqld --max-allowed-packet=$PACKET --innodb-log-file-size=$INNODB_LOG_FILE_SIZE --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci --ssl-ca=/etc/ssl/ca.crt --ssl-cert=/etc/ssl/server.crt --ssl-key=/etc/ssl/server.key --bind-address=0.0.0.0