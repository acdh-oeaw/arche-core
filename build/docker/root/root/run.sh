#!/bin/bash
if [ "$USER_GID" != "" ]; then
    sed -i -e "/x:$USER_GID:/d" /etc/group
    groupmod -g $USER_GID www-data
    chgrp -R www-data /var/www
fi
if [ "$USER_UID" != "" ]; then
    sed -i -e "/x:$USER_UID:/d" /etc/passwd
    usermod -u $USER_UID www-data
    chown -R www-data /var/www
fi
chown -R www-data:www-data /var/run/apache2 /var/run/postgresql

su -l www-data -c 'mkdir -p /var/www/html/build/log'
if [ ! -d /var/www/html/tika ]; then
    su -l www-data -c 'ln -s /var/www/tika /var/www/html/tika'
fi
if [ ! -d /var/www/html/vendor ]; then
    su -l www-data -c 'cd /var/www/html && composer update'
fi
if [ ! -f /var/www/html/config.yaml ]; then
    su -l www-data -c 'cp /var/www/html/tests/config.yaml /var/www/html/config.yaml'
fi

if [ ! -d /var/www/postgresql ]; then
    su -l www-data -c '/usr/lib/postgresql/16/bin/initdb -D /var/www/postgresql --auth=ident -U www-data --locale en_US.UTF-8'
    su -l www-data -c '/usr/lib/postgresql/16/bin/pg_ctl start -D /var/www/postgresql' && \
    su -l www-data -c 'createdb -O www-data www-data' && \
    su -l www-data -c 'psql -f /var/www/html/build/db_schema.sql' && \
    su -l www-data -c '/usr/lib/postgresql/16/bin/pg_ctl stop -D /var/www/postgresql'
fi

/usr/bin/supervisord -c /root/supervisord.conf
