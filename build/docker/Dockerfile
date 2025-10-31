FROM ubuntu:noble
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && \
    apt full-upgrade -y && \
    apt install -y locales
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en
RUN locale-gen en_US.UTF-8 && \
    apt update && \
    apt install -y supervisor git apache2 curl vim locales libapache2-mod-php php-fpm php-cli php-pgsql php-zip php-iconv php-readline php-curl php-intl php-mbstring php-yaml php-bcmath php-dom php-opcache php-gd php-xml php-xdebug openjdk-21-jre-headless postgresql authbind pv sqlite3 postgresql-16-postgis-3 rabbitmq-server && \
    a2enmod rewrite proxy_fcgi setenvif && \
    a2dissite 000-default && \
    echo 'zend_extension=xdebug.so\nxdebug.mode=coverage' | tee /etc/php/*/*/conf.d/20-xdebug.ini && \
    sed -i -E 's/^;? *ffi.enable=.*/ffi.enable=true/' /etc/php/*/*/php.ini && \
    curl -sS https://getcomposer.org/installer -o composer-setup.php && \
    php composer-setup.php --install-dir=/usr/local/bin --filename=composer && \
    rm composer-setup.php && \
    sed -i -E 's/(www-data:.*:)[^:]*/\1\/bin\/bash/g' /etc/passwd && \
    rm /var/www/html/* && \
    chown -R www-data:www-data /var/www && \
    su -l www-data -c 'mkdir /var/www/tika' && \
    su -l www-data -c 'curl https://archive.apache.org/dist/tika/3.0.0/tika-server-standard-3.0.0.jar > /var/www/tika/tika-server.jar' && \
    su -l www-data -c 'curl https://archive.apache.org/dist/tika/3.0.0/tika-app-3.0.0.jar > /var/www/tika/tika-app.jar' && \
    chown -R www-data:www-data /var/run/postgresql
COPY /root /
RUN a2ensite mod_php
WORKDIR /var/www/html
CMD ["/root/run.sh"]

