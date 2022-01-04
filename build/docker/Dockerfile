from ubuntu:focal
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && \
    apt full-upgrade -y && \
    apt install -y locales gnupg && \
    echo "deb http://ppa.launchpad.net/ondrej/php/ubuntu/ focal main" >> /etc/apt/sources.list && \
    echo "deb http://ppa.launchpad.net/ondrej/apache2/ubuntu/ focal main" >> /etc/apt/sources.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 4F4EA0AAE5267A6C
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en
RUN locale-gen en_US.UTF-8 && \
    apt update && \
    apt install -y supervisor git apache2 curl vim locales libapache2-mod-php8.0 php8.0-cli php8.0-pgsql php8.0-zip php8.0-iconv php8.0-readline php8.0-curl php8.0-intl php8.0-mbstring php8.0-yaml php8.0-bcmath php8.0-dom php8.0-opcache php8.0-gd php8.0-xml php8.0-xdebug openjdk-11-jre-headless postgresql authbind pv sqlite3 postgresql-12-postgis-3 rabbitmq-server && \
    a2enmod rewrite && \
    echo 'zend_extension=xdebug.so\nxdebug.mode=coverage' | tee /etc/php/8.0/apache2/conf.d/20-xdebug.ini /etc/php/8.0/cli/conf.d/20-xdebug.ini && \
    curl -sS https://getcomposer.org/installer -o composer-setup.php && \
    php composer-setup.php --install-dir=/usr/local/bin --filename=composer && \
    rm composer-setup.php && \
    sed -i -E 's/(www-data:.*:)[^:]*/\1\/bin\/bash/g' /etc/passwd && \
    rm /var/www/html/* && \
    chown -R www-data:www-data /var/www && \
    su -l www-data -c 'mkdir /var/www/tika' && \
    su -l www-data -c 'curl https://archive.apache.org/dist/tika/2.1.0/tika-server-standard-2.1.0.jar > /var/www/tika/tika-server.jar' && \
    su -l www-data -c 'curl https://archive.apache.org/dist/tika/2.1.0/tika-app-2.1.0.jar > /var/www/tika/tika-app.jar' && \
    chown -R www-data:www-data /var/run/postgresql
COPY /root /
WORKDIR /var/www/html
CMD ["/root/run.sh"]

