FROM php:7.1-cli

# Install dependencies
RUN apt-get update
RUN apt-get install  -y \
        curl \
        git \
        libfreetype6-dev \
        libjpeg62-turbo-dev \
        libmcrypt-dev \
        libpng12-dev \
        libcurl4-openssl-dev \
        pkg-config \
        libssl-dev \
        libssh2-1-dev \
        unixodbc \
        tdsodbc \
        freetds-dev

RUN docker-php-ext-install -j$(nproc) gd
RUN docker-php-ext-install -j$(nproc) mbstring
RUN docker-php-ext-install -j$(nproc) zip

RUN pecl install mongodb && echo "extension=mongodb.so" > /usr/local/etc/php/conf.d/20-mongodb.ini

COPY ./ /app/

CMD php /app/vendor/bin/phpunit --bootstrap  /app/vendor/autoload.php /app/tests/Mongolina