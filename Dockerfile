FROM php:7.4-cli

# Install dependencies
RUN apt-get update -q && \
    apt-get install -y -q \
        libpng-dev libxpm-dev \
        libwebp-dev \
        libfreetype6-dev \
        libjpeg62-turbo-dev \
        && \
    docker-php-ext-configure gd \
        --with-freetype \
        --with-webp \
        --with-jpeg \
        --enable-gd && \
    docker-php-ext-install -j$(nproc) gd

RUN docker-php-ext-install -j$(nproc) gd
RUN apt-get update -q && apt-get install -y -q libonig-dev && \
    docker-php-ext-install -j$(nproc) mbstring

RUN apt-get install -y -q libzip-dev && docker-php-ext-install -j$(nproc) zip

RUN pecl install mongodb && echo "extension=mongodb.so" > /usr/local/etc/php/conf.d/20-mongodb.ini

COPY ./ /app/

CMD php /app/vendor/bin/phpunit --bootstrap  /app/vendor/autoload.php /app/tests/Mongolina