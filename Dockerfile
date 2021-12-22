FROM php:8.1-cli

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

RUN apt-get update -q && pecl install mongodb && echo "extension=mongodb.so" > /usr/local/etc/php/conf.d/20-mongodb.ini

RUN echo 'error_reporting = E_ALL & ~E_DEPRECATED & ~E_NOTICE' >> /usr/local/etc/php/php.ini
COPY ./ /app/

CMD php /app/vendor/bin/phpunit --bootstrap  /app/vendor/autoload.php /app/tests/Mongolina