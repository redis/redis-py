#!/usr/bin/env bash

VAGRANT_DIR=/home/vagrant/redis-py/build_tools
VAGRANT_REDIS_CONF_DIR=$VAGRANT_DIR/redis-configs
VAGRANT_SENTINEL_CONF_DIR=$VAGRANT_DIR/sentinel-configs
REDIS_VERSION=3.2.0
REDIS_DOWNLOAD_DIR=/home/vagrant/redis-downloads
REDIS_PACKAGE=redis-$REDIS_VERSION.tar.gz
REDIS_BUILD_DIR=$REDIS_DOWNLOAD_DIR/redis-$REDIS_VERSION
REDIS_DIR=/var/lib/redis
REDIS_BIN_DIR=$REDIS_DIR/bin
REDIS_CONF_DIR=$REDIS_DIR/conf
REDIS_SAVE_DIR=$REDIS_DIR/backups
REDIS_INSTALLED_INSTANCES_FILE=$REDIS_DIR/redis-instances
SENTINEL_INSTALLED_INSTANCES_FILE=$REDIS_DIR/sentinel-instances

function uninstall_instance() {
    # Expects $1 to be the init.d filename, e.g. redis-nodename or
    # sentinel-nodename

    if [ -a /etc/init.d/$1 ]; then

        echo "======================================"
        echo "UNINSTALLING REDIS SERVER: $1"
        echo "======================================"

        /etc/init.d/$1 stop
        update-rc.d -f $1 remove
        rm -f /etc/init.d/$1
    fi;
    rm -f $REDIS_CONF_DIR/$1.conf
}

function uninstall_all_redis_instances() {
    if [ -a $REDIS_INSTALLED_INSTANCES_FILE ]; then
        cat $REDIS_INSTALLED_INSTANCES_FILE | while read line; do
            uninstall_instance $line;
        done;
    fi
}

function uninstall_all_sentinel_instances() {
    if [ -a $SENTINEL_INSTALLED_INSTANCES_FILE ]; then
        cat $SENTINEL_INSTALLED_INSTANCES_FILE | while read line; do
            uninstall_instance $line;
        done;
    fi
}
