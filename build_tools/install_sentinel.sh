#!/usr/bin/env bash

source /home/vagrant/redis-py/build_tools/redis_vars.sh

for filename in `ls $VAGRANT_SENTINEL_CONF_DIR`; do
    # cuts the order prefix off of the filename, e.g. 001-master -> master
    PROCESS_NAME=sentinel-`echo $filename | cut -f 2- -d -`
    echo "========================================="
    echo "INSTALLING SENTINEL SERVER: $PROCESS_NAME"
    echo "========================================="

    # make sure the instance is uninstalled (it should be already)
    uninstall_instance $PROCESS_NAME

    # base config
    mkdir -p $REDIS_CONF_DIR
    cp $REDIS_BUILD_DIR/sentinel.conf $REDIS_CONF_DIR/$PROCESS_NAME.conf
    # override config values from file
    cat $VAGRANT_SENTINEL_CONF_DIR/$filename >> $REDIS_CONF_DIR/$PROCESS_NAME.conf

    # replace placeholder variables in init.d script
    cp $VAGRANT_DIR/sentinel_init_script /etc/init.d/$PROCESS_NAME
    sed -i "s/{{ PROCESS_NAME }}/$PROCESS_NAME/g" /etc/init.d/$PROCESS_NAME
    # need to read the config file to find out what port this instance will run on
    port=`grep port $VAGRANT_SENTINEL_CONF_DIR/$filename | cut -f 2 -d " "`
    sed -i "s/{{ PORT }}/$port/g" /etc/init.d/$PROCESS_NAME
    chmod 755 /etc/init.d/$PROCESS_NAME

    # and tell update-rc.d about it
    update-rc.d $PROCESS_NAME defaults 99

    # save the $PROCESS_NAME into installed instances file
    echo $PROCESS_NAME >> $SENTINEL_INSTALLED_INSTANCES_FILE

    # start redis
    /etc/init.d/$PROCESS_NAME start
done
