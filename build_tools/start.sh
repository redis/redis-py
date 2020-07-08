#!/usr/bin/env bash

/etc/init.d/redis-master start
/etc/init.d/redis-slave start
/etc/init.d/sentinel-1 start
/etc/init.d/sentinel-2 start
/etc/init.d/sentinel-3 start

tail -f /var/log/redis-master /var/log/redis-slave /var/log/redis-sentinel-1 /var/log/redis-sentinel-2 /var/log/redis-sentinel-3 
