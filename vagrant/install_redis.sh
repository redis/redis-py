#!/usr/bin/env bash

pushd /home/vagrant

# if there's an existing redis running (started at boot?) kill it
if [ -a /etc/init.d/redis_master ]
    then
        sudo /etc/init.d/redis_master stop
        sudo update-rc.d -f redis_master remove
        sudo rm -f /etc/init.d/redis_master
fi

# delete any previous provisioned redis
rm -rf /home/vagrant/redis
rm -rf /home/vagrant/redis-2.8.8.tar.gz

# download, unpack and build redis in /home/vagrant/redis
wget http://download.redis.io/releases/redis-2.8.8.tar.gz
tar zxvf redis-2.8.8.tar.gz
mv redis-2.8.8 /home/vagrant/redis
cd /home/vagrant/redis
make

# include our overridden config options
cat /home/vagrant/redis-py/vagrant/redis-override.conf >> /home/vagrant/redis/redis.conf

# link to our init.d script
sudo cp /home/vagrant/redis-py/vagrant/redis_init_script /etc/init.d/redis_master
# and tell update-rc about it
sudo update-rc.d redis_master defaults 99
# and finally start it
sudo /etc/init.d/redis_master start

popd
