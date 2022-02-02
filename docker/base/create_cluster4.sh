#! /bin/bash
mkdir -p /nodes
touch /nodes/nodemap
for PORT in $(seq 16391 16396); do
  mkdir -p /nodes/$PORT
  if [[ -e /redis.conf ]]; then
    cp /redis.conf /nodes/$PORT/redis.conf
  else
    touch /nodes/$PORT/redis.conf
  fi
  cat << EOF >> /nodes/$PORT/redis.conf
port ${PORT}
cluster-enabled yes
daemonize yes
logfile /redis.log
dir /nodes/$PORT
EOF
  redis-server /nodes/$PORT/redis.conf
  if [ $? -ne 0 ]; then
    echo "Redis failed to start, exiting."
    exit 3
  fi
  echo 127.0.0.1:$PORT >> /nodes/nodemap
done
echo yes | redis-cli --cluster create $(seq -f 127.0.0.1:%g 16391 16396) --cluster-replicas 1
tail -f /redis.log