#! /bin/bash
mkdir -p /nodes
echo -n > /nodes/nodemap
for PORT in $(seq 16379 16384); do
  mkdir -p /nodes/$PORT
  if [[ -e /redis.conf ]]; then
    cp /redis.conf /nodes/$PORT/redis.conf
  else
    touch /nodes/$PORT/redis.conf
  fi
  cat << EOF >> /nodes/$PORT/redis.conf
port $PORT
daemonize yes
logfile /redis.log
dir /nodes/$PORT
EOF
  redis-server /nodes/$PORT/redis.conf
  echo 127.0.0.1:$PORT >> /nodes/nodemap
done
echo yes | redis-cli --cluster create $(seq -f 127.0.0.1:%g 16379 16384) --cluster-replicas 1
tail -f /redis.log
