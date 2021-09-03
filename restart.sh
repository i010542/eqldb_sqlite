cd /root/EqlDB/eqlengine
kill -9 $(ps -ef|grep eqlengine |awk '$0 !~/grep/ {print $2}' |tr -s '\n' ' ')
kill -9 $(ps -ef|grep eqllemon |awk '$0 !~/grep/ {print $2}' |tr -s '\n' ' ')
nohup gunicorn -b 0.0.0.0:8086 --certfile=lemon.net.cn.pem --keyfile=lemon.net.cn.key eqlengine:app &
nohup gunicorn -b 0.0.0.0:443 --certfile=lemon.net.cn.pem --keyfile=lemon.net.cn.key eqllemon:app &

