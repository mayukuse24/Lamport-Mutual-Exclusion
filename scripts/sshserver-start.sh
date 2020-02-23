# Usage: ./script <ssh-server> <server-id> <port>

PORT=${3:-10000}

ssh vxk180007@$1 "bash -s" < ./server-start.sh $2 $PORT develop



