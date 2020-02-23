# Usage: ./script <server-id> <port> <branch>

GIT_BRANCH=${3:-master}
PORT=${2:-10000}

# kill any process running on port
fuser -k $PORT/tcp

# Clone updated repo
git clone --single-branch --branch $GIT_BRANCH https://github.com/mayukuse24/Lamport-Mutual-Exclusion.git
cd Lamport-Mutual-Exclusion
git pull

# Generate files for testing
rm -rf files/$1 2> /dev/null
mkdir -p files/$1
touch files/$1/f1 files/$1/f2 files/$1/f3 files/$1/f4

# Compile and start server
javac tutorial/src/app/*.java -d .
java app.Server $1 0.0.0.0 $PORT


