# Usage: ./script <server-id> <branch> <total-msg-sent=10>

# GIT_BRANCH=${2:-develop}

# Clone updated repo
rm -rf Lamport-Mutual-Exclusion 2> /dev/null
git clone --single-branch --branch develop https://github.com/mayukuse24/Lamport-Mutual-Exclusion.git
cd Lamport-Mutual-Exclusion

# Compile and start server
javac tutorial/src/app/*.java -d .
java app.Client $1 $2


