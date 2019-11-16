#!/bin/sh

__r=0

this_folder=$(dirname $(readlink -f $0))
if [ -z  $this_folder ]; then
  this_folder="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
fi
parent_folder=$(dirname $this_folder)

echo "this_folder: $this_folder | parent_folder: $parent_folder"
CONTAINER=dynamodb4test

echo "starting api tests..."

echo "...starting db container..."
docker run -d -p 8000:8000 --name $CONTAINER amazon/dynamodb-local
_pwd=`pwd`
cd $parent_folder

export DYNDBSTORE_TEST_ENDPOINT="http://localhost:8000"
node_modules/istanbul/lib/cli.js cover node_modules/mocha/bin/_mocha -- -R spec test/*.js
__r=$?

cd $_pwd
echo "...stopping db container..."
docker stop $CONTAINER && docker rm $CONTAINER

echo "...api test done. [$__r]"
exit $__r
