#!/bin/sh

__r=0

this_folder="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
if [ -z $this_folder ]; then
    this_folder=$(dirname $(readlink -f $0))
fi
echo "this_folder: $this_folder"
parent_folder=$(dirname $this_folder)

CONTAINER=dynamodb4test

echo "starting api tests..."

echo "...starting db container..."
docker run -d -p 8000:8000 --name $CONTAINER amazon/dynamodb-local
_pwd=`pwd`
cd $this_folder
mocha --reporter spec
__r=$?
echo "mocha outcopme: $__r"
cd $_pwd
echo "...stopping db container..."
docker stop $CONTAINER && docker rm $CONTAINER

echo "...api test done. []"
exit $__r
