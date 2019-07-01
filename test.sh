#!/bin/sh

this_folder="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
if [ "$this_folder" -eq "" ]; then
    this_folder=$(dirname $(readlink -f $0))
fi
parent_folder=$(dirname $this_folder)

CONTAINER=dynamodb4test

echo "starting api tests..."

echo "...starting db container..."
docker run -d -p 8000:8000 --name $CONTAINER amazon/dynamodb-local
_pwd=`pwd`
cd $this_folder
mocha --reporter spec
echo "mocha outcopme: $?"
cd $_pwd
echo "...stopping db container..."
docker stop $CONTAINER && docker rm $CONTAINER

echo "...api test done."
