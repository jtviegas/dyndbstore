#!/bin/bash

# http://bash.cumulonim.biz/NullGlob.html
shopt -s nullglob

this_folder="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
if [ -z "$this_folder" ]; then
  this_folder=$(dirname $(readlink -f $0))
fi

debug(){
    local __msg="$1"
    echo " [DEBUG] `date` ... $__msg "
}

info(){
    local __msg="$1"
    echo " [INFO]  `date` ->>> $__msg "
}

warn(){
    local __msg="$1"
    echo " [WARN]  `date` *** $__msg "
}

err(){
    local __msg="$1"
    echo " [ERR]   `date` !!! $__msg "
}

if [ ! -f "$this_folder/variables.inc" ]; then
  warn "we DON'T have a 'variables.inc' file"
else
  . "$this_folder/variables.inc"
fi

if [ ! -f "$this_folder/secrets.inc" ]; then
  warn "we DON'T have a 'secrets.inc' file"
else
  . "$this_folder/secrets.inc"
fi

verify(){
  info "[verify] ..."
  local _r=0

  which npm 1>/dev/null
  if [ ! "$?" -eq "0" ] ; then err "please install npm" && return 1; fi

  which docker 1>/dev/null
  if [ ! "$?" -eq "0" ] ; then err "please install docker" && return 1; fi

  info "[verify] ...done."
}


dependencies(){
  info "[dependencies] ..."
  _pwd=`pwd`
  cd "$this_folder"
  # npm install -g something
  # if [ ! "$?" -eq "0" ]; then err "[dependencies] could not install something" && cd "$_pwd" && return 1; fi
  cd "$_pwd"
  info "[dependencies] ...done."
}

publish(){
  info "[publish] ..."
  _pwd=`pwd`
  cd "$this_folder"
  npm publish . --access="public"
  if [ ! "$?" -eq "0" ]; then err "[publish] could not publish" && cd "$_pwd" && return 1; fi
  cd "$_pwd"
  info "[publish] ...done."
}

test(){
  info "[test] ..."
  _pwd=`pwd`

  info "...starting db container..."
  docker run -d -p 8000:8000 --name $DYNDBSTORE_CONTAINER "$DYNDBSTORE_DB_IMAGE"
  if [ ! "$?" -eq "0" ]; then err "[test] could not kick off db image" && return 1; fi

  local __r=0

  cd "$this_folder"
  node_modules/istanbul/lib/cli.js cover node_modules/mocha/bin/_mocha -- -R spec test/*.js
  __r=$?
  if [ ! "$__r" -eq "0" ]; then err "[test] could not test and check coverage" ; fi

  cd "$_pwd"
  info "...stopping db container..."
  docker stop $DYNDBSTORE_CONTAINER && docker rm $DYNDBSTORE_CONTAINER

  info "[test] ...done."
  return $__r
}

usage() {
  cat <<EOM
  usage:
  $(basename $0) { dependencies | publish | test | travis }

      - dependencies: installs dependencies
      - test: runs tests
      - publish: publishes to npm

EOM
  exit 1
}

verify

debug "1: $1 2: $2 3: $3 4: $4 5: $5 6: $6 7: $7 8: $8 9: $9"


case "$1" in
  dependencies)
    dependencies
    ;;
  publish)
    publish
    ;;
  test)
    test
    ;;
  *)
    usage
    ;;
esac