#!/usr/bin/env bash

# ===> COMMON SECTION START  ===>

# http://bash.cumulonim.biz/NullGlob.html
shopt -s nullglob
# -------------------------------
this_folder="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
if [ -z "$this_folder" ]; then
  this_folder=$(dirname $(readlink -f $0))
fi
parent_folder=$(dirname "$this_folder")

# -------------------------------
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
# ---------- CONSTANTS ----------
export FILE_VARIABLES=${FILE_VARIABLES:-".variables"}
export FILE_LOCAL_VARIABLES=${FILE_LOCAL_VARIABLES:-".local_variables"}
export FILE_SECRETS=${FILE_SECRETS:-".secrets"}
export NAME="bashutils"
export INCLUDE_FILE=".${NAME}"
# -------------------------------

if [ ! -f "$this_folder/$FILE_VARIABLES" ]; then
  warn "we DON'T have a $FILE_VARIABLES variables file - creating it"
  touch "$this_folder/$FILE_VARIABLES"
else
  . "$this_folder/$FILE_VARIABLES"
fi

if [ ! -f "$this_folder/$FILE_LOCAL_VARIABLES" ]; then
  warn "we DON'T have a $FILE_LOCAL_VARIABLES variables file - creating it"
  touch "$this_folder/$FILE_LOCAL_VARIABLES"
else
  . "$this_folder/$FILE_LOCAL_VARIABLES"
fi

if [ ! -f "$this_folder/$FILE_SECRETS" ]; then
  warn "we DON'T have a $FILE_SECRETS secrets file - creating it"
  touch "$this_folder/$FILE_SECRETS"
else
  . "$this_folder/$FILE_SECRETS"
fi

# ---------- include bashutils ----------
. ${this_folder}/${INCLUDE_FILE}

# ---------- FUNCTIONS ----------

update_bashutils(){
  echo "[update_bashutils] ..."

  tar_file="${NAME}.tar.bz2"
  _pwd=`pwd`
  cd "$this_folder"

  curl -s https://api.github.com/repos/jtviegas/bashutils/releases/latest \
  | grep "browser_download_url.*${NAME}\.tar\.bz2" \
  | cut -d '"' -f 4 | wget -qi -
  tar xjpvf $tar_file
  if [ ! "$?" -eq "0" ] ; then echo "[update_bashutils] could not untar it" && cd "$_pwd" && return 1; fi
  rm $tar_file

  cd "$_pwd"
  echo "[update_bashutils] ...done."
}

# <=== COMMON SECTION END  <===
# -------------------------------------


# =======>    MAIN SECTION    =======>

# ---------- LOCAL CONSTANTS ----------


# ---------- LOCAL FUNCTIONS ----------

verify(){
  info "[verify] ..."
  local _r=0

  which npm 1>/dev/null
  if [ ! "$?" -eq "0" ] ; then err "please install npm" && return 1; fi

  which docker 1>/dev/null
  if [ ! "$?" -eq "0" ] ; then err "please install docker" && return 1; fi

  which jest 1>/dev/null
  if [ ! "$?" -eq "0" ] ; then err "please install jest" && return 1; fi

  info "[verify] ...done."
}


test(){
  info "[test|in]"
  _pwd=`pwd`

  info "...starting db container..."
  docker pull "$DYNDBSTORE_DB_IMAGE"
  docker run -d -p 8000:8000 --name $DYNDBSTORE_CONTAINER "$DYNDBSTORE_DB_IMAGE"
  if [ ! "$?" -eq "0" ]; then err "[test] could not kick off db image" && return 1; fi
  sleep 3

  local __r=0
  cd "$this_folder"
  jest
  __r=$?
  if [ ! "$__r" -eq "0" ]; then err "[test] could not test and check coverage" ; fi

  cd "$_pwd"
  info "...stopping db container..."
  docker stop $DYNDBSTORE_CONTAINER
  docker rm $DYNDBSTORE_CONTAINER

  result="$?"
  [ "$result" -ne "0" ] && err "[test|out]  => ${result}" && exit 1
  info "[test|out] => ${result}"
}


# -------------------------------------

usage() {
  cat <<EOM
  usage:
  $(basename $0) { option }
    options:
      - test: runs tests
      - publish: publishes to npm

EOM
  exit 1
}

verify

debug "1: $1 2: $2 3: $3 4: $4 5: $5 6: $6 7: $7 8: $8 9: $9"


case "$1" in
  publish)
    npm_publish "$NPM_REGISTRY" "$NPM_TOKEN" "$this_folder"
    ;;
  update)
    update_bashutils
    ;;
  test)
    test
    ;;
  *)
    usage
    ;;
esac