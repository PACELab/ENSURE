#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements; and to You under the Apache License, Version 2.0.

########
#
# use the command line interface to install standard actions deployed
# automatically
#
# To run this command
# ./installRouteMgmt.sh  <AUTH> <APIHOST> <NAMESPACE> <WSK_CLI>
# AUTH, APIHOST and NAMESPACE are found in $HOME/.wskprops
# WSK_CLI="$OPENWHISK_HOME/bin/wsk"

set -e

if [ $# -eq 0 ]
then
echo "Usage: ./installRouteMgmt.sh AUTHKEY APIHOST NAMESPACE PATH_TO_WSK_CLI"
fi

AUTH="$1"
APIHOST="$2"
NAMESPACE="$3"
WSK_CLI="$4"

WHISKPROPS_FILE="$OPENWHISK_HOME/whisk.properties"
if [ -z "$GW_USER" ]; then
   GW_USER=`fgrep apigw.auth.user= $WHISKPROPS_FILE | cut -d'=' -f2`
fi
if [ -z "$GW_PWD" ]; then
    GW_PWD=`fgrep apigw.auth.pwd= $WHISKPROPS_FILE | cut -d'=' -f2-`
fi
if [ -z "$GW_HOST_V2" ]; then
    GW_HOST_V2=`fgrep apigw.host.v2= $WHISKPROPS_FILE | cut -d'=' -f2`
fi

# If the auth key file exists, read the key in the file. Otherwise, take the
# first argument as the key itself.
if [ -f "$AUTH" ]; then
    AUTH=`cat $AUTH`
fi

if [ ! -f $WSK_CLI ]; then
    echo $WSK_CLI is missing
    exit 1
fi

export WSK_CONFIG_FILE= # override local property file to avoid namespace clashes

echo Installing apimgmt package
$WSK_CLI -i --apihost "$APIHOST" package update --auth "$AUTH"  --shared no "$NAMESPACE/apimgmt" \
-a description "This package manages the gateway API configuration." \
-p gwUser "$GW_USER" \
-p gwPwd "$GW_PWD" \
-p gwUrlV2 "$GW_HOST_V2"

echo Creating NPM module .zip files
cd "$OPENWHISK_HOME/core/routemgmt/getApi"
cp "$OPENWHISK_HOME/core/routemgmt/common"/*.js .
npm install
zip -r getApi.zip *

cd "$OPENWHISK_HOME/core/routemgmt/createApi"
cp "$OPENWHISK_HOME/core/routemgmt/common"/*.js .
npm install
zip -r createApi.zip *

cd "$OPENWHISK_HOME/core/routemgmt/deleteApi"
cp "$OPENWHISK_HOME/core/routemgmt/common"/*.js .
npm install
zip -r deleteApi.zip *

echo Installing apimgmt actions
$WSK_CLI -i --apihost "$APIHOST" action update --auth "$AUTH" "$NAMESPACE/apimgmt/getApi" "$OPENWHISK_HOME/core/routemgmt/getApi/getApi.zip" \
-a description 'Retrieve the specified API configuration (in JSON format)' \
--kind nodejs:default \
-a web-export true -a final true

$WSK_CLI -i --apihost "$APIHOST" action update --auth "$AUTH" "$NAMESPACE/apimgmt/createApi" "$OPENWHISK_HOME/core/routemgmt/createApi/createApi.zip" \
-a description 'Create an API' \
--kind nodejs:default \
-a web-export true -a final true

$WSK_CLI -i --apihost "$APIHOST" action update --auth "$AUTH" "$NAMESPACE/apimgmt/deleteApi" "$OPENWHISK_HOME/core/routemgmt/deleteApi/deleteApi.zip" \
-a description 'Delete the API' \
--kind nodejs:default \
-a web-export true -a final true
