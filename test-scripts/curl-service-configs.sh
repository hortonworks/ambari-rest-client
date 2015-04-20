#!/bin/bash

# curl requests for retrieving service configurations from ambari
#AMBARI_HOST=172.17.0.2
#AMBARI_HOST=54.76.147.101
AMBARI_HOST=104.197.18.66
AMBARI_PORT=8080
AMBARI_USER=admin
AMBARI_PWD=admin

AMBARI_REST_CONTEXT_ROOT=api/v1
RESOURCE_BASE=http://$AMBARI_HOST:$AMBARI_PORT/$AMBARI_REST_CONTEXT_ROOT

# get clustername
clusters() {
  curl -u $AMBARI_USER:$AMBARI_PWD $RESOURCE_BASE/clusters #| jq '.items[0].Clusters.cluster_name'
}

cluster() {
  curl -u $AMBARI_USER:$AMBARI_PWD $RESOURCE_BASE/clusters/single-node-hdfs-yarn #| jq '.items[0].Clusters.cluster_name'
}

serviceConfigs(){
  #get configs for the cluster
  curl -u $AMBARI_USER:$AMBARI_PWD $RESOURCE_BASE/clusters/MySingleNodeCluster/configurations
}

serviceConfig() {

  SERVICE_NAME='global'
  VERSION_TAG=1

  #get configs for the service
  curl -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/clusters/MySingleNodeCluster/configurations?type=$SERVICE_NAME&tag=$VERSION_TAG" | jq '.items[0].properties'

}

blueprints(){
  curl -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/blueprints/single-node-hdfs-yarn"
}

hosts(){
  curl -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/hosts?fields=Hosts"
}

tasks(){
  curl -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/clusters/MySingleNodeCluster/requests/1?fields=tasks/Tasks/*"
}

requests(){
  curl -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/clusters/MySingleNodeCluster/requests"
}

services(){
  curl -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/clusters/MySingleNodeCluster/services/HDFS/components?fields=ServiceComponentInfo"
}

test() {
#curl 'http://172.18.0.2:8080/api/v1/clusters/MySingleNodeCluster/services?params/run_smoke_test=true' -X PUT -H 'Cookie: AMBARISESSIONID=1bp9xcxtlk0rp1shus9quzjlho' -H 'Origin: http://172.18.0.2:8080' -H 'Accept-Encoding: gzip,deflate,sdch' -H 'Accept-Language: en-US,en;q=0.8,de;q=0.6,hu;q=0.4,it;q=0.2,ro;q=0.2' -H 'X-Requested-By: X-Requested-By' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' -H 'Accept: application/json, text/javascript, */*; q=0.01' -H 'Referer: http://172.18.0.2:8080/' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --data '{"RequestInfo": {"context" :"_PARSE_.START.ALL_SERVICES"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' --compressed
#curl 'http://172.18.0.2:8080/api/v1/clusters/MySingleNodeCluster/services?params/run_smoke_test=true' -X PUT -H 'Cookie: AMBARISESSIONID=1bp9xcxtlk0rp1shus9quzjlho' -H 'Origin: http://172.18.0.2:8080' -H 'Accept-Encoding: gzip,deflate,sdch' -H 'Accept-Language: en-US,en;q=0.8,de;q=0.6,hu;q=0.4,it;q=0.2,ro;q=0.2' -H 'X-Requested-By: X-Requested-By' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' -H 'Accept: application/json, text/javascript, */*; q=0.01' -H 'Referer: http://172.18.0.2:8080/' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --data '{"RequestInfo": {"context" :"_PARSE_.STOP.ALL_SERVICES"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' --compressed
:
}

startAll(){
  curl --trace-ascii debugdump.txt -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/clusters/MySingleNodeCluster/services?params/run_smoke_test=true" -X PUT -H 'X-Requested-By: X-Requested-By' --data '{"RequestInfo": {"context": "Start All Services"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' --verbose
}

stopAll(){
  curl --trace-ascii debugdump.txt -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/clusters/MySingleNodeCluster/services?params/run_smoke_test=true"  -X PUT -H 'X-Requested-By: X-Requested-By' --data '{"RequestInfo": {"context": "Stop All Services"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' --verbose
}


getHDPRepository(){
  curl -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/stacks/HDP/versions/2.2/operating_systems/redhat6/repositories/HDP-2.2"
}

getUtilsRepository(){
  curl -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/stacks/HDP/versions/2.2/operating_systems/redhat6/repositories/HDP-UTILS-1.1.0.20"
}

putHDPRepository(){
  curl -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/stacks/HDP/versions/2.2/operating_systems/redhat6/repositories/HDP-2.2" -X PUT -H 'X-Requested-By: X-Requested-By' --data '{"Repositories":{"base_url":"http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.2.4.2","verify_base_url":true}}' --verbose
}
