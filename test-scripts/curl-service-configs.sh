#!/bin/bash

# curl requests for retrieving service configurations from ambari
AMBARI_HOST=localhost
AMBARI_PORT=49178
AMBARI_USER=admin
AMBARI_PWD=admin

AMBARI_REST_CONTEXT_ROOT=api/v1
RESOURCE_BASE=http://$AMBARI_HOST:$AMBARI_PORT/$AMBARI_REST_CONTEXT_ROOT

# get clustername
clusters() {
  curl -u $AMBARI_USER:$AMBARI_PWD $RESOURCE_BASE/clusters #| jq '.items[0].Clusters.cluster_name'
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

test() {
#  curl -u $AMBARI_USER:$AMBARI_PWD "$RESOURCE_BASE/clusters/MySingleNodeCluster/configurations?type=global&tag=1"
:
}
