package com.sequenceiq.ambari.client

enum TestResources {
  CLUSTERS("http://localhost:8080/api/v1/clusters"),
  CONFIGURATIONS("http://localhost:8080/api/v1/clusters/MySingleNodeCluster/configurations"),
  BLUEPRINTS("http://localhost:8080/api/v1/blueprints"),
  BLUEPRINT("http://localhost:8080/api/v1/blueprints/single-node-hdfs-yarn"),
  INEXISTENT_BLUEPRINT("http://localhost:8080/api/v1/blueprints/inexistent-blueprint"),
  HOSTS("http://localhost:8080/api/v1/hosts")

  String uri;

  TestResources(String uri) {
    this.uri = uri
  }

  public uri() {
    return this.uri
  }

}
