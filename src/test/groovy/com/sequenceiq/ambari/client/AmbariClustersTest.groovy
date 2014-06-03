package com.sequenceiq.ambari.client

import groovy.util.logging.Slf4j

@Slf4j
class AmbariClustersTest extends AbstractAmbariClientTest {

  private enum Scenario {
    CLUSTERS, CLUSTER
  }

  def "test get cluster as JSON"() {
    given:
    mockResponses(Scenario.CLUSTERS.name()) // for the clustername
    mockRawResponses(Scenario.CLUSTER.name())

    expect:
    String json = ambari.getClusterAsJson();
    log.debug("JSON: {}", json)

  }

  def "test get clusters as JSON"() {
    given:
    mockRawResponses(Scenario.CLUSTERS.name())

    expect:
    String json = ambari.getClustersAsJson();
    log.debug("JSON: {}", json)
  }


  def protected String selectResponseJson(Map resourceRequestMap, String scenarioStr) {
    def thePath = resourceRequestMap.get("path");
    def query = resourceRequestMap.get("query");
    def Scenario scenario = Scenario.valueOf(scenarioStr)
    def json = null
    if (thePath == TestResources.CLUSTERS.uri()) {
      switch (scenario) {
        case Scenario.CLUSTERS: json = "clusters.json"
          break
      }
    } else if (thePath == TestResources.CLUSTER.uri()) {
      switch (scenario) {
        case Scenario.CLUSTER: json = "clusterAll.json"
          break
      }
    } else {
      log.error("Unsupported resource path: {}", thePath)
    }
    return json
  }
}
