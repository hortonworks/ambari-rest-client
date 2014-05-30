package com.sequenceiq.ambari.client

import groovy.util.logging.Slf4j

@Slf4j
class AmbariServicesTest extends AbstractAmbariClientTest {

  private enum Scenario {
    SERVICES, NO_SERVICES, NO_SERVICE_COMPONENTS
  }

  def "test get service components map"() {
    given:
    // get the name of the cluster
    super.mockResponses(Scenario.SERVICES.name())

    when:
    def Map result = ambari.getServiceComponentsMap()

    then:
    [HDFS: [
      DATANODE          : "STARTED",
      HDFS_CLIENT       : "INSTALLED",
      NAMENODE          : "STARTED",
      SECONDARY_NAMENODE: "STARTED"]
    ] == result
  }


  def "test get service components map for empty services"() {
    given:
    mockResponses(Scenario.NO_SERVICES.name())

    when:
    def Map result = ambari.getServiceComponentsMap()

    then:
    [:] == result
  }

  def "test get service components map for empty components"() {
    given:
    // get the name of the cluster
    mockResponses(Scenario.NO_SERVICE_COMPONENTS.name())

    when:
    def Map result = ambari.getServiceComponentsMap()

    then:
    [HDFS: [:]] == result
  }

  def "test get services as map"() {
    given:
    super.mockResponses(Scenario.SERVICES.name())

    when:
    def result = ambari.getServicesMap()

    then:
    [HDFS: "STARTED"] == result
  }

  def "test get services as map for empty result"() {
    given:
    mockResponses(Scenario.NO_SERVICES.name())

    when:
    def result = ambari.getServicesMap()

    then:
    [:] == result
  }

  def private String selectResponseJson(Map resourceRequestMap, String scenarioStr) {
    def thePath = resourceRequestMap.get("path");
    def Scenario scenario = Scenario.valueOf(scenarioStr)

    def json = null
    if (thePath == TestResources.CLUSTERS.uri()) {
      json = "clusters.json"
    } else if (thePath == TestResources.SERVICES.uri()) {
      switch (scenario) {
        case Scenario.SERVICES: json = "services.json"
          break
        case Scenario.NO_SERVICES: json = "no-services.json"
          break
        case Scenario.NO_SERVICE_COMPONENTS:
          json = "services.json"
          break
      }
    } else if (thePath == TestResources.SERVICE_COMPONENTS.uri()) {
      switch (scenario) {
        case Scenario.NO_SERVICE_COMPONENTS: json = "no-service-components-hdfs.json"
          break
        default:
          json = "service-components-hdfs.json"
          break
      }
    } else {
      log.error("Unsupported resource path: {}", thePath)
    }
    return json
  }
}
