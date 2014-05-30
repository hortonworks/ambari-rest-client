package com.sequenceiq.ambari.client

import groovy.util.logging.Slf4j

@Slf4j
class AmbariServiceConfigurationTest extends AbstractAmbariClientTest {

  private enum Scenario {
    CONFIGURATIONS, MULTIPLE_VERSIONS
  }

  def "test request service configurations map"() {
    given:
    mockResponses(Scenario.CONFIGURATIONS.name())

    when:
    Map<String, Map<String, String>> serviceConfigMap = ambari.getServiceConfigMap();

    then:
    serviceConfigMap != [:]
    serviceConfigMap.get("yarn-site") != [:]
  }

  def "test request service configurations with multiple versions"() {
    given:
    mockResponses(Scenario.MULTIPLE_VERSIONS.name())

    when:
    Map<String, Map<String, String>> serviceConfigMap = ambari.getServiceConfigMap();

    then:
    serviceConfigMap != [:]
    serviceConfigMap.get("yarn-site") != [:]
  }

  // ---- helper method definitions

  def protected String selectResponseJson(Map resourceRequestMap, String scenarioStr) {
    def thePath = resourceRequestMap.get("path")
    def theQuery = resourceRequestMap.get("query")
    def Scenario scenario = Scenario.valueOf(scenarioStr)

    def json = null
    if (thePath == TestResources.CLUSTERS.uri()) {
      json = "clusters.json"
    } else if (thePath == TestResources.CONFIGURATIONS.uri()) {
      if (!theQuery) {
        switch (scenario) {
          case Scenario.MULTIPLE_VERSIONS:
            json = "service-versions-multiple.json"
            break
          case Scenario.CONFIGURATIONS:
            json = "service-versions.json"
            break
        }
      } else {
        json = "service-config.json"
      }

    } else {
      log.error("Unsupported resource path: {}", thePath)
    }
    return json
  }

}
