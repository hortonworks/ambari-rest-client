package com.sequenceiq.ambari.client

import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import spock.lang.Specification

@Slf4j
class AmbariServiceConfigTest extends Specification {

  private enum Scenario {
    DEFAULT, MULTIPLE_VERSIONS
  }

  def ambariClient = new AmbariClient()

  def "test request service configurations map"() {
    given:
    mockResponses(Scenario.DEFAULT)

    when:
    Map<String, Map<String, String>> serviceConfigMap = ambariClient.getServiceConfigMap();

    then:
    serviceConfigMap != [:]
    serviceConfigMap.get("yarn-site") != [:]
  }

  def "test request service configurations with multiple versions"() {
    given:
    mockResponses(Scenario.MULTIPLE_VERSIONS)

    when:
    Map<String, Map<String, String>> serviceConfigMap = ambariClient.getServiceConfigMap();

    then:
    serviceConfigMap != [:]
    serviceConfigMap.get("yarn-site") != [:]
  }

  // ---- helper method definitions

  def mockResponses(Scenario scenario) {
    def scenarioFolder = null
    switch (scenario) {
      case Scenario.MULTIPLE_VERSIONS:
        scenarioFolder = "versions/"
        break
      default: scenarioFolder = "./"
        break
    }
    // mocking the getResource method of the class being tested
    ambariClient.metaClass.getResource = { Map resourceRequestMap ->
      String jsonFileName = scenarioFolder + selectResponseJson(resourceRequestMap)
      String jsonAsText = getClass().getClassLoader().getResourceAsStream(jsonFileName).text
      return new JsonSlurper().parseText(jsonAsText)
    }
  }

  def private String selectResponseJson(Map resourceRequestMap) {
    def thePath = resourceRequestMap.get("path");
    def query = resourceRequestMap.get("query");
    def json = null
    if (thePath == TestResources.CLUSTERS.uri()) {
      json = "clusters.json"
    } else if (thePath == TestResources.CONFIGURATIONS.uri()) {
      if (query) {
        json = "service-config.json"
      } else {
        json = "service-versions.json"
      }
    } else {
      log.error("Unsupported resource path: {}", thePath)
    }
    return json
  }
}
