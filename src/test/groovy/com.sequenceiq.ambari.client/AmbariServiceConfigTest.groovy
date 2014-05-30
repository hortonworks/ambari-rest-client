package com.sequenceiq.ambari.client

import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import spock.lang.Specification

@Slf4j
class AmbariServiceConfigTest extends Specification {

  enum ResourcePath {
    CLUSTER("http://localhost:8080/api/v1/clusters"),
    CONFIGURATIONS("http://localhost:8080/api/v1/clusters/MySingleNodeCluster/configurations")

    String id;

    ResourcePath(String id) {
      this.id = id
    }

    public id() {
      return id
    }
  }

  enum Scenario {
    DEFAULT, MULTIPLE_VERSIONS
  }

  def ambariClient = new AmbariClient()

  def setupSpec() {
    log.debug("Setup spec ...")
  }

  def setup() {
    log.debug("Setup...")
  }

  def "test request service configurations map"() {
    given:
    mockResponses(Scenario.DEFAULT)

    when:
    Map<String, Map<String, String>> serviceConfigMap = ambariClient.getServiceConfigMap();

    then:
    assert serviceConfigMap != [:]
    assert serviceConfigMap.get("yarn-site") != [:]
  }

  def "test request service configurations with multiple versions"() {
    given:
    mockResponses(Scenario.MULTIPLE_VERSIONS)

    when:
    Map<String, Map<String, String>> serviceConfigMap = ambariClient.getServiceConfigMap();

    then:
    assert serviceConfigMap != [:]
    assert serviceConfigMap.get("yarn-site") != [:]
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
    def json
    if (thePath == ResourcePath.CLUSTER.id()) {
      json = "clusters.json"
    } else if (thePath == ResourcePath.CONFIGURATIONS.id()) {
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
