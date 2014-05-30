package com.sequenceiq.ambari.client

import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import spock.lang.Specification

@Slf4j
abstract class AbstractAmbariClientTest extends Specification {

  def protected ambari = new AmbariClient()

  def protected mockResponses(String scenarioStr) {
    // mocking the getResource method of the class being tested
    ambari.metaClass.getResource = { Map resourceRequestMap ->
      String jsonFileName = selectResponseJson(resourceRequestMap, scenarioStr)
      String jsonAsText = getClass().getClassLoader().getResourceAsStream(jsonFileName).text
      return new JsonSlurper().parseText(jsonAsText)
    }
  }

  def protected selectResponseJson

}
