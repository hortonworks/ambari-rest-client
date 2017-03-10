package com.sequenceiq.ambari.client

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

@Slf4j
class AmbariClusterTemplateTest extends AbstractAmbariClientTest {

    private enum Scenario {
        AMBARI_VERSION
    }

    def "test hide quick links"() {
        given:
        mockResponses(Scenario.AMBARI_VERSION.name())

        when:
        def result = ambari.createClusterJson("test-bp", [:], "admin", "ALWAYS_APPLY",
          "admin/admin", "key", "MIT-KDC", true)

        then:
        def expected = new JsonSlurper().parseText(getClass().getClassLoader().getResourceAsStream("cluster_template.json")?.text)
        def actual = new JsonSlurper().parseText(result)
        expected == actual

    }

    protected String selectResponseJson(Map resourceRequestMap, String scenarioStr) {
        def thePath = resourceRequestMap.get("path");
        Scenario scenario = Scenario.valueOf(scenarioStr)
        def json = null
        if (thePath == TestResources.GET_AMBARI_VERSION.uri()) {
            switch (scenario) {
                case Scenario.AMBARI_VERSION: json = "ambari_version.json"
                    break
            }
        } else {
            log.error("Unsupported resource path: {}", thePath)
        }
        return json
    }

}
