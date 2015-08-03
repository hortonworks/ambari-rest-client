/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sequenceiq.ambari.client

import groovy.util.logging.Slf4j

@Slf4j
class AmbariDecommissionTest extends AbstractAmbariClientTest {

  private enum Scenario {
    NOT_EMPTY_RESULT, EMPTY_RESULT, LIVE_NODES
  }

  def "test get decommissioning nodes"() {
    given:
    ambari.getClusterName() >> "MySingleNodeCluster"
    mockResponses(Scenario.NOT_EMPTY_RESULT.name())

    when:
    Map<String, Long> result = ambari.getDecommissioningDataNodes();

    then:
    result == ["node1-15-1417034863208.c.siq-haas.internal": 1578,
               "node3-9-1417034863274.c.siq-haas.internal" : 1436]
  }

  def "test get decommissioning nodes with no nodes"() {
    given:
    ambari.getClusterName() >> "MySingleNodeCluster"
    mockResponses(Scenario.EMPTY_RESULT.name())

    when:
    Map<String, Long> result = ambari.getDecommissioningDataNodes();

    then:
    result == [:]
  }

  def "test get dfs block size of the data nodes"() {
    given:
    ambari.getClusterName() >> "MySingleNodeCluster"
    mockResponses(Scenario.LIVE_NODES.name())

    when:
    def result = ambari.getDFSSpace();

    then:
    result == ["decomtest-1-1417096578294.c.siq-haas.internal": [9360900096: 57344],
               "decomtest-9-1417096577273.c.siq-haas.internal": [9360748544: 208896],
               "decomtest-3-1417096576478.c.siq-haas.internal": [9360900096: 57344],
               "decomtest-4-1417096576724.c.siq-haas.internal": [9360777216: 180224],
               "decomtest-5-1417096578527.c.siq-haas.internal": [9360777216: 180224]]
  }

  def protected String selectResponseJson(Map resourceRequestMap, String scenarioStr) {
    def thePath = resourceRequestMap.get("path");
    def query = resourceRequestMap.get("query");
    def Scenario scenario = Scenario.valueOf(scenarioStr)
    def json = null
    if (thePath == TestResources.GET_DECOM_NODES.uri()) {
      switch (scenario) {
        case Scenario.NOT_EMPTY_RESULT: json = "decommission.json"
          break
        case Scenario.EMPTY_RESULT: json = "decommission2.json"
          break
        case Scenario.LIVE_NODES: json = "livenodes.json"
          break
      }
    } else {
      log.error("Unsupported resource path: {}", thePath)
    }
    return json
  }
}
