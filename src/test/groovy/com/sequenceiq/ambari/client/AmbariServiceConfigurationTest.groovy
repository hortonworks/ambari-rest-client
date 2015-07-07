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
class AmbariServiceConfigurationTest extends AbstractAmbariClientTest {

  private enum Scenario {
    MULTIPLE_VERSIONS
  }

  def "test request service configurations map"() {
    given:
    mockResponses(Scenario.MULTIPLE_VERSIONS.name())

    when:
    Map<String, Map<String, String>> serviceConfigMap = ambari.getServiceConfigMap();

    then:
    77 == serviceConfigMap.size()
    serviceConfigMap.get("yarn-site") != [:]
  }

  def "test request service configurations map for HDFS property"() {
    given:
    mockResponses(Scenario.MULTIPLE_VERSIONS.name())

    when:
    Map<String, Map<String, String>> serviceConfigMap = ambari.getServiceConfigMap("hdfs-site");

    then:
    1 == serviceConfigMap.size()
    serviceConfigMap.get("hdfs-site").get("dfs.datanode.data.dir") == "/hadoopfs/fs1/hdfs/datanode"
  }

  def "test request service configurations map for host groups property"() {
    given:
    mockResponses(Scenario.MULTIPLE_VERSIONS.name())

    when:
    Map<String, Map<String, String>> serviceConfigMap = ambari.getServiceConfigMapByHostGroup("host_group_master_2")

    then:
    77 == serviceConfigMap.size()
    serviceConfigMap.get("hdfs-site").get("dfs.datanode.data.dir") == "/hadoopfs/fs1/hdfs/datanode,/hadoopfs/fs2/hdfs/datanode"
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
        json = "service-config-versions.json"
    } else {
      log.error("Unsupported resource path: {}", thePath)
    }
    return json
  }

}
