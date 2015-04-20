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

import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

@Slf4j
class AmbariStackRepositoryTest extends AbstractAmbariClientTest {

  def slurper = new JsonSlurper()

  private enum Scenario {
    GET_STACK_REPO, ADD_STACK_REPO
  }

  def "test get stack repositories as JSON"() {
    given:
    mockResponses(Scenario.GET_STACK_REPO.name())

    when:
    def response = ambari.getStackRepositoryAsJson("HDP", "2.2", "redhat6", "HDP-2.2" );

    then:
    def result = slurper.parseText(response)
    result.Repositories.base_url == "http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.2.4.2"

  }


  def "test add stack repository"() {
    given:
    def context
    ambari.getAmbari().metaClass.put = { Map request ->
      context = request
    }

    when:
    def response = ambari.addStackRepository("HDP", "2.2", "redhat6", "HDP-2.2", "http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.2.4.2", true );

    then:
    context.path == "stacks/HDP/versions/2.2/operating_systems/redhat6/repositories/HDP-2.2"
    def result = slurper.parseText(context.body)
    result.Repositories.base_url == "http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.2.4.2"
    result.Repositories.verify_base_url == true

  }


  def protected String selectResponseJson(Map resourceRequestMap, String scenarioStr) {
    def thePath = resourceRequestMap.get("path");
    def query = resourceRequestMap.get("query");
    def Scenario scenario = Scenario.valueOf(scenarioStr)
    def json = null
    if (thePath == TestResources.STACKS.uri()) {
      json = "stack_repository.json"
    } else {
      log.error("Unsupported resource path: {}", thePath)
    }
    return json
  }
}
