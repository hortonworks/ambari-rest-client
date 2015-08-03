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
package com.sequenceiq.ambari.client.services

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j
import groovyx.net.http.ContentType
import groovyx.net.http.HttpResponseException

@Slf4j
trait StackService extends ClusterService {

  /**
   * Returns the active Stack urls as json
   *
   * @param stack name of the stack e.g HDP
   * @param stackVersion version of the stack e.g. 2.2
   * @param osType type of os e.g redhat6
   * @param repoId e.g HDP-2.2 or HDP-UTILS-1.1.0.20
   * @throws HttpResponseException in case of error
   */
  def String getStackRepositoryAsJson(String stack, String stackVersion, String osType, String repoId) throws HttpResponseException {
    String path = "stacks/$stack/versions/$stackVersion/operating_systems/$osType/repositories/$repoId";
    Map resourceRequestMap = utils.getResourceRequestMap(path, null)
    return utils.getRawResource(resourceRequestMap)
  }

  /**
   * Adds a Stack Repository to the Ambari server.
   * This API may be invoked multiple times to set Base URL for multiple OS types or Stack versions. If this step is not performed,
   * by default, blueprints will use the latest Base URL defined in the Stack. Exception is thrown if fails.
   *
   * @param stack name of the stack e.g HDP
   * @param stackVersion version of the stack e.g. 2.2
   * @param osType type of os e.g redhat6
   * @param repoId e.g HDP-2.2 or HDP-UTILS-1.1.0.20
   * @param baseUrl the url of repo
   * @param verify verify the base url
   * @throws groovyx.net.http.HttpResponseException in case of error
   */
  def String addStackRepository(String stack, String stackVersion, String osType, String repoId, String baseUrl, boolean verify) throws HttpResponseException {
    def Map bodyMap = [
            'Repositories': ['base_url': baseUrl, 'verify_base_url': verify]
    ]
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "stacks/$stack/versions/$stackVersion/operating_systems/$osType/repositories/$repoId")
    putRequestMap.put('body', new JsonBuilder(bodyMap).toPrettyString());
    ambari.put(putRequestMap)
  }
}