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
import com.sequenceiq.ambari.client.AmbariClientUtils
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import groovyx.net.http.RESTClient

@Slf4j
trait CommonService {

  static final int PAD = 30

  boolean debugEnabled = false
  AmbariClientUtils utils = new AmbariClientUtils(this)
  JsonSlurper slurper = new JsonSlurper()
  String clusterNameCache

  abstract String getClusterName()
  abstract RESTClient getAmbari()

  /**
   * Sets the debug variable. Used by printing the API calls for the Ambari Shell.
   *
   * @param enabled enable or disable
   */
  def setDebugEnabled(boolean enabled) {
    debugEnabled = enabled;
  }
}