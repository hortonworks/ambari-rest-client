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
import groovyx.net.http.HttpResponseException
import org.apache.http.client.ClientProtocolException

@Slf4j
trait ViewService extends ClusterService {

  /**
   * Creates a default files view to browse HDFS.
   */
  def void createFilesView() throws URISyntaxException, ClientProtocolException, HttpResponseException, IOException {
    def viewInfo = [
      "instance_name" : "files",
      "label"         : "Files View",
      "visible"       : true,
      "description"   : "Files View",
      "cluster_handle": getClusterName()
    ]
    createView(viewInfo, "files")
  }

  /**
   * Creates a default PIG view.
   */
  def void createPigView() throws URISyntaxException, ClientProtocolException, HttpResponseException, IOException {
    def viewInfo = [
      "instance_name" : "pig",
      "label"         : "Pig View",
      "visible"       : true,
      "description"   : "Pig View",
      "cluster_handle": getClusterName(),
      "properties"    : [
        "scripts.dir": "/user/\${username}/pig/scripts",
        "jobs.dir"   : "/user/\${username}/pig/jobs"
      ]
    ]
    createView(viewInfo, "pig")
  }

  /**
   * Creates a default HIVE view.
   */
  def void createHiveView() throws URISyntaxException, ClientProtocolException, HttpResponseException, IOException {
    def viewInfo = [
      "instance_name" : "hive",
      "label"         : "Hive View",
      "visible"       : true,
      "description"   : "Hive View",
      "cluster_handle": getClusterName(),
      "properties"    : [
        "scripts.dir"                   : "/user/\${username}/hive/scripts",
        "jobs.dir"                      : "/user/\${username}/hive/jobs",
        "scripts.settings.defaults-file": "/user/\${username}/.\${instanceName}.defaultSettings"
      ]
    ]
    createView(viewInfo, "hive")
  }

  /**
   * Generic API call to create views.
   * @param viewInfo required properties for the view
   * @param type type of the view
   */
  def void createView(Map viewInfo, String type) throws URISyntaxException, ClientProtocolException, HttpResponseException, IOException {
    def body = new JsonBuilder(["ViewInstanceInfo": viewInfo]).toPrettyString()
    ambari.post(path: "views/${type.toUpperCase()}/versions/1.0.0/instances/${type.toLowerCase()}", body: body, { it })
  }

  def getViewDefinitions() throws URISyntaxException, ClientProtocolException, HttpResponseException, IOException {
    List<String> result = new ArrayList<>()
    try {
      def slurper = new groovy.json.JsonSlurper()
      def body = ambari.get(path: "views/", query: ['fields': 'versions/instances'])?.data?.text
      def resultJson = slurper.parseText(body);
      resultJson.items.each { item ->
          item.versions.each { version ->
              version.instances.each { instance ->
                  result.add(instance.ViewInstanceInfo.view_name + "/"
                          + instance.ViewInstanceInfo.version + "/"
                          + instance.ViewInstanceInfo.instance_name)
              }
          }
      }
    } catch (e) {
      log.info('Error occurred during GET request to viewinfo, exception: ', e)
    }
    return result
  }
}