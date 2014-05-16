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

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovyx.net.http.RESTClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class AmbariClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmbariClient.class)
  private static final int PAD = 30
  private static final int OK_RESPONSE = 200
  def slurper = new JsonSlurper();
  def RESTClient ambari
  def clusterName
  boolean debugEnabled = false;

  AmbariClient(host = 'localhost', port = '8080', user = 'admin', password = 'admin') {
    ambari = new RESTClient("http://${host}:${port}/api/v1/" as String)
    ambari.headers['Authorization'] = 'Basic ' + "$user:$password".getBytes('iso-8859-1').encodeBase64()
    ambari.headers['X-Requested-By'] = 'ambari'
  }

  def setDebugEnabled(boolean enabled) {
    debugEnabled = enabled;
  }

  def String getClusterName() {
    if (!clusterName) {
      def cluster = getClusters().items[0]?.Clusters
      if (cluster && cluster.desired_configs) {
        clusterName = cluster.cluster_name
      }
    }
    return clusterName
  }

  def boolean doesBlueprintExists(String id) {
    def result = false
    try {
      result = ambari.get(path: "blueprints/$id", query: ['fields': "Blueprints"]).status == OK_RESPONSE
    } catch (e) {
      LOGGER.info("Blueprint does not exist", e)
    }
    return result
  }

  def String showBlueprint(String id) {
    def resp = getBlueprint(id)
    if (resp) {
      def groups = resp.host_groups.collect {
        def name = it.name
        def comps = it.components.collect { it.name.padRight(PAD).padLeft(PAD + 10) }.join("\n")
        return "HOSTGROUP: $name\n$comps"
      }.join("\n")
      return "[${resp.Blueprints.stack_name}:${resp.Blueprints.stack_version}]\n$groups"
    }
    return "Not found"
  }

  def String showBlueprints() {
    getBlueprints().items.collect { "${it.Blueprints.blueprint_name.padRight(PAD)} [${it.Blueprints.stack_name}:${it.Blueprints.stack_version}]" }.join("\n")
  }

  def getBlueprints() {
    slurp("blueprints", "Blueprints")
  }

  def List<String> getHostGroups(String blueprint) {
    def result = getBlueprint(blueprint)
    result != null ? result.host_groups.collect { it.name } : new ArrayList<String>()
  }

  def String showClusterBlueprint() {
    ambari.get(path: "clusters/${getClusterName()}", query: ['format': "blueprint"]).data.text
  }

  def String addBlueprint(String url) {
    try {
      def blueprint = new URL(url)?.text
      if (blueprint) {
        postBlueprint(blueprint)
      } else {
        "Cannot read blueprint from $url"
      }
    } catch (e) {
      LOGGER.error("Invalid URL {}", url, e)
      "Invalid URL: ($url)"
    }
  }

  def boolean createCluster(String clusterName, String blueprintName, Map hostGroups) {
    def result = true
    try {
      ambari.post(path: "clusters/$clusterName", body: createClusterJson(blueprintName, hostGroups), { it })
    } catch (e) {
      LOGGER.error("Error during create cluster post", e)
      result = false
    }
    return result
  }

  def getClusters() {
    slurp("clusters", "Clusters")
  }

  def String showClusterList() {
    getClusters().items.collect { "[$it.Clusters.cluster_id] $it.Clusters.cluster_name:$it.Clusters.version" }.join("\n")
  }

  def getTasks(request = 1) {
    getAllResources("requests/$request", "tasks/Tasks")
  }

  def String showTaskList(request = 1) {
    getTasks(request).tasks.collect { "${it.Tasks.command_detail.padRight(PAD)} [${it.Tasks.status}]" }.join("\n")
  }

  def getHosts() {
    getAllResources("hosts", "Hosts")
  }

  def String showHostList() {
    getHosts().items.collect { "$it.Hosts.host_name [$it.Hosts.host_status] $it.Hosts.ip $it.Hosts.os_type:$it.Hosts.os_arch" }.join("\n")
  }

  def getServiceComponents(service) {
    getAllResources("services/$service/components", "ServiceComponentInfo")
  }

  def String showServiceComponents() {
    getServices().items.collect {
      def name = it.ServiceInfo.service_name
      def state = it.ServiceInfo.state
      def componentList = getServiceComponents(name).items.collect {
        "    ${it.ServiceComponentInfo.component_name.padRight(PAD)}  [$it.ServiceComponentInfo.state]"
      }.join("\n")
      "${name.padRight(PAD)} [$state]\n$componentList"
    }.join("\n")
  }

  def getServices() {
    getAllResources("services", "ServiceInfo")
  }

  def String showServiceList() {
    getServices().items.collect { "${it.ServiceInfo.service_name.padRight(PAD)} [$it.ServiceInfo.state]" }.join("\n")
  }

  def getHostComponents(host) {
    getAllResources("hosts/$host/host_components", "HostRoles")
  }

  def String showHostComponentList(host) {
    getHostComponents(host).items.collect { "${it.HostRoles.component_name.padRight(PAD)} [$it.HostRoles.state]" }.join("\n")
  }

  def getAllResources(resourceName, fields) {
    slurp("clusters/${getClusterName()}/$resourceName", "$fields/*")
  }

  def Map getBlueprint(id) {
    try {
      slurp("blueprints/$id", "host_groups,Blueprints")
    } catch (e) {
      LOGGER.error("Error during requesting blueprint: $id", e)
    }
  }

  /**
   * Posts the blueprint JSON to Ambari with name 'bp' in the URL
   * because it does not matter here. The blueprint's name is
   * provided in the JSON.
   *
   * @param blueprint json
   * @return response message
   */
  private String postBlueprint(String blueprint) {
    try {
      def status = ambari.post(path: "blueprints/bp", body: blueprint, { it }).status
      "Success: $status"
    } catch (e) {
      LOGGER.error("Error during blueprint post", e)
      "Error adding the blueprint: $e.message"
    }
  }

  private def createClusterJson(String name, Map hostGroups) {
    def builder = new JsonBuilder()
    def groups = hostGroups.collect {
      def hostList = it.value.collect { ['fqdn': it] }
      [name: it.key, hosts: hostList]
    }
    builder { "blueprint" name; "host-groups" groups }
    builder.toPrettyString()
  }

  private def slurp(path, fields) {
    if (debugEnabled) {
      def baseUri = ambari.getUri();
      println "[DEBUG] ${baseUri}${path}?fields=$fields"
    }
    slurper.parseText(ambari.get(path: "$path", query: ['fields': "$fields"]).data.text)
  }
}
