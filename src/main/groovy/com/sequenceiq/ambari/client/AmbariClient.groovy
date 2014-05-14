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
import groovyx.net.http.RESTClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class AmbariClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmbariClient.class)
  private static final int PAD = 30
  def slurper = new JsonSlurper();
  def RESTClient ambari
  def clusterName
  boolean debugEnabled = false;

  AmbariClient(host = 'localhost', port = '8080', user = 'admin', password = 'admin') {
    ambari = new RESTClient("http://${host}:${port}/api/v1/" as String)
    ambari.headers['Authorization'] = 'Basic ' + "$user:$password".getBytes('iso-8859-1').encodeBase64()
    ambari.headers['X-Requested-By'] = 'ambari'
    clusterName = clusters().items[0].Clusters.cluster_name
  }

  def setDebugEnabled(boolean enabled) {
    debugEnabled = enabled;
  }

  def String getClusterName() {
    clusterName
  }

  def slurp(path, fields) {
    if (debugEnabled) {
      def baseUri = ambari.getUri();
      println "[DEBUG] ${baseUri}${path}?fields=$fields"
    }
    slurper.parseText(ambari.get(path: "$path", query: ['fields': "$fields"]).data.text)
  }

  def getAllResources(resourceName, fields) {
    slurp("clusters/$clusterName/$resourceName", "$fields/*")
  }

  def blueprint(String id) {
    try {
      def resp = slurp("blueprints/$id", "host_groups,Blueprints")
      def groups = resp.host_groups.collect {
        def name = it.name
        def comps = it.components.collect { it.name.padRight(PAD).padLeft(PAD + 10) }.join("\n")
        return "HOSTGROUP: $name\n$comps"
      }.join("\n")
      return "[${resp.Blueprints.stack_name}:${resp.Blueprints.stack_version}]\n$groups"
    } catch (e) {
      LOGGER.error("Error during requesting blueprint: $id", e)
    }
    return "Not found"
  }

  def blueprints() {
    slurp("blueprints", "Blueprints")
  }

  def String getClusterBlueprint() {
    ambari.get(path: "clusters/$clusterName", query: ['format': "blueprint"]).data.text
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

  def String blueprintList() {
    blueprints().items.collect { "${it.Blueprints.blueprint_name.padRight(PAD)} [${it.Blueprints.stack_name}:${it.Blueprints.stack_version}]" }.join("\n")
  }

  def clusters() {
    slurp("clusters", "Clusters")
  }

  def String clusterList() {
    clusters().items.collect { "[$it.Clusters.cluster_id] $it.Clusters.cluster_name:$it.Clusters.version" }.join("\n")
  }

  def tasks(request = 1) {
    getAllResources("requests/$request", "tasks/Tasks")
  }

  def String taskList(request = 1) {
    tasks(request).tasks.collect { "${it.Tasks.command_detail.padRight(PAD)} [${it.Tasks.status}]" }.join("\n")
  }

  def hosts() {
    getAllResources("hosts", "Hosts")
  }

  def String hostList() {
    hosts().items.collect { "$it.Hosts.host_name [$it.Hosts.host_status] $it.Hosts.ip $it.Hosts.os_type:$it.Hosts.os_arch" }.join("\n")
  }

  def serviceComponents(service) {
    getAllResources("services/$service/components", "ServiceComponentInfo")
  }

  def String allServiceComponents() {
    services().items.collect {
      def name = it.ServiceInfo.service_name
      def state = it.ServiceInfo.state
      def componentList = serviceComponents(name).items.collect {
        "    ${it.ServiceComponentInfo.component_name.padRight(PAD)}  [$it.ServiceComponentInfo.state]"
      }.join("\n")
      "${name.padRight(PAD)} [$state]\n$componentList"
    }.join("\n")
  }

  def services() {
    getAllResources("services", "ServiceInfo")
  }

  def String serviceList() {
    services().items.collect { "${it.ServiceInfo.service_name.padRight(PAD)} [$it.ServiceInfo.state]" }.join("\n")
  }

  def hostComponents(host) {
    getAllResources("hosts/$host/host_components", "HostRoles")
  }

  def String hostComponentList(host) {
    hostComponents(host).items.collect { "${it.HostRoles.component_name.padRight(PAD)} [$it.HostRoles.state]" }.join("\n")
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
}
