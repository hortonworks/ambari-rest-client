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
trait BlueprintService extends ClusterService {

  /**
   * Posts the blueprint JSON to Ambari with name 'bp' in the URL
   * because it does not matter here. The blueprint's name is
   * provided in the JSON.
   *
   * @param blueprint json
   * @return response message
   */
  private void postBlueprint(String blueprint, Boolean topologyValidation = true) throws Exception {
    if (debugEnabled) {
      println "[DEBUG] POST ${ambari.getUri()}blueprints/bp"
    }
    if (topologyValidation == null || topologyValidation) {
      ambari.post(path: 'blueprints/bp', body: blueprint, { it })
    } else {
      ambari.post(path: 'blueprints/bp', 'query': ["validate_topology": "false"], body: blueprint, { it })
    }
  }

  /**
   * Returns the blueprint json as String.
   *
   * @param id id of the blueprint
   * @return json as String, exception if thrown is it fails
   */
  def String getBlueprintAsJson(id) {
    Map resourceRequestMap = utils.getResourceRequestMap("blueprints/$id", ['fields': 'host_groups,Blueprints'])
    return utils.getRawResource(resourceRequestMap)
  }

  /**
   * Checks whether the blueprint exists or not.
   *
   * @param id id of the blueprint
   * @return true if exists false otherwise
   */
  def boolean doesBlueprintExist(String id) {
    def result = false
    try {
      def Map resourceRequest = utils.getResourceRequestMap("blueprints/$id", ['fields': 'Blueprints'])
      def jsonResponse = utils.getSlurpedResource(resourceRequest)
      result = !(jsonResponse.status)
    } catch (e) {
      log.info('Blueprint does not exist', e)
    }
    return result
  }

  /**
   * Checks whether there are available blueprints or not.
   *
   * @return true if blueprints are available false otherwise
   */
  def boolean isBlueprintAvailable() {
    return utils.getBlueprints().items?.size > 0
  }

  /**
   * Returns a pre-formatted String of the blueprint.
   *
   * @param id id of the blueprint
   * @return formatted String
   */
  def String showBlueprint(String id) {
    def resp = utils.getBlueprint(id)
    if (resp) {
      def groups = resp.host_groups.collect {
        def name = it.name
        def comps = it.components.collect { it.name.padRight(PAD).padLeft(PAD + 10) }.join('\n')
        return "HOSTGROUP: $name\n$comps"
      }.join('\n')
      return "[${resp.Blueprints.stack_name}:${resp.Blueprints.stack_version}]\n$groups"
    }
    return 'Not found'
  }

  /**
   * Returns the host group - components mapping of the blueprint.
   *
   * @param id id of the blueprint
   * @return Map where the key is the host group and the value is the list of components
   */
  def Map<String, List<String>> getBlueprintMap(String id) {
    def result = utils.getBlueprint(id)?.host_groups?.collectEntries { [(it.name): it.components.collect { it.name }] }
    result ?: [:]
  }

  /**
   * Returns a pre-formatted String of the blueprints.
   *
   * @return formatted blueprint list
   */
  def String showBlueprints() {
    utils.getBlueprints().items.collect {
      "${it.Blueprints.blueprint_name.padRight(PAD)} [${it.Blueprints.stack_name}:${it.Blueprints.stack_version}]"
    }.join('\n')
  }

  /**
   * Returns a Map containing the blueprint name - stack association.
   *
   * @return Map where the key is the blueprint's name value is the used stack
   */
  def Map<String, String> getBlueprintsMap() {
    def result = utils.getBlueprints().items?.collectEntries {
      [(it.Blueprints.blueprint_name): it.Blueprints.stack_name + ':' + it.Blueprints.stack_version]
    }
    result ?: new HashMap()
  }

  /**
   * Returns the name of the host groups for a given blueprint.
   *
   * @param blueprint id of the blueprint
   * @return host group list or empty list
   */
  def List<String> getHostGroups(String blueprint) {
    def result = utils.getBlueprint(blueprint)
    result ? result.host_groups.collect { it.name } : new ArrayList<String>()
  }

  /**
   * Returns a pre-formatted String of a blueprint used by the cluster.
   *
   * @return formatted String
   */
  def String showClusterBlueprint() throws Exception {
    ambari.get(path: "clusters/${getClusterName()}", query: ['format': 'blueprint']).data.text
  }

  /**
   * Adds a blueprint to the Ambari server. Exception is thrown if fails.
   *
   * @param json blueprint as json
   * @throws groovyx.net.http.HttpResponseException in case of error
   */
  def String addBlueprint(String json, Boolean topologyValidation=true) throws HttpResponseException {
    postBlueprint(json, topologyValidation)
  }

  /**
   * Extends the given blueprint with global configurations.
   *
   * @param blueprintJson original blueprint JSON as String
   * @param newConfigs global configuration map - <site_config_name <config_name, config_value>>
   * @return extended blueprints JSON as String
   */
  @Deprecated
  def String extendBlueprintGlobalConfiguration(String blueprintJson, Map<String, Map<String, String>> newConfigs, boolean forced = false) {
    def blueprintMap = slurper.parseText(blueprintJson)
    def configurations = blueprintMap.configurations
    if (!configurations) {
      if (newConfigs) {
        def conf = []
        newConfigs.each { conf << [(it.key): ['properties': it.value]] }
        blueprintMap << ['configurations': conf]
      }
    } else {
      newConfigs.each {
        def site = it.key
        def index = utils.indexOfConfig(configurations, site)
        if (index == -1) {
          configurations << ["$site": ['properties': it.value]]
        } else {
          def existingConf = configurations.get(index)
          def sitemap = existingConf."$site".properties == null ? existingConf."$site" : existingConf."$site".properties
          if (forced) {
            sitemap.putAll(it.value)
          } else {
            it.value.each { sitemap.putIfAbsent(it.key, it.value) }
          }
        }
      }
    }
    new JsonBuilder(blueprintMap).toPrettyString()
  }

  /**
   * Extends the given blueprint with host group level configurations.
   *
   * @param blueprintJson original blueprint JSON as String
   * @param newConfigs host group level configurations - <host_group_name <site_config_name <config_name, config_value>>>
   * @return extended blueprints JSON as String
   */
  @Deprecated
  def String extendBlueprintHostGroupConfiguration(String blueprintJson, Map<String, Map<String, Map<String, String>>> newConfigs, boolean forced = false) {
    def blueprintMap = slurper.parseText(blueprintJson)
    def globalConfigs = blueprintMap.configurations ?: []
    def filteredNewConfigs
    if (forced) {
      filteredNewConfigs = newConfigs
    } else {
        filteredNewConfigs = [:]
        newConfigs.each { hostgroup, hostgroupConfig ->
            def filteredHostgroupConfigs = [:]
            filteredNewConfigs.put(hostgroup, filteredHostgroupConfigs)
            hostgroupConfig.each { hostgroupSite, siteMap ->
                def filteredSiteMap = [:]
                filteredHostgroupConfigs.put(hostgroupSite, filteredSiteMap)
                def index = utils.indexOfConfig(globalConfigs, hostgroupSite)
                def globalSiteMap = index != -1 ? globalConfigs.get(index)."$hostgroupSite" : [:]
                if (globalSiteMap.properties) {
                    globalSiteMap = globalSiteMap.properties
                }
                filteredSiteMap.putAll(siteMap.findAll { !globalSiteMap.containsKey(it.key) })
            }
        }
    }
    for (int j = 0; j < filteredNewConfigs.size(); j++) {
      def configurations = blueprintMap.host_groups.find { it.name == filteredNewConfigs.keySet().getAt(j) }.configurations
      if (!configurations) {
        if (filteredNewConfigs) {
          def conf = filteredNewConfigs.get(filteredNewConfigs.keySet().getAt(j)).collect { [(it.key): it.value] }
          blueprintMap.host_groups.find { it.name == filteredNewConfigs.keySet().getAt(j) } << ['configurations': conf]
        }
      } else {
        filteredNewConfigs.get(filteredNewConfigs.keySet().getAt(j)).each { site, values ->
          def index = utils.indexOfConfig(configurations, site)
          if (index == -1) {
            configurations << ["$site": values]
          } else {
            def existingConf = configurations.get(index)
            def sitemap = existingConf."$site".properties == null ? existingConf."$site" : existingConf."$site".properties
            if (forced) {
              sitemap.putAll(values)
            } else {
              values.each { sitemap.putIfAbsent(it.key, it.value) }
            }
          }
        }
      }
    }
    new JsonBuilder(blueprintMap).toPrettyString()
  }

  /**
   * Adds the default blueprints.
   *
   * @throws HttpResponseException in case of error
   */
  def void addDefaultBlueprints() throws HttpResponseException {
    addBlueprint(utils.getResourceContent('blueprints/multi-node-hdfs-yarn'))
    addBlueprint(utils.getResourceContent('blueprints/single-node-hdfs-yarn'))
    addBlueprint(utils.getResourceContent('blueprints/lambda-architecture'))
    addBlueprint(utils.getResourceContent('blueprints/hdp-singlenode-default'))
    addBlueprint(utils.getResourceContent('blueprints/hdp-multinode-default'))
  }

  /**
   * Add hosts to the cluster and install all the services defined in the blueprint's host group.
   *
   * @param bpName name of the blueprint
   * @param hostGroup which host group to add the hosts to
   * @param hosts list of hosts in form of FQDN
   */
  def int addHostsWithBlueprint(String bpName, String hostGroup, List<String> hosts) throws Exception {
    def hostMap = hosts.collect { ["blueprint": bpName, "host_group": hostGroup, "host_name": it] }
    def json = new JsonBuilder(hostMap).toPrettyString()
    ambari.post(path: "clusters/${getClusterName()}/hosts", body: json, {
      utils.getRequestId(it)
    })
  }

  /**
   * Add hosts with rack infos to the cluster and install all the services defined in the blueprint's host group.
   *
   * @param bpName name of the blueprint
   * @param hostGroup which host group to add the hosts to
   * @param hostsWithRackInfo list of hosts in form of FQDN with rack info
   */
  def int addHostsAndRackInfoWithBlueprint(String bpName, String hostGroup, Map<String, String> hostsWithRackInfo) throws Exception {
    def hostMap = hostsWithRackInfo.collect { ["blueprint": bpName, "host_group": hostGroup, "host_name": it.key, "Hosts/rack_info": it.value] }
    def json = new JsonBuilder(hostMap).toPrettyString()
    ambari.post(path: "clusters/${getClusterName()}/hosts", body: json, {
      utils.getRequestId(it)
    })
  }

  /**
   * Update rack information for host
   *
   * @param host
   * @param rackInfo
   * @throws Exception
   */
  def void updateRack(String host, String rackInfo) throws Exception {
    def request = [
            RequestInfo: [
                    context: "Set Rack",
                    query: "Hosts/host_name.in(" + host + ")"
            ],
            Body: [
                    Hosts: [
                            rack_info: rackInfo
                    ]
            ]
    ]
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/${getClusterName()}/hosts")
    putRequestMap.put('body', new JsonBuilder(request).toPrettyString());
    ambari.put(putRequestMap)
  }
}
