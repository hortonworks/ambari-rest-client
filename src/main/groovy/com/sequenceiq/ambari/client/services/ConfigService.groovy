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

@Slf4j
trait ConfigService extends ClusterService {

  /**
   * In case of adding new nodes to the cluster these nodes should be added to the
   * appropriate config groups as well otherwise they'll use the default one. This method
   * adds the given hosts to the desired config groups for every config type like: yarn-site, hdfs-site etc..
   *
   * @param hosts hosts to be added to the groups
   * @param tag tag of the config group
   */
  def void addHostsToConfigGroups(List<String> hosts, String tag) {
    def groups = getConfigGroups(tag)
    if (groups) {
      groups.each {
        it.remove('href')
        def currentHosts = it.ConfigGroup.hosts
        hosts.each {
          currentHosts << ['host_name': it]
        }
        def Map<String, ?> putRequestMap = [:]
        putRequestMap.put('requestContentType', ContentType.URLENC)
        putRequestMap.put('path', "clusters/${getClusterName()}/config_groups/$it.ConfigGroup.id")
        putRequestMap.put('body', new JsonBuilder(it).toPrettyString());
        ambari.put(putRequestMap)
      }
    }
  }

  /**
   * Modify an existing configuration. Be ware you'll have to provide the whole configuration
   * otherwise properties might get lost.
   *
   * @param type type of the configuration e.g capacity-scheduler
   * @param properties properties to be used
   */
  def modifyConfiguration(String type, Map<String, String> properties) {
    Map bodyMap = [
            'Clusters': ['desired_config': ['type': type, 'tag': "version${System.currentTimeMillis()}", 'properties': properties]]
    ]
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/${getClusterName()}")
    putRequestMap.put('body', new JsonBuilder(bodyMap).toPrettyString());
    ambari.put(putRequestMap)
  }

  private List getConfigGroups(String tag) {
    def groups = utils.getAllResources('config_groups', 'ConfigGroup')
    groups.items.findAll {
      it.ConfigGroup.group_name.endsWith(tag)
    }
  }
}
