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

import groovy.util.logging.Slf4j

@Slf4j
trait AlertService extends ClusterService {

  /**
   * Creates a new alert definition.
   *
   * @param definition alert definition as json
   */
  def void createAlert(String definition) {
    ambari.post(path: "clusters/${getClusterName()}/alert_definitions", body: definition, { it })
  }

  /**
   * Returns the alert definitions.
   *
   * @return collection of definitions
   */
  def List<Map<String, String>> getAlertDefinitions() {
    utils.getAllResources('alert_definitions', 'AlertDefinition').items.collect {
      def details = [:]
      def definition = it.AlertDefinition
      details << ['enabled': definition.enabled]
      details << ['scope': definition.scope]
      details << ['interval': definition.interval as String]
      details << ['description': definition.description]
      details << ['name': definition.name]
      details << ['label': definition.label]
      details << ['service_name': definition.service_name]
      details
    }
  }

  def Map<String, List<Map<String, Object>>> getAlerts() {
    return getAlerts(Collections.emptyList())
  }

  /**
   * Returns all the defined alerts grouped by alert definition name.
   * A filter can be applied for scopes: HOST, ANY, SERVICE
   *
   * @return alert properties
   */
  def Map<String, List<Map<String, Object>>> getAlerts(List<String> scope) {
    utils.getAllResources('alerts', 'Alert').items.findAll {
      scope == [] || scope.contains(it.Alert.scope)
    }.collect { it.Alert }.groupBy { it.definition_name }
  }

  /**
   * Get a specified alert by id.
   *
   * @param id id of the alert
   * @return alert or empty collection
   */
  Map<String, Object> getAlert(int id) {
    def response = utils.getAllResources("alerts/$id")
    response ? response.Alert : [:]
  }

  /**
   * Get a specified alert by name. Can return multiple
   * values if the alert is HOST based.
   *
   * @param definitionName alert definition name
   * @return list of alerts or empty collection
   */
  List<Map<String, Object>> getAlert(String definitionName) {
    getAlerts()[definitionName] ?: []
  }

  /**
   * Get a specified alert history by id.
   *
   * @param id id of the alert history
   * @return history or empty collection
   */
  Map<String, Object> getAlertHistory(int id) {
    def response = utils.getAllResources("alert_history/$id")
    response ? response.AlertHistory : [:]
  }

  /**
   * Returns the history of a certain alert.
   *
   * @param alertDefinition alert definition name
   * @param count desired number of result from the latest one
   * @return list of alert properties or empty collection
   */
  def List<Map<String, Object>> getAlertHistory(String alertDefinition, int count) {
    def result = []
    def prePredicate = ['AlertHistory/definition_name': alertDefinition]
    def items = utils.getAllPredictedResources('alert_history', prePredicate).items
    if (items) {
      def itemSize = items.size
      def from = itemSize - count > -1 ? itemSize - count : 0
      from = from == itemSize ? itemSize - 1 : from
      items[from..itemSize - 1].collect { it.AlertHistory.id }.each {
        result << getAlertHistory(it)
      }
    }
    result
  }
}