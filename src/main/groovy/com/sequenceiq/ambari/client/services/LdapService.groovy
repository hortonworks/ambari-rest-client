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
trait LdapService extends CommonService {

  /**
   * Configure ldap for ambari
   *
   * @param ldapProperties
   */
  def void configureLdap(Map ldapProperties) {
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "services/AMBARI/components/AMBARI_SERVER/configurations/ldap-configuration")

    Map bodyMap = [
            'Configuration': [category: 'ldap-configuration', properties: ldapProperties]
    ]

    putRequestMap.put('body', new JsonBuilder(bodyMap).toPrettyString())
    utils.putAndGetId(putRequestMap)
  }

  /**
   * Sync ldap
   */
  def void syncLdap() throws Exception {
    def body = new JsonBuilder([['Event': ['specs': [
            [
                    'principal_type': 'users',
                    'sync_type'     : 'all'
            ],
            [
                    'principal_type': 'groups',
                    'sync_type'     : 'all'
            ]
    ]]]]).toPrettyString()
    ambari.post(path: 'ldap_sync_events', body: body, { it })
  }

}