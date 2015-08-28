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
trait KerberosService extends ClusterService {

  /**
   * Create a default kerberos config (same as with the wizzard)
   *
   * @param kdcHost key distribution center's host
   * @param realm kerberos realm
   * @param domain kerberos domain
   */
  def void createKerberosConfig(String kdcHost, String realm, String domain) {
    def model = [
      'KDC_HOST'     : kdcHost,
      'REALM'        : realm,
      'DOMAIN'       : domain,
      'TAG'          : "version${System.currentTimeMillis()}",
      'ATTR_TEMPLATE': utils.getResourceContent('templates/kerberos-attr-template'),
      'CONTENT'      : utils.getResourceContent('templates/kerberos-content-template'),]
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/${getClusterName()}")
    putRequestMap.put('body', utils.createJson('kerberos-config.json', model));
    ambari.put(putRequestMap)
  }

  /**
   * Creates a default kerberos descriptor for the different Hadoop services.
   *
   * @param realm kerberos realm
   */
  def void createKerberosDescriptor(String realm) {
    def json = utils.getResourceContent('templates/kerberos-descriptor').replaceAll('actual-realm', realm)
    ambari.post(path: "clusters/${getClusterName()}/artifacts/kerberos_descriptor", body: json, { it })
  }

  /**
   * In order to add/remove or change any service or host related config in a kerberos secure cluster you need
   * to set the session attributes which contains an admin principal.
   *
   * @param principal admin principal
   * @param password password for the admin principal
   */
  def void setKerberosSession(String principal, String password) {
    def session = ['session_attributes': ['kerberos_admin': ['principal': principal, 'password': password]]]
    def Map<String, ?> kdcPut = [:]
    kdcPut.put('requestContentType', ContentType.URLENC)
    kdcPut.put('path', "clusters/${getClusterName()}")
    kdcPut.put('body', new JsonBuilder(session).toPrettyString());
    ambari.put(kdcPut)
  }

  /**
   * Enables kerberos security on the cluster. It will generate the necessary keytabs using the previously
   * provided krb5-conf.
   *
   * @return id of the request
   */
  def int enableKerberos() {
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/${getClusterName()}")
    putRequestMap.put('body', new JsonBuilder(['Clusters': ['security_type': 'KERBEROS']]).toPrettyString())

    utils.putAndGetId(putRequestMap)
  }

  /**
   * Generate the keytabs.
   *
   * @param missingOnly if set to true, keytabs will be generated only for missing hosts and services
   * @return id of the request
   */
  def int generateKeytabs(boolean missingOnly) {
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/${getClusterName()}")
    putRequestMap.put('body', new JsonBuilder(['Clusters': ['security_type': 'KERBEROS']]).toPrettyString())
    putRequestMap.put('query', ['regenerate_keytabs': missingOnly ? 'missing' : 'all'])

    utils.putAndGetId(putRequestMap)
  }
}
