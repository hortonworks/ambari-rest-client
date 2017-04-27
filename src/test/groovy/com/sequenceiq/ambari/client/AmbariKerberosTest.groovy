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

class AmbariKerberosTest extends AbstractAmbariClientTest {

  def slurper = new JsonSlurper()

  def "test create kerberos config"() {
    given:
    def context
    ambari.getClusterName() >> "cluster"
    ambari.getAmbari().metaClass.put = { Map request -> context = request }

    when:
    ambari.createKerberosConfig('host', 'realm', 'domain')

    then:
    def body = slurper.parseText(context.body)
    'host' == body?.Clusters?.desired_config?.get(0)?.properties?.kdc_hosts
    'realm' == body?.Clusters?.desired_config?.get(0)?.properties?.realm
    'domain' == body?.Clusters?.desired_config?.get(1)?.properties?.domains
    true == body?.Clusters?.desired_config?.get(1)?.properties?.manage_krb5_conf
  }

  def "test set kerberos session"() {
    given:
    def context
    ambari.getClusterName() >> "cluster"
    ambari.getAmbari().metaClass.put = { Map request -> context = request }

    when:
    ambari.setKerberosSession('princ', 'passwd')

    then:
    def body = slurper.parseText(context.body)
    'princ' == body?.session_attributes?.kerberos_admin?.principal
    'passwd' == body?.session_attributes?.kerberos_admin?.password
  }

  def "test create kerberos decriptor"() {
    given:
    def context
    ambari.getClusterName() >> "cluster"
    ambari.getAmbari().metaClass.post = { Map request, Closure c -> context = request }

    when:
    ambari.createKerberosDescriptor('realm')

    then:
    def body = slurper.parseText(context.body)
    'realm' == body?.artifact_data?.properties?.realm
    'KERBEROS' == body?.artifact_data?.services?.find { it.name == 'KERBEROS' }?.name
  }

  def "test enable cluster"() {
    given:
    def context
    ambari.getClusterName() >> "cluster"
    ambari.getAmbari().metaClass.put = { Map request -> context = request }
    ambari.getSlurper().metaClass.parseText { String text -> return ["Requests": ["id": 1]] }

    when:
    ambari.enableKerberos()

    then:
    def body = slurper.parseText(context.body)
    'KERBEROS' == body?.Clusters?.security_type
  }

  def "test generate missing keytabs"() {
    given:
    def context
    ambari.getClusterName() >> "cluster"
    ambari.getAmbari().metaClass.put = { Map request -> context = request }
    ambari.getSlurper().metaClass.parseText { String text -> return ["Requests": ["id": 1]] }

    when:
    ambari.generateKeytabs(true)

    then:
    def body = slurper.parseText(context.body)
    'KERBEROS' == body?.Clusters?.security_type
    'missing' == context?.query?.regenerate_keytabs
  }

  def "test generate keytabs"() {
    given:
    def context
    ambari.getClusterName() >> "cluster"
    ambari.getAmbari().metaClass.put = { Map request -> context = request }
    ambari.getSlurper().metaClass.parseText { String text -> return ["Requests": ["id": 1]] }

    when:
    ambari.generateKeytabs(false)

    then:
    def body = slurper.parseText(context.body)
    'KERBEROS' == body?.Clusters?.security_type
    'all' == context?.query?.regenerate_keytabs
  }

  def "test add kerberos config to clear blueprint"() {
    given:
    def json = getClass().getClassLoader().getResourceAsStream("multi-node-hdfs-yarn.json").text

    when:
    def blueprint = ambari.extendBlueprintWithKerberos(json, "mit-kdc", "hostname.node.dc1.consul", "NODE.DC1.CONSUL", "node.dc1.consul,node.consul", null, null, true, null)

    then:
    def expected = slurper.parseText(getClass().getClassLoader().getResourceAsStream("multi-node-hdfs-yarn-kerb.json").text)
    def actual = slurper.parseText(blueprint)
    actual == expected
  }

  def "test add kerberos config to kerberos configured blueprint"() {
    given:
    def json = getClass().getClassLoader().getResourceAsStream("multi-node-hdfs-yarn-default-kerb.json").text

    when:
    def blueprint = ambari.extendBlueprintWithKerberos(json, "mit-kdc", "hostname.node.dc1.consul", "NODE.DC1.CONSUL", "node.dc1.consul,node.consul", null, null, true, null)

    then:
    def expected = slurper.parseText(getClass().getClassLoader().getResourceAsStream("multi-node-hdfs-yarn-kerb.json").text)
    def actual = slurper.parseText(blueprint)
    actual == expected
  }

  def "test add kerberos config to kerberos configured blueprint with descriptor"() {
    given:
    def json = getClass().getClassLoader().getResourceAsStream("multi-node-hdfs-yarn-default-kerb_descriptor.json").text

    when:
    def blueprint = ambari.extendBlueprintWithKerberos(json, "mit-kdc", "hostname.node.dc1.consul", "NODE.DC1.CONSUL", "node.dc1.consul,node.consul", null, null, false, null)

    then:
    def expected = slurper.parseText(getClass().getClassLoader().getResourceAsStream("multi-node-hdfs-yarn-default-kerb_descriptor_fixed.json").text)
    def actual = slurper.parseText(blueprint)
    actual == expected
  }

  def protected String selectResponseJson(Map resourceRequestMap, String scenarioStr) {
  }

}
