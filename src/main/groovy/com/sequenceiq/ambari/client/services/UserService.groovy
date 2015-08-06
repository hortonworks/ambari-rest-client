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
trait UserService extends ClusterService {

  /**
   * Create a new Ambari user.
   */
  def createUser(String user, String password, boolean admin) {
    def context = ['Users/active': true, 'Users/admin': admin, 'Users/password': password, 'Users/user_name': user]
    ambari.post(path: 'users', body: new JsonBuilder(context).toPrettyString(), { it })
  }

  /**
   * Delete an Ambari user.
   */
  def deleteUser(String user) {
    ambari.delete(path: "users/$user")
  }

  /**
   * Change the password of an Ambari user.
   */
  def changePassword(String user, String oldPassword, String newPassword, boolean admin) {
    def roles = ['user']
    if (admin) {
      roles << 'admin'
    }
    def context = ['Users/password': newPassword, 'Users/old_password': oldPassword]
    ambari.put(path: "users/$user", body: new JsonBuilder(context).toPrettyString(), requestContentType: ContentType.URLENC)
  }
}