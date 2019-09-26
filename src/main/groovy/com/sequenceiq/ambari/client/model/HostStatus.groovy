package com.sequenceiq.ambari.client.model

class HostStatus {
  private String hostStatus;

  private Map<String, HostComponentStatuses> hostComponentsStatuses;

  String getHostStatus() {
    return hostStatus
  }

  void setHostStatus(String hostStatus) {
    this.hostStatus = hostStatus
  }

  Map<String, HostComponentStatuses> getHostComponentsStatuses() {
    return hostComponentsStatuses
  }

  void setHostComponentsStatuses(Map<String, HostComponentStatuses> hostComponentsStatuses) {
    this.hostComponentsStatuses = hostComponentsStatuses
  }

  @Override
  String toString() {
    final StringBuilder sb = new StringBuilder("HostStatus{");
    sb.append("hostStatus='").append(hostStatus).append('\'');
    sb.append(", hostComponentsStatuses=").append(hostComponentsStatuses);
    sb.append('}');
    return sb.toString();
  }
}
