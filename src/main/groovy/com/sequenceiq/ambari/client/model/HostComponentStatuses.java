package com.sequenceiq.ambari.client.model;

public class HostComponentStatuses {
    private String desired_admin_state;

    private String desired_state;

    private String maintenance_state;

    private String state;

    private String upgrade_state;

    public String getDesired_admin_state() {
        return desired_admin_state;
    }

    public void setDesired_admin_state(String desired_admin_state) {
        this.desired_admin_state = desired_admin_state;
    }

    public String getDesired_state() {
        return desired_state;
    }

    public void setDesired_state(String desired_state) {
        this.desired_state = desired_state;
    }

    public String getMaintenance_state() {
        return maintenance_state;
    }

    public void setMaintenance_state(String maintenance_state) {
        this.maintenance_state = maintenance_state;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getUpgrade_state() {
        return upgrade_state;
    }

    public void setUpgrade_state(String upgrade_state) {
        this.upgrade_state = upgrade_state;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HostComponentStatuses{");
        sb.append("desired_admin_state='").append(desired_admin_state).append('\'');
        sb.append(", desired_state='").append(desired_state).append('\'');
        sb.append(", maintenance_state='").append(maintenance_state).append('\'');
        sb.append(", state='").append(state).append('\'');
        sb.append(", upgrade_state='").append(upgrade_state).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
