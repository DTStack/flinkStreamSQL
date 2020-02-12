package com.dtstack.flink.sql.side.elasticsearch6.table;

import com.dtstack.flink.sql.side.SideTableInfo;
import com.google.common.base.Preconditions;

/**
 * @author yinxi
 * @date 2020/1/13 - 15:00
 */
public class Elasticsearch6SideTableInfo extends SideTableInfo {

    private static final String CURR_TYPE = "elasticsearch6";

    private String address;

    private String index;

    private String clusterName;

    private String esType;

    private boolean authMesh = false;

    private String userName;

    private String password;

    public String getEsType() {
        return esType;
    }

    public void setEsType(String esType) {
        this.esType = esType;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public String getType() {
        //return super.getType().toLowerCase() + TARGET_SUFFIX;
        return super.getType().toLowerCase();
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public boolean isAuthMesh() {
        return authMesh;
    }

    public void setAuthMesh(boolean authMesh) {
        this.authMesh = authMesh;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Elasticsearch6SideTableInfo() {
        setType(CURR_TYPE);
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(address, "elasticsearch6 type of address is required");
        Preconditions.checkNotNull(index, "elasticsearch6 type of index is required");
        Preconditions.checkNotNull(esType, "elasticsearch6 type of type is required");
        Preconditions.checkNotNull(clusterName, "elasticsearch6 type of clusterName is required");

        if (isAuthMesh()) {
            Preconditions.checkNotNull(userName, "elasticsearch6 type of userName is required");
            Preconditions.checkNotNull(password, "elasticsearch6 type of password is required");
        }

        return true;
    }
}
