/*
 *  Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package io.ballerina.stdlib.sql.transaction;

import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.transactions.BallerinaTransactionContext;
import io.ballerina.stdlib.sql.datasource.SQLDatasource;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

/**
 * {@code SQLTransactionContext} transaction context for SQL transactions.
 *
 * @since 2.0.0
 */
public class SQLTransactionContext implements BallerinaTransactionContext {
    private Connection conn;
    private XAResource xaResource;
    private SQLDatasource dataSource;
    private XAConnection xaConnection;

    public SQLTransactionContext(Connection conn, XAResource resource) {
        this.conn = conn;
        this.xaResource = resource;
    }

    public SQLTransactionContext(Connection conn, XAResource resource, XAConnection xaConnection,
                                 SQLDatasource dataSource) {
        this.conn = conn;
        this.xaResource = resource;
        this.xaConnection = xaConnection;
        this.dataSource = dataSource;
    }

    public SQLTransactionContext(Connection conn) {
        this.conn = conn;
    }

    public Connection getConnection() {
        return this.conn;
    }

    @Override
    public void commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            throw ErrorCreator.createError(StringUtils.fromString("transaction commit failed:" + e.getMessage()));
        }
    }

    @Override
    public void rollback() {
        try {
            if (!conn.isClosed()) {
                conn.rollback();
            }
        } catch (SQLException e) {
            throw ErrorCreator.createError(StringUtils.fromString("transaction rollback failed:" + e.getMessage()));
        }
    }

    @Override
    public void close() {
        try {
            if (!conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            throw ErrorCreator.createError(StringUtils.fromString("connection close failed:" + e.getMessage()));
        }
    }

    @Override
    public XAResource getXAResource() {
        return this.xaResource;
    }

//    @Override
    public void refreshXAResource() {
        try{
            if (this.xaResource != null) {
                assertConnectionIsStillAlive();
            } else {
                forceRefreshXAConnection();
            }
        } catch (XAException e) {
            throw ErrorCreator.createError(StringUtils.fromString("XAException occurred:" + e.getMessage()));
        }
    }

    private void assertConnectionIsStillAlive() throws XAException {
        this.xaResource.isSameRM(this.xaResource);
    }

    private void forceRefreshXAConnection() throws XAException {
        XAResource xaResource = null;

        if ( this.xaConnection != null ) {
            try {
                this.xaConnection.close ();
            } catch ( Exception err ) {
                // connection timed out, ignore.
            }
        }

        try {
            this.xaConnection = this.dataSource.getXAConnection();
            if ( this.xaConnection != null )
                xaResource = this.xaConnection.getXAResource();
        } catch ( SQLException sql ) {
            System.out.println("Failed to refresh XAConnection: " + sql.getMessage());
        }

        this.xaResource = xaResource;
    }
}
