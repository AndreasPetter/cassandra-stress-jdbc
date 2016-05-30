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
package org.apache.cassandra.stress.util;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.cassandra.stress.settings.StressSettings;

import com.zaxxer.hikari.HikariDataSource;

public class JdbcDriverClient
{

	/**
	 * Simple Java re-make of Scala Either type
	 */
	public static class Either<Left, Right> {
		private Left left = null;
		private Right right = null;
		public static <Left, Right> Either<Left, Right> left(Left left) {
			return new Either<Left, Right>(left, null);
		}
		public static <Left, Right> Either<Left, Right> right(Right right) {
			return new Either<Left, Right>(null, right);
		}
		public Either(Left left, Right right) {
			this.left = left;
			this.right = right;
		}
		public Left getLeft() {
			return left;
		}
		public Right getRight() {
			return right;
		}
		public boolean isLeft() {
			return left != null;
		}
		public boolean isRight() {
			return right != null;
		}
	}
	
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    public final String username;
    public final String password;
    public final String driver;
    public final String url;
    
    private static final Object syncObject = new Object();
    
    private static volatile HikariDataSource ds;
    
    public JdbcDriverClient(StressSettings settings)
    {
        this.username = settings.mode.username;
        this.password = settings.mode.password;
        this.driver = settings.jdbcURL.driver;
        this.url = settings.jdbcURL.url;
    }

    public void connect() throws Exception
    {
    	synchronized(syncObject) {
    		if(ds == null) {
    			ds = new HikariDataSource();
    			ds.setUsername(username);
    			ds.setPassword(password);
    			ds.setJdbcUrl(url);
    			ds.setAutoCommit(true);
    			ds.setMaximumPoolSize(150);
    			// force Hikari to initialize
    			ds.getConnection().close();
    		}
    	}
    }

    public Connection getConnection()
    {
        try {
			return ds.getConnection();
		} catch (SQLException e) {
			System.err.println("Cannot retrieve connection from pool " + e);
			return null;
		}
    }

    public Either<ResultSet, Integer> execute(String query)
    {
    	Connection conn = getConnection();
    	try {
    		PreparedStatement prep = conn.prepareStatement(query);
    		boolean resultType = prep.execute();
    		Either<ResultSet, Integer> result = null;
    		if(resultType) {
    			ResultSet rset = prep.getResultSet();
    			result = Either.left(rset);
    		} else {
    			result = Either.right(prep.getUpdateCount());
    		}
    		return result;
    	} catch (SQLException e) {
    		System.err.println("Could not execute prepared statement " + query + " because " + e);
		} finally {
    		try {
				conn.close();
			} catch (SQLException e) {
	    		System.err.println("Could not close connection for prepared statement " + query);
			}
    	}
    	return null;
    }

    public void disconnect()
    {
        ds.close();
    }
}
