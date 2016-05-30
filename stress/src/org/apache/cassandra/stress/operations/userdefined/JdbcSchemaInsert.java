package org.apache.cassandra.stress.operations.userdefined;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.cassandra.stress.generate.Distribution;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.PartitionIterator;
import org.apache.cassandra.stress.generate.RatioDistribution;
import org.apache.cassandra.stress.generate.Row;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.JdbcDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;

public class JdbcSchemaInsert extends JdbcSchemaStatement
{
	
	private final SeedManager seedManager;
	
    public JdbcSchemaInsert(Timer timer, StressSettings settings, PartitionGenerator generator, SeedManager seedManager, Distribution batchSize, RatioDistribution useRatio, JdbcDriverClient client, String query) throws SQLException
    {
        super(timer, settings, new DataSpec(generator, seedManager, batchSize, useRatio), new NamedParameterStatement(query));
        this.seedManager = seedManager; 
    }


    private void bindRow(Row row) throws SQLException {
    	for (int i = 0 ; i < argumentIndex.length ; i++)
        {
            bindBuffer[i] = row.get(argumentIndex[i]);
            if (bindBuffer[i] == null && !spec.partitionGenerator.permitNulls(argumentIndex[i]))
                throw new IllegalStateException();
        }
    	statement.bind(bindBuffer);
    }
    
    
    private class JdbcRun extends Runner
    {
        final JdbcDriverClient client;

        private JdbcRun(JdbcDriverClient client)
        {
            this.client = client;
        }

        public boolean run() throws Exception
        {
            partitionCount = partitions.size();
            int count = 0;
            boolean didBatch = false;
            Connection connection = client.getConnection();
            try {
            	statement.prepareStatement(connection);
	            for (PartitionIterator iterator : partitions)
	                while (iterator.hasNext()) {
	                	if((count > 0) && (count % 100 == 0)) {
	                		count = 0;
	                		statement.executeBatch();
	                		statement.getOriginalStatement().clearBatch();
	                	}
	                	if(count > 0) {
	                		didBatch = true;
	                		statement.addBatch();
	                	}
	                	bindRow(iterator.next());
	                	seedManager.next(JdbcSchemaInsert.this);
	                	count += 1;
	                }
	            if(count > 0) {
	            	if(didBatch) {
	            		statement.executeBatch();
	            	} else {
	            		statement.execute();
	            	}
	            }
            } finally {
            	connection.close();
            }
            
            rowCount += count;

            return true;
        }    	
    }
    
    @Override
    public void run(JavaDriverClient client) throws IOException
    {
    	throw new IOException(new UnsupportedOperationException());
    }

    public boolean isWrite()
    {
        return true;
    }

    @Override
    public void run(ThriftClient client) throws IOException
    {
    	throw new IOException(new UnsupportedOperationException());
    }

    @Override
    public void run(JdbcDriverClient client) throws IOException
    {
        timeWithRetry(new JdbcRun(client));
    }
}
