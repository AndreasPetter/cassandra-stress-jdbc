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
import java.sql.SQLException;
import java.util.List;

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.transport.SimpleClient;

public abstract class JdbcSchemaStatement extends Operation
{

    final NamedParameterStatement statement;
    final int[] argumentIndex;
    final Object[] bindBuffer;

    public JdbcSchemaStatement(Timer timer, StressSettings settings, DataSpec spec,
    		NamedParameterStatement statement) throws SQLException
    {
        super(timer, settings, spec);
        this.statement = statement;
        argumentIndex = new int[statement.getNamedParameters().size()];
        bindBuffer = new Object[argumentIndex.length];
        List<String> names = statement.getNamedParameters();
        int i = 0;
        for (String name : names)
            argumentIndex[i++] = spec.partitionGenerator.indexOf(name);

    }

    @Override
    public void run(SimpleClient client) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    abstract class Runner implements RunOp
    {
        int partitionCount;
        int rowCount;

        @Override
        public int partitionCount()
        {
            return partitionCount;
        }

        @Override
        public int rowCount()
        {
            return rowCount;
        }
    }

}
