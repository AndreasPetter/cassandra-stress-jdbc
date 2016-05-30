package org.apache.cassandra.stress.settings;
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


import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsJDBC implements Serializable
{
	private static final long serialVersionUID = 1L;
	public String url;
	public String driver;
	public boolean upper;

    public SettingsJDBC(Options options)
    {
        url = options.url.value();
        driver = options.driver.value();
        upper = Boolean.valueOf(options.upper.value());
    }

    // Option Declarations

    public static final class Options extends GroupedOptions
    {
        final OptionSimple url = new OptionSimple("url=", "[a-zA-Z_0-9\\./:@]+", null, "JDBC URL", true);
        final OptionSimple driver = new OptionSimple("driver=", "[a-zA-Z_0-9\\./]+", null, "JDBC Driver class", true);
        final OptionSimple upper = new OptionSimple("upper=", "true|false", "false", "If the (default = false)", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(url, driver, upper);
        }
    }

    // CLI Utility Methods

    public static SettingsJDBC get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-jdbc");
        if (params == null)
            return new SettingsJDBC(new Options());

        GroupedOptions options = GroupedOptions.select(params, new Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -jdbc options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsJDBC((Options) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-jdbc", new Options());
    }

    public static Runnable helpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp();
            }
        };
    }
}
