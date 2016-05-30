package org.apache.cassandra.stress.operations.userdefined;

import java.sql.ParameterMetaData;

public interface NamedParameterMetaData extends ParameterMetaData {
	public String getParameterName(int index);
}
