package org.apache.cassandra.stress.operations.userdefined;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.ParameterMetaData;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.system_add_column_family;

public class NamedParameterMetaDataProxy implements InvocationHandler {

	private Map<Integer, String> names;
	private ParameterMetaData metaData;

	NamedParameterMetaDataProxy(Map<Integer, String> names,
			ParameterMetaData metaData) {
		this.names = names;
		this.metaData = metaData;
	}

	public String getParameterName(int index) {
		return names.get(index + 1);
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		try {
			return NamedParameterMetaDataProxy.class.getDeclaredMethod(
					method.getName(), method.getParameterTypes()).invoke(this,
					args);
		} catch (NoSuchMethodException e) {
			return ParameterMetaData.class.getDeclaredMethod(method.getName(),
					method.getParameterTypes()).invoke(metaData, args);
		}
	}
}
