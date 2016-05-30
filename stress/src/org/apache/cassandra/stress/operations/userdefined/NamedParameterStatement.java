package org.apache.cassandra.stress.operations.userdefined;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;

public class NamedParameterStatement {

	private PreparedStatement statement = null;

	private final String parsedQuery;

	private final Map<String, List<Integer>> indexes;

	private Map<Integer, String> names;

	private String query;

	public NamedParameterStatement(String query) throws SQLException {
		indexes = new HashMap<String, List<Integer>>();
		names = new HashMap<Integer, String>();
		this.query = query;
		parsedQuery = parseQuery(query, indexes, names);
	}

	public String getQueryString() {
		return query;
	}

	public void prepareStatement(Connection connection) throws SQLException {
		statement = connection.prepareStatement(parsedQuery);
	}

	private static final String parseQuery(String query,
			Map<String, List<Integer>> paramMap, Map<Integer, String> names) {
		int length = query.length();
		StringBuffer parsedQuery = new StringBuffer(length);
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;
		int index = 1;

		for (int i = 0; i < length; i++) {
			char c = query.charAt(i);
			if (inSingleQuote) {
				if (c == '\'') {
					inSingleQuote = false;
				}
			} else if (inDoubleQuote) {
				if (c == '"') {
					inDoubleQuote = false;
				}
			} else {
				if (c == '\'') {
					inSingleQuote = true;
				} else if (c == '"') {
					inDoubleQuote = true;
				} else if (c == ':' && i + 1 < length
						&& Character.isJavaIdentifierStart(query.charAt(i + 1))) {
					int j = i + 2;
					while (j < length
							&& Character.isJavaIdentifierPart(query.charAt(j))) {
						j++;
					}
					String name = query.substring(i + 1, j);
					c = '?';
					i += name.length();

					List<Integer> indexList = (List<Integer>) paramMap
							.get(name);
					if (indexList == null) {
						indexList = new ArrayList<Integer>();
						paramMap.put(name, indexList);
					}
					indexList.add(new Integer(index));

					names.put(new Integer(index), name);

					index++;
				}
			}
			parsedQuery.append(c);
		}
		return parsedQuery.toString();
	}

	public List<String> getNamedParameters() {
		Integer[] indices = names.keySet().toArray(new Integer[0]);
		Arrays.sort(indices);
		List<String> result = new ArrayList<String>(indices.length);
		for (Integer index : indices) {
			result.add(names.get(index));
		}
		return result;
	}

	private void showError() throws SQLException {
		SQLException sqlException = new SQLException("Need to prepare statement before");
		sqlException.printStackTrace();
		throw sqlException;
	}
	
	public NamedParameterMetaData getParameterMetaData() throws SQLException {
		if (statement == null)
			showError();
		ParameterMetaData pm = statement.getParameterMetaData();
		InvocationHandler handler = new NamedParameterMetaDataProxy(names, pm);
		NamedParameterMetaData proxy = (NamedParameterMetaData) Proxy
				.newProxyInstance(
						NamedParameterMetaData.class.getClassLoader(),
						new Class[] { NamedParameterMetaData.class }, handler);
		return proxy;
	}

	private List<Integer> getIndexes(String name) {
		List<Integer> indices = indexes.get(name);
		if (indices == null) {
			throw new IllegalArgumentException("Parameter not found: " + name);
		}
		return indices;
	}

	class FuncParam<T> {
		private int index;
		private T value;

		public FuncParam(int index, T value) {
			this.index = index;
			this.value = value;
		}

		public int getIndex() {
			return index;
		}

		public T getValue() {
			return value;
		}
	}

	abstract class SetAnyFunc<T> implements Function<FuncParam<T>, Void> {
		protected abstract void execute(int index, T value) throws SQLException;

		@Override
		public Void apply(FuncParam<T> v) {
			try {
				execute(v.getIndex(), v.getValue());
			} catch (SQLException sqlE) {
				throw new RuntimeException(sqlE);
			}
			return null;
		}
	}

	private SetAnyFunc<String> setStringFunc = new SetAnyFunc<String>() {
		@Override
		protected void execute(int index, String value) throws SQLException {
			statement.setString(index, value);
		}
	};

	private SetAnyFunc<Integer> setIntFunc = new SetAnyFunc<Integer>() {
		@Override
		protected void execute(int index, Integer value) throws SQLException {
			statement.setInt(index, value);
		}
	};

	private SetAnyFunc<Long> setLongFunc = new SetAnyFunc<Long>() {
		@Override
		protected void execute(int index, Long value) throws SQLException {
			statement.setLong(index, value);
		}
	};

	private SetAnyFunc<Object> setObjectFunc = new SetAnyFunc<Object>() {
		@Override
		protected void execute(int index, Object value) throws SQLException {
			statement.setObject(index, value);
		}
	};

	private SetAnyFunc<Timestamp> setTimestampFunc = new SetAnyFunc<Timestamp>() {
		@Override
		protected void execute(int index, Timestamp value) throws SQLException {
			statement.setTimestamp(index, value);
		}
	};

	private <T> void setAnything(String name, T value, SetAnyFunc<T> function)
			throws SQLException {
		if (statement == null)
			showError();
		Iterator<Integer> indexes = getIndexes(name).iterator();
		while (indexes.hasNext()) {
			function.apply(new FuncParam<T>(indexes.next(), value));
		}
	}

	public void setObject(String name, Object value) throws SQLException {
		setAnything(name, value, setObjectFunc);
	}

	public void setString(String name, String value) throws SQLException {
		setAnything(name, value, setStringFunc);
	}

	public void setInt(String name, int value) throws SQLException {
		setAnything(name, value, setIntFunc);
	}

	public void setLong(String name, long value) throws SQLException {
		setAnything(name, value, setLongFunc);
	}

	public void setTimestamp(String name, Timestamp value) throws SQLException {
		setAnything(name, value, setTimestampFunc);
	}

	public void addBatch() throws SQLException {
		if (statement == null)
			showError();
		statement.addBatch();
	}

	public boolean execute() throws SQLException {
		try {
			return statement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public ResultSet executeQuery() throws SQLException {
		if (statement == null)
			showError();
		try {
			return statement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public int executeUpdate() throws SQLException {
		if (statement == null)
			showError();
		try {
			return statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public int[] executeBatch() throws SQLException {
		if (statement == null)
			showError();
		try {
			return statement.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public PreparedStatement getOriginalStatement() {
		return statement;
	}

	public void close() throws SQLException {
		if (statement == null)
			showError();
		statement.close();
	}

	public void bind(Object[] values) throws SQLException {
		if (statement == null)
			showError();
		ParameterMetaData metaData = statement.getParameterMetaData();
		for (int i = 1; i <= metaData.getParameterCount(); i++) {
			statement.setObject(i, values[i - 1]);
		}
	}
}
