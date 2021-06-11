public class SQLTerm {

	String _strTableName;
	String _strColumnName;
	String _strOperator;
	Object _objValue;

	public SQLTerm() {
		_strTableName = null;
		_strColumnName = null;
		_strOperator = null;
		_objValue = null;
	}

	public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue)
			throws DBAppException {
		strOperator = strOperator.trim();
		if (!(strOperator.equals(">") || strOperator.equals(">=") || strOperator.equals("<") || strOperator.equals("<=")
				|| strOperator.equals("!=") || strOperator.equals("="))) {
			throw new DBAppException("Invalid Operator");
		}
		if (!(objValue.getClass().equals("java.lang.Integer") || objValue.getClass().equals("java.lang.String")
				|| objValue.getClass().equals("java.lang.Double") || objValue.getClass().equals("java.lang.Boolean")
				|| objValue.getClass().equals("java.util.Date"))) {
			throw new DBAppException("Invalid Data Type");
		}

		this._strTableName = strTableName;
		this._strColumnName = strColumnName;
		this._strOperator = strOperator;
		this._objValue = objValue;
	}
}