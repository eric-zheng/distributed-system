/*
 * Structure of table:
 * key=tablename+delim
 * value=colunmname1+delim+colunmname2+delim+delim+data1+delim+data2+delim....
 * */
package Table;

import java.util.*;

public class Table {
	private static final char delim = '`';
	private String tableName;
	private ArrayList<String> column;
	private ArrayList<ArrayList<String>> data;

	public Table() { // honestly should never be used
		tableName = null;
		column = null;
		data = null;
	}

	public Table(String key, String value) {
		// parse table name
		tableName = key.substring(0, key.length());

		column = new ArrayList<String>();
		// parse table column names
		int pos = 0;
		String word = "";
		while (true) {
			if (value.charAt(pos) != delim) {
				word = word + value.charAt(pos);
				pos++;
			} else {
				pos++;
				column.add(word);
				word = "";
				if (value.charAt(pos) == delim) {
					pos++;
					break;
				}
			}
		}

		// parse table data
		word = "";
		int columnNumber = 0;
		data = new ArrayList<ArrayList<String>>();
		for (String name : column) {
			data.add(new ArrayList<String>());
		}

		while (pos < value.length()) {
			if (value.charAt(pos) != delim) {
				word = word + value.charAt(pos);
				pos++;
			} else {
				pos++;
				data.get(columnNumber).add(word);
				columnNumber = (columnNumber + 1) % column.size();
				word = "";
			}
		}
	}

	public Table(String tableName, ArrayList<String> meta) { // new empty table
		this.tableName = tableName;
		column = new ArrayList<String>();
		for (String col : meta) {
			column.add(col);
		}
		data = new ArrayList<ArrayList<String>>();
		for (String name : column) {
			data.add(new ArrayList<String>());
		}
	}

	public void printTable() {
		System.out.println("tableName: " + tableName);

		for (int i = 0; i < column.size(); i++) {
			System.out.format("%15s", column.get(i));

		}
		System.out.println("");

		for (int i = 0; i < column.size(); i++) {
			System.out.print("---------------");
		}
		System.out.println("");

		for (int j = 0; j < data.get(0).size(); j++) {
			for (int i = 0; i < column.size(); i++) {
				System.out.format("%15s", data.get(i).get(j));
			}
			System.out.println("");
		}

	}

	public ArrayList<String> get(int index) { // get a row at index
		ArrayList<String> result = new ArrayList<String>();
		if (index >= data.get(0).size()) {
			System.out.println("Table.get: index out of bound");
			return null;
		}

		for (ArrayList<String> temp : data) {
			result.add(temp.get(index));
		}

		return result;
	}

	public boolean add(ArrayList<String> row) { // add a new row
		if (row.size() != column.size()) {
			System.out.println("Table.add: input and table dimension does not match");
			return false;
		}
		for (int i = 0; i < column.size(); i++) {
			data.get(i).add(row.get(i));
		}
		return true;
	}

	public boolean remove(int rowNum) { // remove the corresponding row
		if (rowNum >= data.get(0).size() || rowNum < 0) {
			System.out.println("Table.remove: row number " + rowNum + " not in range");
			return false;
		}

		for (int i = 0; i < column.size(); i++) {
			data.get(i).remove(rowNum);
		}
		return true;
	}

	public static String tableNameToKey(String name) {
		return name + delim;
	}

	public boolean filter(ArrayList<String> selectedCol, String whereStatement) {
		// filter out anything that is not in the criteria
		if (selectedCol == null || selectedCol.size() == 0) {
			return false;
		}

		if (!executeWhere(whereStatement)) {
			return false;
		}

		// remove columns that are not selected. then reorder the table
		ArrayList<ArrayList<String>> newData = new ArrayList<ArrayList<String>>();
		for (String name : selectedCol) {
			int index = column.indexOf(name);
			if (index == -1) {
				System.out.println("Table.filter: column name incorrect");
				return false;
			}
			newData.add(data.get(index));
		}

		column = selectedCol;
		data = newData;
		return true;
	}

	public boolean executeWhere(String whereStatement) {
		if (whereStatement == null) {
			return true;
		}
		ArrayList<ArrayList<String>> newData = new ArrayList<ArrayList<String>>();
		for (String name : column) {
			newData.add(new ArrayList<String>());
		}

		for (int i = 0; i < data.get(0).size(); i++) {
			if (keepRow(i, whereStatement)) {
				for (int j = 0; j < column.size(); j++) {
					newData.get(j).add(data.get(j).get(i));
				}
			}
		}

		data = newData;
		return true;
	}

	// returns true if we want to keep the row
	// stack reducer
	// the operators allowed are == != < > <= >= && || ( ) "
	public boolean keepRow(int row, String whereStatement) {

		Stack<Token> reducer = new Stack<Token>();

		// create lexime
		ArrayList<Token> lexime = lexer(whereStatement);
		// substitute var with corresponding literal
		for (int i = 0; i < lexime.size(); i++) {
			if (lexime.get(i).type.equals("var")) {
				String name = lexime.get(i).val;
				int index = column.indexOf(name);
				if (index == -1) {
					System.out.println("Table.keepRow: column not found");
					return true; // true means we are keeping the row since the vars are invalid so we will do
									// nothing
				}

				String value = data.get(index).get(row);
				lexime.get(i).type = "lit";
				lexime.get(i).val = value;
			}
		}

		// at this point we should have only lit and ops

		// now we begin to reduce
		while (lexime.size() > 0) {
			Token current = removeTop(lexime);

			if (current.val.equals(")")) { // begin to reduce the expression
				while (true) {
					Token temp = reducer.pop(); // storing temporary stack element

					if (temp.type.equals("lit") || temp.type.equals("bool")) {

						Token op = reducer.pop();

						if (!op.type.equals("op")) {
							System.out.println("Wrong grammar: expecting an operator");
						}
						if (op.val.equals("(")) { // finish the reduce
							reducer.push(temp);
							break;
						}

						Token firstArg = reducer.pop();

						if (!firstArg.type.equals(temp.type)) {
							System.out.println("Wrong grammar: argument types must match");
							return true;
						}

						Token result = Token.evaluate(firstArg, op, temp);

						reducer.push(result);

					} else {
						System.out.println("Wrong grammar: first item to reduce is not a literal or a boolean");
						return true;
					}

				}

			} else {
				reducer.push(current);
			}

		}

		// the final reduction makes sure that there is only one item left in the
		// array(a simple boolean)
		if (reducer.size() != 1) {
			while (reducer.size() != 1) {
				Token temp = reducer.pop(); // storing temporary stack element

				if (temp.type.equals("bool") || temp.type.equals("lit")) {

					Token op = reducer.pop();

					if (!op.type.equals("op")) {
						System.out.println("Wrong grammar: expecting an operator");
					}

					Token firstArg = reducer.pop();

					if (!firstArg.type.equals(temp.type)) {
						System.out.println("Wrong grammar: argument types must match");
						return true;
					}

					Token result = Token.evaluate(firstArg, op, temp);

					reducer.push(result);

				} else {
					System.out.println("Wrong grammar: first item to reduce is not a literal or a boolean!");
					return true;
				}

			}
		}

		return reducer.pop().val.equals("true");
	}

	public static Token removeTop(ArrayList<Token> s) {
		if (s.size() == 0) {
			return null;
		}
		return s.remove(0);
	}

	public static ArrayList<Token> lexer(String whereStatement) {
		ArrayList<Token> result = new ArrayList<Token>();

		result.add(new Token("op", "("));
		result.add(new Token("op", "("));
		result.add(new Token("op", "("));

		int pos = 0;

		while (pos < whereStatement.length()) {
			char current = whereStatement.charAt(pos);
			pos++;

			if (current == ' ' || current == '\n' || current == '\t') {
				continue;
			} else if (current == '(') {
				result.add(new Token("op", "("));
				result.add(new Token("op", "("));
				result.add(new Token("op", "("));
			} else if (current == ')') {
				result.add(new Token("op", ")"));
				result.add(new Token("op", ")"));
				result.add(new Token("op", ")"));
			} else if (current == '&') {
				if (pos >= whereStatement.length()) {
					System.out.println("Lexer Error: &");
					return null;
				}
				current = whereStatement.charAt(pos);
				pos++;
				if (current == '&') { // &&
					result.add(new Token("op", ")"));
					result.add(new Token("op", "&&"));
					result.add(new Token("op", "("));
				} else {
					System.out.println("Lexer Error: &&");
					return null;
				}
			} else if (current == '|') {
				if (pos >= whereStatement.length()) {
					System.out.println("Lexer Error: |");
					return null;
				}
				current = whereStatement.charAt(pos);
				pos++;
				if (current == '|') { // ||
					result.add(new Token("op", ")"));
					result.add(new Token("op", ")"));
					result.add(new Token("op", "||"));
					result.add(new Token("op", "("));
					result.add(new Token("op", "("));
				} else {
					System.out.println("Lexer Error: ||");
					return null;
				}
			} else if (current == '=') {
				if (pos >= whereStatement.length()) {
					System.out.println("Lexer Error: =");
					return null;
				}
				current = whereStatement.charAt(pos);
				pos++;
				if (current == '=') { // ==
					result.add(new Token("op", "=="));
				} else {
					System.out.println("current: " + current);
					System.out.println("Lexer Error: ==");
					return null;
				}
			} else if (current == '!') {
				if (pos >= whereStatement.length()) {
					System.out.println("Lexer Error: !");
					return null;
				}
				current = whereStatement.charAt(pos);
				pos++;
				if (current == '=') { // !=
					result.add(new Token("op", "!="));
				} else {
					System.out.println("Lexer Error: !=");
					return null;
				}
			} else if (current == '<') {
				if (pos >= whereStatement.length()) {
					System.out.println("Lexer Error: <");
					return null;
				}
				current = whereStatement.charAt(pos);

				if (current == '=') { // <=
					pos++;
					result.add(new Token("op", "<="));

				} else {
					result.add(new Token("op", "<"));
				}
			} else if (current == '>') {
				if (pos >= whereStatement.length()) {
					System.out.println("Lexer Error: >");
					return null;
				}
				current = whereStatement.charAt(pos);

				if (current == '=') { // >=
					pos++;
					result.add(new Token("op", ">="));

				} else {
					result.add(new Token("op", ">"));
				}
			} else if (current == '"') { // literal
				String value = "";

				if (pos >= whereStatement.length()) {
					System.out.println("Lexer Error: lit");
					return null;
				}

				while (whereStatement.charAt(pos) != '"') {
					value = value + whereStatement.charAt(pos);
					pos++;
					if (pos >= whereStatement.length()) {
						System.out.println("Lexer Error: lit end");
						return null;
					}
				}
				pos++;
				result.add(new Token("lit", value));
			}

			else if (Character.isDigit(current)) { // number
				String value = "";
				value = value + current;

				if (pos >= whereStatement.length()) {
					result.add(new Token("lit", value));
					result.add(new Token("op", ")"));
					result.add(new Token("op", ")"));
					result.add(new Token("op", ")"));
					return result;
				}

				while (Character.isDigit(whereStatement.charAt(pos))) {
					value = value + whereStatement.charAt(pos);
					pos++;
					if (pos >= whereStatement.length()) {
						result.add(new Token("lit", value));
						result.add(new Token("op", ")"));
						result.add(new Token("op", ")"));
						result.add(new Token("op", ")"));
						return result;
					}
				}
				result.add(new Token("lit", value));
			}

			else { // variable
				String value = "";
				value = value + current;

				if (pos >= whereStatement.length()) {
					result.add(new Token("var", value));
					result.add(new Token("op", ")"));
					result.add(new Token("op", ")"));
					result.add(new Token("op", ")"));
					return result;
				}

				while (whereStatement.charAt(pos) != '"' && whereStatement.charAt(pos) != '('
						&& whereStatement.charAt(pos) != ')' && whereStatement.charAt(pos) != '|'
						&& whereStatement.charAt(pos) != '&' && whereStatement.charAt(pos) != '<'
						&& whereStatement.charAt(pos) != '>' && whereStatement.charAt(pos) != '='
						&& whereStatement.charAt(pos) != '!' && whereStatement.charAt(pos) != ' '
						&& whereStatement.charAt(pos) != '\n' && whereStatement.charAt(pos) != '\t') {
					value = value + whereStatement.charAt(pos);
					pos++;
					if (pos >= whereStatement.length()) {
						result.add(new Token("var", value));
						result.add(new Token("op", ")"));
						result.add(new Token("op", ")"));
						result.add(new Token("op", ")"));
						return result;
					}
				}
				result.add(new Token("var", value));
			}
		}

		result.add(new Token("op", ")"));
		result.add(new Token("op", ")"));
		result.add(new Token("op", ")"));
		return result;
	}

	public String getKey() {
		return tableName + delim;
	}

	public String getValue() {
		String result = "";
		for (String s : column) {
			result = result + s + delim;
		}
		result = result + delim;

		for (int i = 0; i < data.get(0).size(); i++) {
			for (int j = 0; j < data.size(); j++) {
				result = result + data.get(j).get(i) + delim;
			}
		}

		return result;
	}
}

class Token {
	public String type; // possible states are lit, op, var, bool(used for reducer)
	public String val;

	public Token(String type, String val) {
		this.type = type;
		this.val = val;
	}

	public static Token evaluate(Token arg1, Token op, Token arg2) {
		if (op.val.equals("==")) {
			return new Token("bool", "" + (arg1.val.compareTo(arg2.val) == 0));
		} else if (op.val.equals("!=")) {
			return new Token("bool", "" + (arg1.val.compareTo(arg2.val) != 0));
		} else if (op.val.equals("<")) {
			return new Token("bool", "" + (arg1.val.compareTo(arg2.val) < 0));
		} else if (op.val.equals("<=")) {
			return new Token("bool", "" + (arg1.val.compareTo(arg2.val) <= 0));
		} else if (op.val.equals(">")) {
			return new Token("bool", "" + (arg1.val.compareTo(arg2.val) > 0));
		} else if (op.val.equals(">=")) {
			return new Token("bool", "" + (arg1.val.compareTo(arg2.val) >= 0));
		} else if (op.val.equals("&&")) {
			return new Token("bool", "" + (arg1.val.equals("true") && arg2.val.equals("true")));
		} else if (op.val.equals("||")) {
			return new Token("bool", "" + (arg1.val.equals("true") || arg2.val.equals("true")));
		}
		System.out.println("Unrecognized operator: " + op.val);

		return new Token("bool", "" + (false));
	}

	public String toString() {
		return "token: {" + type + "," + val + "}";
	}
}