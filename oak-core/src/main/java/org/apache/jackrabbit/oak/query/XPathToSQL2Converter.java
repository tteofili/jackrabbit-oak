/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.util.ISO9075;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;

/**
 * This class can can convert a XPATH query to a SQL2 query.
 */
public class XPathToSQL2Converter {

    static final Logger LOG = LoggerFactory.getLogger(XPathToSQL2Converter.class);

    // Character types, used during the tokenizer phase
    private static final int CHAR_END = -1, CHAR_VALUE = 2;
    private static final int CHAR_NAME = 4, CHAR_SPECIAL_1 = 5, CHAR_SPECIAL_2 = 6;
    private static final int CHAR_STRING = 7, CHAR_DECIMAL = 8;

    // Token types
    private static final int KEYWORD = 1, IDENTIFIER = 2, END = 4, VALUE_STRING = 5, VALUE_NUMBER = 6;
    private static final int MINUS = 12, PLUS = 13, OPEN = 14, CLOSE = 15;

    // The query as an array of characters and character types
    private String statement;
    private char[] statementChars;
    private int[] characterTypes;

    // The current state of the parser
    private int parseIndex;
    private int currentTokenType;
    private String currentToken;
    private boolean currentTokenQuoted;
    private ArrayList<String> expected;
    private Selector currentSelector = new Selector();
    private ArrayList<Selector> selectors = new ArrayList<Selector>();

    /**
     * Convert the query to SQL2.
     *
     * @param query the query string
     * @return the SQL2 query
     * @throws ParseException if parsing fails
     */
    public String convert(String query) throws ParseException {
        query = query.trim();
        boolean explain = query.startsWith("explain ");
        if (explain) {
            query = query.substring("explain".length()).trim();
        }
        boolean measure = query.startsWith("measure");
        if (measure) {
            query = query.substring("measure".length()).trim();
        }
        
        if (query.isEmpty()) {
            // special case, will always result in an empty result
            query = "//jcr:root";
        }
        
        initialize(query);
        
        expected = new ArrayList<String>();
        read();
        
        if (currentTokenType == END) {
            throw getSyntaxError("the query may not be empty");
        }

        currentSelector.name = "a";

        ArrayList<Expression> columnList = new ArrayList<Expression>();
        
        String pathPattern = "";
        boolean startOfQuery = true;

        while (true) {
            
            // if true, path or nodeType conditions are not allowed
            boolean shortcut = false;
            boolean slash = readIf("/");
            
            if (!slash) {
                if (startOfQuery) {
                    // the query doesn't start with "/"
                    currentSelector.path = "/";
                    pathPattern = "/";
                    currentSelector.isChild = true;
                } else {
                    break;
                }
            } else if (readIf("jcr:root")) {
                // "/jcr:root" may only appear at the beginning
                if (!pathPattern.isEmpty()) {
                    throw getSyntaxError("jcr:root needs to be at the beginning");
                }
                if (readIf("/")) {
                    // "/jcr:root/"
                    currentSelector.path = "/";
                    pathPattern = "/";
                    if (readIf("/")) {
                        // "/jcr:root//"
                        pathPattern = "//";
                        currentSelector.isDescendant = true;
                    } else {
                        currentSelector.isChild = true;
                    }
                } else {
                    // for example "/jcr:root[condition]"
                    pathPattern = "/%";
                    currentSelector.path = "/";
                    shortcut = true;
                }
            } else if (readIf("/")) {
                // "//" was read
                pathPattern += "%";
                currentSelector.isDescendant = true;
            } else {
                // the token "/" was read
                pathPattern += "/";
                if (startOfQuery) {
                    currentSelector.path = "/";
                } else {
                    currentSelector.isChild = true;
                }
            }
            if (shortcut) {
                // "*" and so on are not allowed now
            } else if (readIf("*")) {
                // "...*"
                pathPattern += "%";
                if (!currentSelector.isDescendant) {
                    if (selectors.size() == 0 && currentSelector.path.equals("")) {
                        // the query /* is special
                        currentSelector.path = "/";
                    }
                }
            } else if (readIf("text")) {
                // "...text()"
                currentSelector.isChild = false;
                pathPattern += "jcr:xmltext";
                read("(");
                read(")");
                if (currentSelector.isDescendant) {
                    currentSelector.nodeName = "jcr:xmltext";
                } else {
                    currentSelector.path = PathUtils.concat(currentSelector.path, "jcr:xmltext");
                }
            } else if (readIf("element")) {
                // "...element(..."
                read("(");
                if (readIf(")")) {
                    // any
                    pathPattern += "%";
                } else {
                    if (readIf("*")) {
                        // any
                        pathPattern += "%";
                    } else {
                        String name = readPathSegment();
                        pathPattern += name;
                        appendNodeName(name);
                    }
                    if (readIf(",")) {
                        currentSelector.nodeType = readIdentifier();
                    }
                    read(")");
                }
            } else if (readIf("@")) {
                rewindSelector();
                Property p = readProperty();
                columnList.add(p);
            } else if (readIf("rep:excerpt")) {
                rewindSelector();
                readExcerpt();
                Property p = new Property(currentSelector, "rep:excerpt");
                columnList.add(p);
            } else if (readIf("(")) {
                rewindSelector();
                do {
                    if (readIf("@")) {
                        Property p = readProperty();
                        columnList.add(p);
                    } else if (readIf("rep:excerpt")) {
                        readExcerpt();
                        Property p = new Property(currentSelector, "rep:excerpt");
                        columnList.add(p);
                    }
                } while (readIf("|"));
                read(")");
            } else if (currentTokenType == IDENTIFIER) {
                // path restriction
                String name = readPathSegment();
                pathPattern += name;
                appendNodeName(name);
            } else if (readIf(".")) {
                // just "." this is simply ignored, so that
                // "a/./b" is the same as "a/b"
                if (readIf(".")) {
                    // ".." means "the parent of the node"
                    // handle like a regular path restriction
                    String name = "..";
                    pathPattern += name;
                    if (!currentSelector.isChild) {
                        currentSelector.nodeName = name;
                    } else {
                        if (currentSelector.isChild) {
                            currentSelector.isChild = false;
                            currentSelector.isParent = true;
                        }
                    }
                } else {
                    if (selectors.size() > 0) {
                        currentSelector = selectors.remove(selectors.size() - 1);
                        currentSelector.condition = null;
                        currentSelector.joinCondition = null;
                    }
                }
            } else {
                throw getSyntaxError();
            }
            if (readIf("[")) {
                Expression c = parseConstraint();
                currentSelector.condition = add(currentSelector.condition, c);
                read("]");
            }
            startOfQuery = false;
            nextSelector(false);
        }
        if (selectors.size() == 0) {
            nextSelector(true);
        }
        // the current selector wasn't used so far
        // go back to the last one
        currentSelector = selectors.get(selectors.size() - 1);
        if (selectors.size() == 1) {
            currentSelector.onlySelector = true;
        }
        ArrayList<Order> orderList = new ArrayList<Order>();
        if (readIf("order")) {
            read("by");
            do {
                Order order = new Order();
                order.expr = parseExpression();
                if (readIf("descending")) {
                    order.descending = true;
                } else {
                    readIf("ascending");
                }
                orderList.add(order);
            } while (readIf(","));
        }
        if (!currentToken.isEmpty()) {
            throw getSyntaxError("<end>");
        }
        StringBuilder buff = new StringBuilder();
        
        // explain | measure ...
        if (explain) {
            buff.append("explain ");
        } else if (measure) {
            buff.append("measure ");
        }
        
        // select ...
        buff.append("select ");
        buff.append(new Property(currentSelector, QueryImpl.JCR_PATH).toString());
        if (selectors.size() > 1) {
            buff.append(" as ").append('[').append(QueryImpl.JCR_PATH).append(']');
        }
        buff.append(", ");
        buff.append(new Property(currentSelector, QueryImpl.JCR_SCORE).toString());
        if (selectors.size() > 1) {
            buff.append(" as ").append('[').append(QueryImpl.JCR_SCORE).append(']');
        }
        if (columnList.isEmpty()) {
            buff.append(", ");
            buff.append(new Property(currentSelector, "*").toString());
        } else {
            for (int i = 0; i < columnList.size(); i++) {
                buff.append(", ");
                Expression e = columnList.get(i);
                String columnName = e.toString();
                buff.append(columnName);
                if (selectors.size() > 1) {
                    buff.append(" as [").append(e.getColumnAliasName()).append("]");
                }
            }
        }
        
        // from ...
        buff.append(" from ");
        for (int i = 0; i < selectors.size(); i++) {
            Selector s = selectors.get(i);
            if (i > 0) {
                buff.append(" inner join ");
            }
            String nodeType = s.nodeType;
            if (nodeType == null) {
                nodeType = "nt:base";
            }
            buff.append('[' + nodeType + ']').append(" as ").append(s.name);
            if (s.joinCondition != null) {
                buff.append(" on ").append(s.joinCondition);
            }
        }
        
        // where ...
        Expression where = null;
        for (Selector s : selectors) {
            where = add(where, s.condition);
        }
        if (where != null) {
            buff.append(" where ").append(where.toString());
        }
        // order by ...
        if (!orderList.isEmpty()) {
            buff.append(" order by ");
            for (int i = 0; i < orderList.size(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(orderList.get(i));
            }
        }

        // leave original xpath string as a comment
        buff.append(" /* xpath: ");
        buff.append(query);
        buff.append(" */");
        return buff.toString();
    }
    
    private void appendNodeName(String name) {
        if (!currentSelector.isChild) {
            currentSelector.nodeName = name;
        } else {
            if (selectors.size() > 0) {
                // no explicit path restriction - so it's a node name restriction
                currentSelector.isChild = true;
                currentSelector.nodeName = name;
            } else {
                currentSelector.isChild = false;
                String oldPath = currentSelector.path;
                // further extending the path
                currentSelector.path = PathUtils.concat(oldPath, name);
            }
        }
    }
    
    /**
     * Switch back to the old selector when reading a property. This occurs
     * after reading a "/", but then reading a property or a list of properties.
     * For example ".../(@prop)" is actually not a child node, but the same node
     * (selector) as before.
     */
    private void rewindSelector() {
        if (selectors.size() > 0) {
            currentSelector = selectors.remove(selectors.size() - 1);
            // prevent (join) conditions are added again
            currentSelector.isChild = false;
            currentSelector.isDescendant = false;
            currentSelector.path = "";
            currentSelector.nodeName = null;
        }
    }

    private void nextSelector(boolean force) throws ParseException {
        boolean isFirstSelector = selectors.size() == 0;
        String path = currentSelector.path;
        Expression condition = currentSelector.condition;
        Expression joinCondition = null;
        if (currentSelector.nodeName != null) {
            Function f = new Function("name");
            f.params.add(new SelectorExpr(currentSelector));
            String n = currentSelector.nodeName;
            // encode again, because it will be decoded again
            n = ISO9075.encode(n);
            Condition c = new Condition(f, "=", 
                    Literal.newString(n), 
                    Expression.PRECEDENCE_CONDITION);
            condition = add(condition, c);
        }
        if (currentSelector.isDescendant) {
            if (isFirstSelector) {
                if (!path.isEmpty()) {
                    if (!PathUtils.isAbsolute(path)) {
                        path = PathUtils.concat("/", path);
                    }
                    Function c = new Function("isdescendantnode");
                    c.params.add(new SelectorExpr(currentSelector));
                    c.params.add(Literal.newString(path));
                    condition = add(condition, c);
                }
            } else {
                Function c = new Function("isdescendantnode");
                c.params.add(new SelectorExpr(currentSelector));
                c.params.add(new SelectorExpr(selectors.get(selectors.size() - 1)));
                joinCondition = c;
            } 
        } else if (currentSelector.isParent) {
            if (isFirstSelector) {
                throw getSyntaxError();
            } else {
                Function c = new Function("ischildnode");
                c.params.add(new SelectorExpr(selectors.get(selectors.size() - 1)));
                c.params.add(new SelectorExpr(currentSelector));
                joinCondition = c;
            }
        } else if (currentSelector.isChild) {
            if (isFirstSelector) {
                if (!path.isEmpty()) {
                    if (!PathUtils.isAbsolute(path)) {
                        path = PathUtils.concat("/", path);
                    }
                    Function c = new Function("ischildnode");
                    c.params.add(new SelectorExpr(currentSelector));
                    c.params.add(Literal.newString(path));
                    condition = add(condition, c);
                }
            } else {
                Function c = new Function("ischildnode");
                c.params.add(new SelectorExpr(currentSelector));
                c.params.add(new SelectorExpr(selectors.get(selectors.size() - 1)));
                joinCondition = c;
            }
        } else {
            if (!force && condition == null && joinCondition == null) {
                // a child node of a given path, such as "/test"
                // use the same selector for now, and extend the path
            } else if (PathUtils.isAbsolute(path)) {
                Function c = new Function("issamenode");
                c.params.add(new SelectorExpr(currentSelector));
                c.params.add(Literal.newString(path));
                condition = add(condition, c);
            }
        }
        if (force || condition != null || joinCondition != null) {
            String nextSelectorName = "" + (char) (currentSelector.name.charAt(0) + 1);
            if (nextSelectorName.compareTo("x") > 0) {
                throw getSyntaxError("too many joins");
            }
            Selector nextSelector = new Selector();
            nextSelector.name = nextSelectorName;
            currentSelector.condition = condition;
            currentSelector.joinCondition = add(currentSelector.joinCondition, joinCondition);
            selectors.add(currentSelector);
            currentSelector = nextSelector;
        }
    }

    private static Expression add(Expression old, Expression add) {
        if (old == null) {
            return add;
        } else if (add == null) {
            return old;
        }
        return new Condition(old, "and", add, Expression.PRECEDENCE_AND);
    }

    private Expression parseConstraint() throws ParseException {
        Expression a = parseAnd();
        while (readIf("or")) {
            a = new Condition(a, "or", parseAnd(), Expression.PRECEDENCE_OR);
        }
        return a;
    }

    private Expression parseAnd() throws ParseException {
        Expression a = parseCondition();
        while (readIf("and")) {
            a = new Condition(a, "and", parseCondition(), Expression.PRECEDENCE_AND);
        }
        return a;
    }

    private Expression parseCondition() throws ParseException {
        Expression a;
        if (readIf("fn:not") || readIf("not")) {
            read("(");
            a = parseConstraint();
            if (a instanceof Condition && ((Condition) a).operator.equals("is not null")) {
                // not(@property) -> @property is null
                Condition c = (Condition) a;
                c = new Condition(c.left, "is null", null, Expression.PRECEDENCE_CONDITION);
                a = c;
            } else {
                Function f = new Function("not");
                f.params.add(a);
                a = f;
            }
            read(")");
        } else if (readIf("(")) {
            a = parseConstraint();
            read(")");
        } else {
            Expression e = parseExpression();
            if (e.isCondition()) {
                return e;
            }
            a = parseCondition(e);
        }
        return a;
    }

    private Condition parseCondition(Expression left) throws ParseException {
        Condition c;
        if (readIf("=")) {
            c = new Condition(left, "=", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf("<>")) {
            c = new Condition(left, "<>", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf("!=")) {
            c = new Condition(left, "<>", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf("<")) {
            c = new Condition(left, "<", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf(">")) {
            c = new Condition(left, ">", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf("<=")) {
            c = new Condition(left, "<=", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf(">=")) {
            c = new Condition(left, ">=", parseExpression(), Expression.PRECEDENCE_CONDITION);
        // TODO support "x eq y"? it seems this only matches for single value properties?  
        // } else if (readIf("eq")) {
        //    c = new Condition(left, "==", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else {
            c = new Condition(left, "is not null", null, Expression.PRECEDENCE_CONDITION);
        }
        return c;
    }

    private Expression parseExpression() throws ParseException {
        if (readIf("@")) {
            return readProperty();
        } else if (readIf("true")) {
            return Literal.newBoolean(true);
        } else if (readIf("false")) {
            return Literal.newBoolean(false);
        } else if (currentTokenType == VALUE_NUMBER) {
            Literal l = Literal.newNumber(currentToken);
            read();
            return l;
        } else if (currentTokenType == VALUE_STRING) {
            Literal l = Literal.newString(currentToken);
            read();
            return l;
        } else if (readIf("-")) {
            if (currentTokenType != VALUE_NUMBER) {
                throw getSyntaxError();
            }
            Literal l = Literal.newNumber('-' + currentToken);
            read();
            return l;
        } else if (readIf("+")) {
            if (currentTokenType != VALUE_NUMBER) {
                throw getSyntaxError();
            }
            return parseExpression();
        } else {
            return parsePropertyOrFunction();
        }
    }

    private Expression parsePropertyOrFunction() throws ParseException {
        StringBuilder buff = new StringBuilder();
        boolean isPath = false;
        while (true) {
            if (currentTokenType == IDENTIFIER) {
                String name = readIdentifier();
                buff.append(name);
            } else if (readIf("*")) {
                // any node
                buff.append('*');
                isPath = true;
            } else if (readIf(".")) {
                buff.append('.');
                if (readIf(".")) {
                    buff.append('.');
                }
                isPath = true;
            } else if (readIf("@")) {
                if (readIf("*")) {
                    // xpath supports @*, even thought jackrabbit may not
                    buff.append('*');
                } else {
                    buff.append(readIdentifier());
                }
                return new Property(currentSelector, buff.toString());
            } else {
                break;
            }
            if (readIf("/")) {
                isPath = true;
                buff.append('/');
            } else {
                break;
            }
        }
        if (!isPath && readIf("(")) {
            return parseFunction(buff.toString());
        } else if (buff.length() > 0) {
            // path without all attributes, as in:
            // jcr:contains(jcr:content, 'x')
            if (buff.toString().equals(".")) {
                buff = new StringBuilder("*");
            } else {
                buff.append("/*");
            }
            return new Property(currentSelector, buff.toString());
        }
        throw getSyntaxError();
    }

    private Expression parseFunction(String functionName) throws ParseException {
        if ("jcr:like".equals(functionName)) {
            Condition c = new Condition(parseExpression(), 
                    "like", null, Expression.PRECEDENCE_CONDITION);
            read(",");
            c.right = parseExpression();
            read(")");
            return c;
        } else if ("jcr:contains".equals(functionName)) {
            Function f = new Function("contains");
            f.params.add(parseExpression());
            read(",");
            f.params.add(parseExpression());
            read(")");
            return f;
        } else if ("jcr:score".equals(functionName)) {
            Function f = new Function("score");
            f.params.add(new SelectorExpr(currentSelector));
            read(")");
            return f;
        } else if ("xs:dateTime".equals(functionName)) {
            Expression expr = parseExpression();
            Cast c = new Cast(expr, "date");
            read(")");
            return c;
        } else if ("fn:lower-case".equals(functionName)) {
            Function f = new Function("lower");
            f.params.add(parseExpression());
            read(")");
            return f;
        } else if ("fn:upper-case".equals(functionName)) {
            Function f = new Function("upper");
            f.params.add(parseExpression());
            read(")");
            return f;
        } else if ("fn:name".equals(functionName)) {
            Function f = new Function("name");
            if (!readIf(")")) {
                // only name(.) and name() are currently supported
                read(".");
                read(")");
            }
            f.params.add(new SelectorExpr(currentSelector));
            return f;
        } else if ("jcr:deref".equals(functionName)) {
             // TODO maybe support jcr:deref
             throw getSyntaxError("jcr:deref is not supported");
        } else if ("rep:similar".equals(functionName)) {
             // TODO maybe support rep:similar
             throw getSyntaxError("rep:similar is not supported");
        } else if ("rep:spellcheck".equals(functionName)) {
            // TODO maybe support rep:spellcheck as in
            // /jcr:root[rep:spellcheck('${query}')]/(rep:spellcheck())            
            throw getSyntaxError("rep:spellcheck is not supported");
        } else {
            throw getSyntaxError("jcr:like | jcr:contains | jcr:score | xs:dateTime | " + 
                    "fn:lower-case | fn:upper-case | fn:name");
        }
    }

    private boolean readIf(String token) throws ParseException {
        if (isToken(token)) {
            read();
            return true;
        }
        return false;
    }

    private boolean isToken(String token) {
        boolean result = token.equals(currentToken) && !currentTokenQuoted;
        if (result) {
            return true;
        }
        addExpected(token);
        return false;
    }

    private void read(String expected) throws ParseException {
        if (!expected.equals(currentToken) || currentTokenQuoted) {
            throw getSyntaxError(expected);
        }
        read();
    }

    private Property readProperty() throws ParseException {
        if (readIf("*")) {
            return new Property(currentSelector, "*");
        }
        return new Property(currentSelector, readIdentifier());
    }
    
    private void readExcerpt() throws ParseException {
        read("(");
        if (!readIf(")")) {
            // only rep:excerpt(.) and rep:excerpt() are currently supported
            read(".");
            read(")");
        }
    }

    private String readPathSegment() throws ParseException {
        String raw = readIdentifier();
        return ISO9075.decode(raw);
    }

    private String readIdentifier() throws ParseException {
        if (currentTokenType != IDENTIFIER) {
            throw getSyntaxError("identifier");
        }
        String s = currentToken;
        read();
        return s;
    }

    private void addExpected(String token) {
        if (expected != null) {
            expected.add(token);
        }
    }

    private void initialize(String query) throws ParseException {
        if (query == null) {
            query = "";
        }
        statement = query;
        int len = query.length() + 1;
        char[] command = new char[len];
        int[] types = new int[len];
        len--;
        query.getChars(0, len, command, 0);
        command[len] = ' ';
        int startLoop = 0;
        for (int i = 0; i < len; i++) {
            char c = command[i];
            int type = 0;
            switch (c) {
            case '@':
            case '|':
            case '/':
            case '-':
            case '(':
            case ')':
            case '{':
            case '}':
            case '*':
            case ',':
            case ';':
            case '+':
            case '%':
            case '?':
            case '$':
            case '[':
            case ']':
                type = CHAR_SPECIAL_1;
                break;
            case '!':
            case '<':
            case '>':
            case '=':
                type = CHAR_SPECIAL_2;
                break;
            case '.':
                type = CHAR_DECIMAL;
                break;
            case '\'':
                type = CHAR_STRING;
                types[i] = CHAR_STRING;
                startLoop = i;
                while (command[++i] != '\'') {
                    checkRunOver(i, len, startLoop);
                }
                break;
            case '\"':
                type = CHAR_STRING;
                types[i] = CHAR_STRING;
                startLoop = i;
                while (command[++i] != '\"') {
                    checkRunOver(i, len, startLoop);
                }
                break;
            case ':':
            case '_':
                type = CHAR_NAME;
                break;
            default:
                if (c >= 'a' && c <= 'z') {
                    type = CHAR_NAME;
                } else if (c >= 'A' && c <= 'Z') {
                    type = CHAR_NAME;
                } else if (c >= '0' && c <= '9') {
                    type = CHAR_VALUE;
                } else {
                    if (Character.isJavaIdentifierPart(c)) {
                        type = CHAR_NAME;
                    }
                }
            }
            types[i] = (byte) type;
        }
        statementChars = command;
        types[len] = CHAR_END;
        characterTypes = types;
        parseIndex = 0;
    }

    private void checkRunOver(int i, int len, int startLoop) throws ParseException {
        if (i >= len) {
            parseIndex = startLoop;
            throw getSyntaxError();
        }
    }

    private void read() throws ParseException {
        currentTokenQuoted = false;
        if (expected != null) {
            expected.clear();
        }
        int[] types = characterTypes;
        int i = parseIndex;
        int type = types[i];
        while (type == 0) {
            type = types[++i];
        }
        int start = i;
        char[] chars = statementChars;
        char c = chars[i++];
        currentToken = "";
        switch (type) {
        case CHAR_NAME:
            while (true) {
                type = types[i];
                // the '-' can be part of a name,
                // for example in "fn:lower-case"
                // the '.' can be part of a name,
                // for example in "@offloading.status"
                if (type != CHAR_NAME && type != CHAR_VALUE 
                        && chars[i] != '-'
                        && chars[i] != '.') {
                    break;
                }
                i++;
            }
            currentToken = statement.substring(start, i);
            if (currentToken.isEmpty()) {
                throw getSyntaxError();
            }
            currentTokenType = IDENTIFIER;
            parseIndex = i;
            return;
        case CHAR_SPECIAL_2:
            if (types[i] == CHAR_SPECIAL_2) {
                i++;
            }
            currentToken = statement.substring(start, i);
            currentTokenType = KEYWORD;
            parseIndex = i;
            break;
        case CHAR_SPECIAL_1:
            currentToken = statement.substring(start, i);
            switch (c) {
            case '+':
                currentTokenType = PLUS;
                break;
            case '-':
                currentTokenType = MINUS;
                break;
            case '(':
                currentTokenType = OPEN;
                break;
            case ')':
                currentTokenType = CLOSE;
                break;
            default:
                currentTokenType = KEYWORD;
            }
            parseIndex = i;
            return;
        case CHAR_VALUE:
            long number = c - '0';
            while (true) {
                c = chars[i];
                if (c < '0' || c > '9') {
                    if (c == '.') {
                        readDecimal(start, i);
                        break;
                    }
                    if (c == 'E' || c == 'e') {
                        readDecimal(start, i);
                        break;
                    }
                    currentTokenType = VALUE_NUMBER;
                    currentToken = String.valueOf(number);
                    parseIndex = i;
                    break;
                }
                number = number * 10 + (c - '0');
                if (number > Integer.MAX_VALUE) {
                    readDecimal(start, i);
                    break;
                }
                i++;
            }
            return;
        case CHAR_DECIMAL:
            if (types[i] != CHAR_VALUE) {
                currentTokenType = KEYWORD;
                currentToken = ".";
                parseIndex = i;
                return;
            }
            readDecimal(i - 1, i);
            return;
        case CHAR_STRING:
            if (chars[i - 1] == '\'') {
                readString(i, '\'');
            } else {
                readString(i, '\"');
            }
            return;
        case CHAR_END:
            currentToken = "";
            currentTokenType = END;
            parseIndex = i;
            return;
        default:
            throw getSyntaxError();
        }
    }

    private void readString(int i, char end) throws ParseException {
        char[] chars = statementChars;
        String result = null;
        while (true) {
            for (int begin = i;; i++) {
                if (chars[i] == end) {
                    if (result == null) {
                        result = statement.substring(begin, i);
                    } else {
                        result += statement.substring(begin - 1, i);
                    }
                    break;
                }
            }
            if (chars[++i] != end) {
                break;
            }
            i++;
        }
        currentToken = result;
        parseIndex = i;
        currentTokenType = VALUE_STRING;
    }

    private void readDecimal(int start, int i) throws ParseException {
        char[] chars = statementChars;
        int[] types = characterTypes;
        while (true) {
            int t = types[i];
            if (t != CHAR_DECIMAL && t != CHAR_VALUE) {
                break;
            }
            i++;
        }
        if (chars[i] == 'E' || chars[i] == 'e') {
            i++;
            if (chars[i] == '+' || chars[i] == '-') {
                i++;
            }
            if (types[i] != CHAR_VALUE) {
                throw getSyntaxError();
            }
            while (types[++i] == CHAR_VALUE) {
                // go until the first non-number
            }
        }
        parseIndex = i;
        String sub = statement.substring(start, i);
        try {
            new BigDecimal(sub);
        } catch (NumberFormatException e) {
            throw new ParseException("Data conversion error converting " + sub + " to BigDecimal: " + e, i);
        }
        currentToken = sub;
        currentTokenType = VALUE_NUMBER;
    }

    private ParseException getSyntaxError() {
        if (expected == null || expected.isEmpty()) {
            return getSyntaxError(null);
        } else {
            StringBuilder buff = new StringBuilder();
            for (String exp : expected) {
                if (buff.length() > 0) {
                    buff.append(", ");
                }
                buff.append(exp);
            }
            return getSyntaxError(buff.toString());
        }
    }

    private ParseException getSyntaxError(String expected) {
        int index = Math.max(0, Math.min(parseIndex, statement.length() - 1));
        String query = statement.substring(0, index) + "(*)" + statement.substring(index).trim();
        if (expected != null) {
            query += "; expected: " + expected;
        }
        return new ParseException("Query:\n" + query, index);
    }

    /**
     * A selector.
     */
    static class Selector {

        /**
         * The selector name.
         */
        String name;
        
        /**
         * Whether this is the only selector in the query.
         */
        boolean onlySelector;
        
        /**
         * The node type, if set, or null.
         */
        String nodeType;
        
        /**
         * Whether this is a child node of the previous selector or a given path.
         * Examples:
         * <ul><li>/jcr:root/*
         * </li><li>/jcr:root/test/*
         * </li><li>/jcr:root/element()
         * </li><li>/jcr:root/element(*)
         * </li></ul>
         */
        boolean isChild;
        
        /**
         * Whether this is a parent node of the previous selector or given path.
         * Examples:
         * <ul><li>testroot//child/..[@foo1]
         * </li><li>/jcr:root/test/descendant/..[@test]
         * </li></ul>
         */
        boolean isParent;
        
        /**
         * Whether this is a descendant of the previous selector or a given path.
         * Examples:
         * <ul><li>/jcr:root//descendant
         * </li><li>/jcr:root/test//descendant
         * </li><li>/jcr:root[@x]
         * </li><li>/jcr:root (just by itself)
         * </li></ul>
         */
        boolean isDescendant;
        
        /**
         * The path (only used for the first selector).
         */
        String path = "";
        
        /**
         * The node name, if set.
         */
        String nodeName;
        
        /**
         * The condition for this selector.
         */
        Expression condition;
        
        /**
         * The join condition from the previous selector.
         */
        Expression joinCondition;
        
    }

    /**
     * An expression.
     */
    abstract static class Expression {
        
        static final int PRECEDENCE_OR = 1, PRECEDENCE_AND = 2, 
                PRECEDENCE_CONDITION = 3, PRECEDENCE_OPERAND = 4;
        
        /**
         * Whether this is a condition.
         * 
         * @return true if it is 
         */
        boolean isCondition() {
            return false;
        }
        
        /**
         * Get the operator / operation precedence. The JCR specification uses:
         * 1=OR, 2=AND, 3=condition, 4=operand  
         * 
         * @return the precedence (as an example, multiplication needs to return
         *         a higher number than addition)
         */
        int getPrecedence() {
            return PRECEDENCE_OPERAND;
        }
        
        /**
         * Get the column alias name of an expression. For a property, this is the
         * property name (no matter how many selectors the query contains); for
         * other expressions it matches the toString() method.
         * 
         * @return the simple column name
         */
        String getColumnAliasName() {
            return toString();
        }
        
        /**
         * Whether the result of this expression is a name. Names are subject to
         * ISO9075 encoding.
         * 
         * @return whether this expression is a name.
         */
        boolean isName() {
            return false;
        }

    }

    /**
     * A selector parameter.
     */
    static class SelectorExpr extends Expression {

        private final Selector selector;

        SelectorExpr(Selector selector) {
            this.selector = selector;
        }

        @Override
        public String toString() {
            return selector.name;
        }

    }

    /**
     * A literal expression.
     */
    static class Literal extends Expression {

        final String value;
        final String rawText;

        Literal(String value, String rawText) {
            this.value = value;
            this.rawText = rawText;
        }

        public static Expression newBoolean(boolean value) {
            return new Literal(String.valueOf(value), String.valueOf(value));
        }

        static Literal newNumber(String s) {
            return new Literal(s, s);
        }

        static Literal newString(String s) {
            return new Literal(SQL2Parser.escapeStringLiteral(s), s);
        }

        @Override
        public String toString() {
            return value;
        }

    }

    /**
     * A property expression.
     */
    static class Property extends Expression {

        final Selector selector;
        final String name;

        Property(Selector selector, String name) {
            this.selector = selector;
            this.name = name;
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder();
            if (!selector.onlySelector) {
                buff.append(selector.name).append('.');
            }
            if (name.equals("*")) {
                buff.append('*');
            } else {
                buff.append('[').append(name).append(']');
            }
            return buff.toString();
        }
        
        @Override
        public String getColumnAliasName() {
            return name;
        }

    }

    /**
     * A condition.
     */
    static class Condition extends Expression {

        final Expression left;
        final String operator;
        Expression right;
        final int precedence;

        /**
         * Create a new condition.
         * 
         * @param left the left hand side operator, or null
         * @param operator the operator
         * @param right the right hand side operator, or null
         * @param precedence the operator precedence (Expression.PRECEDENCE_...)
         */
        Condition(Expression left, String operator, Expression right, int precedence) {
            this.left = left;
            this.operator = operator;
            this.right = right;
            this.precedence = precedence;
        }
        
        @Override
        int getPrecedence() {
            return precedence;
        }

        @Override
        public String toString() {
            String leftExpr;
            boolean leftExprIsName;
            if (left == null) {
                leftExprIsName = false;
                leftExpr = "";
            } else {
                leftExprIsName = left.isName();
                leftExpr = left.toString();
                if (left.getPrecedence() < precedence) {
                    leftExpr = "(" + leftExpr + ")";
                }
            }
            boolean impossible = false;
            String rightExpr;
            if (right == null) {
                rightExpr = "";
            } else {
                if (leftExprIsName && !"like".equals(operator)) {
                    // need to de-escape _x0020_ and so on
                    if (!(right instanceof Literal)) {
                        throw new IllegalArgumentException(
                                "Can only compare a name against a string literal, not " + right);
                    }
                    Literal l = (Literal) right;
                    String raw = l.rawText;
                    String decoded = ISO9075.decode(raw);
                    String encoded = ISO9075.encode(decoded);
                    rightExpr = SQL2Parser.escapeStringLiteral(decoded);
                    if (!encoded.toUpperCase().equals(raw.toUpperCase())) {
                        // nothing can potentially match
                        impossible = true;
                    }
                } else {
                    rightExpr = right.toString();
                }
                if (right.getPrecedence() < precedence) {
                    rightExpr = "(" + right + ")";
                }
            }
            if (impossible) {
                // a condition that can not possibly be true
                return "upper(" + leftExpr + ") = 'never matches'";
            }
            return (leftExpr + " " + operator + " " + rightExpr).trim();
        }

        @Override
        boolean isCondition() {
            return true;
        }

    }

    /**
     * A function call.
     */
    static class Function extends Expression {

        final String name;
        final ArrayList<Expression> params = new ArrayList<Expression>();

        Function(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder(name);
            buff.append('(');
            for (int i = 0; i < params.size(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(params.get(i).toString());
            }
            buff.append(')');
            return buff.toString();
        }

        @Override
        boolean isCondition() {
            return name.equals("contains") || name.equals("not");
        }
        
        @Override
        boolean isName() {
            if ("upper".equals(name) || "lower".equals(name)) {
                return params.get(0).isName();
            }
            return "name".equals(name);
        }

    }

    /**
     * A cast operation.
     */
    static class Cast extends Expression {

        final Expression expr;
        final String type;

        Cast(Expression expr, String type) {
            this.expr = expr;
            this.type = type;
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("cast(");
            buff.append(expr.toString());
            buff.append(" as ").append(type).append(')');
            return buff.toString();
        }

        @Override
        boolean isCondition() {
            return false;
        }

    }

    /**
     * An order by expression.
     */
    static class Order {

        boolean descending;
        Expression expr;

        @Override
        public String toString() {
            return expr + (descending ? " desc" : "");
        }

    }

}

