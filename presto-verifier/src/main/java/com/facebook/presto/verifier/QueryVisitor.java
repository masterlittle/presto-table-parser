package com.facebook.presto.verifier;

import com.facebook.presto.sql.tree.*;

import java.util.HashSet;
import java.util.Set;

public class QueryVisitor<R, C> extends DefaultTraversalVisitor<Set<String>, C> {

    Set<String> tableList = new HashSet<>();

    @Override
    protected Set<String> visitSingleColumn(SingleColumn node, C context) {
        Expression expression = node.getExpression();
        return process(expression, context);
    }

    @Override
    protected Set<String> visitQuery(Query node, C context) {
        if (node.getWith().isPresent()) {
            process(node.getWith().get(), context);
        }
        process(node.getQueryBody(), context);
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }
        return tableList;
    }

    @Override
    protected Set<String> visitTable(Table node, C context) {
        tableList.add(node.getName().toString());
        return visitQueryBody(node, context);
    }


}
