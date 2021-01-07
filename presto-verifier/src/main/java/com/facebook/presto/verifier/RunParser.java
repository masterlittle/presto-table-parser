package com.facebook.presto.verifier;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RunParser {
    public List<String> run(String query) {
        try {
            SqlParser sqlParser = new SqlParser();
            Statement statement = sqlParser.createStatement(query, ParsingOptions.builder().build());
            Set<String> s =  statement.accept(new QueryVisitor<Set<String>, Object>(), this);
            List<String> tables = new ArrayList<>();
            tables.addAll(s);
            return tables;
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            return new ArrayList<String>(){{ add("Incorrect Query"); }};
        }
    }

}
