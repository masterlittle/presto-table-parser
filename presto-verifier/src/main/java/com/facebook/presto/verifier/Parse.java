package com.facebook.presto.verifier;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Stat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class Parse {
        static String sql = "select date(task_created_on) as dt,\n" +
                "b.uuid,\n" +
                "b.first_name,\n" +
                "a.task_id,\n" +
                "a.usertask_uuid as task_reference_id,\n" +
                "a.runner_task_id,\n" +
                "a.runner_id ,\n" +
                "c.first_name as Runner_Name,\n" +
                "a.rt_task_distance,\n" +
                "d.actualdeliverycharge,\n" +
                "case when cast(json_extract(e.value,'$.enable_cod_on_task') as varchar)='true' then 'COD' else 'NON COD' end as COD_flag,\n" +
                "case when cast(json_extract(e.value,'$.new_cod_flow') as varchar)='true' then 'New flow' end as COD_flow_flag,\n" +
                "cast(json_extract(e.value,'$.actual_cod_amount') as varchar) as actual_cod_amount,\n" +
                "f.reference_id as reference_id,\n" +
                "g.request_id as request_id\n" +
                "FROM metrics.dunzo_task a \n" +
                "LEFT JOIN metrics.dunzo_user b\n" +
                "ON a.user_id = b.id \n" +
                "LEFT JOIN metrics.dunzo_user c\n" +
                "ON a.runner_id = c.id\n" +
                "LEFT JOIN metrics.dz_task_c1_calculation d\n" +
                "on a.task_id=d.task_id\n" +
                "left join\n" +
                "    ( select entity_id,entity_type,\n" +
                "    value from\n" +
                "    metrics.common_entityattribute\n" +
                "    where name='cod_instructions'\n" +
                "    and entity_type='user_task') e\n" +
                "    on e.entity_id=a.usertask_uuid\n" +
                "left join\n" +
                "    ( select entity_id,entity_type,\n" +
                "    value as reference_id\n" +
                "    from\n" +
                "    metrics.common_entityattribute\n" +
                "    where name='reference_id'\n" +
                "    and entity_type='user_task') f\n" +
                "    on f.entity_id=a.usertask_uuid\n" +
                "    left join\n" +
                "    ( select entity_id,entity_type,\n" +
                "    value as request_id\n" +
                "    from\n" +
                "    metrics.common_entityattribute\n" +
                "    where name='request_id'\n" +
                "    and entity_type='user_task') g\n" +
                "    on g.entity_id=a.usertask_uuid\n" +
                "WHERE runner_task_status = 'COMPLETED'\n" +
                "and task_creation_type IN ('APIUSER')\n" +
                "and task_created_on >= date '2019-05-01'  \n" +
                "ORDER BY 1 desc";

    public static void main(String[] args) throws IOException {
        RunParser runParser = new RunParser();
        CSVParser csvParser = readCSV("/Users/shitij/VSCode/tableau-metadata/tableau/output.csv", new ArrayList<>());
        List<String> headerNames = csvParser.getHeaderNames();
        CSVParser csvParser2 = readCSV("/Users/shitij/VSCode/tableau-metadata/tableau/output.csv", headerNames);
        List<OutputRecord> outputRecordList = new ArrayList<>();
        for (CSVRecord csvRecord: csvParser2){
            String query = csvRecord.get("query");
            outputRecordList.add(
                    new OutputRecord(csvRecord.get("query_id"),query, csvRecord.get("datasource_name"),
                            csvRecord.get("datasource_id"), csvRecord.get("datasource_project_name"),
                            csvRecord.get("workbook_name"), csvRecord.get("workbook_id"),
                            csvRecord.get("workbook_project_name"),
                            csvRecord.get("dashboard_id"), csvRecord.get("dashboard_name"), runParser.run(query))
            );
        }
        writeCSV(outputRecordList);
    }
    static CSVParser readCSV(String filename, List<String> headers) throws IOException {
        BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(filename));
        CSVParser csvParser = new CSVParser(bufferedReader, CSVFormat.EXCEL.withHeader(headers.toArray(new String[]{})));
        return csvParser;
    }

    static void writeCSV(List<OutputRecord> outputRecords) throws IOException {
        List<String> attributes =  Arrays.stream(outputRecords.get(0).getClass().getDeclaredFields()).map(a -> a.getName()).collect(Collectors.toList());
        Writer writer = Files.newBufferedWriter(Paths.get("table_name_output.csv"));
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.EXCEL.withHeader(attributes.toArray(new String[]{})));
        for (OutputRecord csvRecord: outputRecords) {
            csvPrinter.printRecord(csvRecord.query_id, csvRecord.query, csvRecord.datasource_name,
                    csvRecord.datasource_id, csvRecord.datasource_project_name, csvRecord.workbook_name,
                    csvRecord.workbook_id, csvRecord.workbook_project_name, csvRecord.dashboard_id, csvRecord.dashboard_name,
                    csvRecord.tableNames);
        }
    }

    static class OutputRecord {
        String query_id;
        String query;
        String datasource_name;
        String datasource_id;
        String datasource_project_name;
        String workbook_name;
        String workbook_id;
        String workbook_project_name;
        String dashboard_id;
        String dashboard_name;
        List<String> tableNames;

        public OutputRecord() {
        }

        public OutputRecord(String query_id, String query, String datasource_name, String datasource_id,
                            String datasource_project_name, String workbook_name, String workbook_id,
                            String workbook_project_name, String dashboard_id, String dashboard_name,
                            List<String> tableNames) {
            this.query_id = query_id;
            this.query = query;
            this.datasource_name = datasource_name;
            this.datasource_id = datasource_id;
            this.datasource_project_name = datasource_project_name;
            this.workbook_name = workbook_name;
            this.workbook_id = workbook_id;
            this.workbook_project_name = workbook_project_name;
            this.dashboard_id = dashboard_id;
            this.dashboard_name = dashboard_name;
            this.tableNames = tableNames;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OutputRecord)) return false;
            OutputRecord that = (OutputRecord) o;
            return query_id.equals(that.query_id) &&
                    query.equals(that.query) &&
                    Objects.equals(tableNames, that.tableNames) &&
                    Objects.equals(datasource_name, that.datasource_name) &&
                    Objects.equals(datasource_id, that.datasource_id) &&
                    Objects.equals(datasource_project_name, that.datasource_project_name) &&
                    Objects.equals(workbook_name, that.workbook_name) &&
                    Objects.equals(workbook_id, that.workbook_id) &&
                    Objects.equals(workbook_project_name, that.workbook_project_name) &&
                    Objects.equals(dashboard_id, that.dashboard_id) &&
                    Objects.equals(dashboard_name, that.dashboard_name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query_id, query, datasource_name, tableNames, datasource_id, datasource_project_name, workbook_name, workbook_id, workbook_project_name, dashboard_id, dashboard_name);
        }

        @Override
        public String toString() {
            return query_id + ',' + query + ',' + dashboard_name + ',' + datasource_id + ',' + datasource_project_name +
                    ',' + workbook_name + ',' + workbook_id + ',' + workbook_project_name + ',' + dashboard_id + ',' +
                    ',' + dashboard_name + ',' + tableNames + '\n';
        }
    }

}