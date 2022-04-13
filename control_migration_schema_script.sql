/* 
	NOTE: Make sure to change the <warehouse> and <schedule> values for tasks.
*/

/*
  	==========================================
    Control migration tables
    ==========================================
*/ 

DROP SCHEMA IF EXISTS CONTROL_MIGRATION;
CREATE SCHEMA         CONTROL_MIGRATION;
USE SCHEMA            CONTROL_MIGRATION;

-- This is a preliminary table of metadata control. This table should be filled with the table's
-- metadata from the s3 buckets and its respective mappings on Snowflake. They should be the same,
-- however considerations have been made in case the mapping is not 100% identical. 

-- Ideally, this data should be filled with a script ran on either the source database, Snowflake, or the s3 buckets.
CREATE OR REPLACE TABLE CONTROL_MIGRATION.DMS_METADATA
(
    full_path               varchar(500)    -- Full s3 path to table's files
    , db_table              varchar(50)    -- Table name on the source
    , db_schema             varchar(50)    -- Database name on the source
    , stage                 varchar(100)   -- Stage where the files are located
    , file_format           varchar(50)    -- File format to use on incremental_migrations
    , primary_keys          varchar(100)   -- Primary keys split by comma
    , sf_database           varchar(50)    -- Snowflake database name
    , sf_schema             varchar(50)    -- Snowflake schema name 
    , sf_table              varchar(50)    -- Snowflake table name
    , cloud_provider        varchar(50)    -- Cloud provider
    , bucket                varchar(100)   -- Bucket where the data is stored
    , db_prefix             varchar(100)   -- s3 database folder prefix. This would refer to the path to the database's folder within the s3 bucket.
    , additional_config     variant        -- Additional configuration options. Ideally a JSON with other parameters, that might be useful in the future such as partitioning.
    , last_full_load_date   datetime       -- Last date on which the data was fully loaded. This would be for control purposes.
    , last_incremental_file varchar(500)   -- Last file loaded. Used to control how much data is loaded from the external tables.
);

-- This is the execution queue. A main task will call a procedure that fills this table in order 
-- for children tasks to iterate over these records to execute the table loads.
-- This table contains the schema, the table and the type of the load (full or incremental). It
-- currently only works with full loads.
CREATE OR REPLACE TABLE CONTROL_MIGRATION.EXECUTION_QUEUE
(
    full_path         varchar(500)
    , type            varchar(10)  -- Type of load to run
    , task_in_charge  int          -- Position in the queue. This is used to control concurrency.
);

/*
    ==========================================
    Control migration procedures
    ==========================================
*/

-- This procedure prepares the execution queue for the children tasks to execute
-- More details on tasks later on this file.
-- This function should evaluate data from the stage and compare with the latest
-- full_load_date or latest_incremental_file to determine what to load.
CREATE OR REPLACE PROCEDURE prepare_migration_queue(manual_run boolean)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var task_validation = false;
    var task_count;
    
    if(MANUAL_RUN) {
        task_validation = true;
        task_count = 1;
    } else {
        var showTasksCMD = snowflake.execute({sqlText: `SHOW TASKS LIKE 'DATA_MIGRATION_CHILD%';`});
        var taskValidationTXT = `
            SELECT 
                COUNT(*) as task_count
                , SUM(CASE WHEN "state" = 'started' THEN 1 ELSE 0 END) as active_count
            FROM
                TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            ;
        `;

        var taskValidationCMD = snowflake.execute({sqlText: taskValidationTXT});
        var correctCount = false;

        if(taskValidationCMD.next()) {
            task_count = taskValidationCMD.getColumnValue('TASK_COUNT');
            var active_count = taskValidationCMD.getColumnValue('ACTIVE_COUNT');
            task_validation = task_count === active_count;
        }
    }
    
    if(task_validation) {
    
        // This query gets all stages registered on dms_metadata to see which stages need to be analyzed.
        var stagesRS = snowflake.execute({sqlText: `SELECT DISTINCT stage FROM control_migration.dms_metadata WHERE stage IS NOT NULL;`});
        var stagesCMD = [];

        truncateCMD = snowflake.execute({sqlText: `TRUNCATE TABLE control_migration.execution_queue;`});

        // Iterate stages to list their files and build a query from all stages by doing a union per each stage.
        while(stagesRS.next()){
            var stage = stagesRS.getColumnValue('STAGE');
            var rs = snowflake.execute({sqlText: `list @${stage}`})
            var stageQuery = `SELECT '${stage}' as stage, "name" as file, "last_modified" as file_date FROM TABLE(RESULT_SCAN('${rs.getQueryId()}'))`
            stagesCMD.push(stageQuery);
        }

        file_stages_sub_select = stagesCMD.join(' union ');

        // Queries all files from the stage and groups per full_path to get the last_incremental_file and full_load file
        // and compares to what is registed in dms_metadata to determine what type of load is necessary, if any.
        stages_summaryCMD = `
            INSERT INTO control_migration.execution_queue (FULL_PATH, TYPE, TASK_IN_CHARGE) 
            SELECT 
                dms.full_path
                , CASE 
                    WHEN stage.last_incremental_file > dms.last_incremental_file AND stage.full_load_file_date > dms.last_full_load_date THEN 'B'
                    WHEN stage.last_incremental_file > dms.last_incremental_file THEN 'I'
                    WHEN stage.full_load_file_date   > dms.last_full_load_date   THEN 'F'
                    ELSE 'N'
                END AS load_type
                , uniform(1, ${task_count}, seq1()) as t
            FROM 
            dms_metadata dms
            INNER JOIN
            (
                SELECT
                    stage
                    , REGEXP_REPLACE(file, '/(LOAD[0-9]{8}|2[0-9]{7}-[0-9]{9})..*$', '') as file_prefix
                    , MAX(CASE WHEN regexp_like(file, '.*/LOAD.*..*$') THEN '0' ELSE substring(file, position('/', file, 6) + 1) END) as last_incremental_file
                    , MAX(CASE WHEN regexp_like(file, '.*/LOAD.*..*$') THEN to_timestamp(file_date, 'DY, DD MON YYYY HH24:MI:SS GMT') ELSE NULL END) as full_load_file_date
                FROM
                (
                    ${file_stages_sub_select}
                )
                GROUP BY 
                    stage
                    , REGEXP_REPLACE(file, '/(LOAD[0-9]{8}|2[0-9]{7}-[0-9]{9})..*$', '')
            ) AS stage
                ON dms.full_path = stage.file_prefix
                    AND UPPER(dms.stage) = UPPER(stage.stage)
                    AND (
                        stage.last_incremental_file > dms.last_incremental_file
                        OR stage.full_load_file_date > dms.last_full_load_date
                    )
            ;
        `;
        
        var stages_summaryRS = snowflake.execute({sqlText: stages_summaryCMD});
        
        return stages_summaryRS.getNumRowsAffected();
        
    } else {
        throw 'Not all child tasks are active. Execute SHOW TASKS to see suspended tasks and activate them with ALTER TASK <name> RESUME;';
    }
$$
;

-- This procedure iterates over the execution queue looking for new tables to load
-- and calls the respective procedure depending on the load type required for the table
-- Stops executing when there are no more records on the queue.
CREATE OR REPLACE PROCEDURE CONTROL_MIGRATION.LOAD_TABLE(TASK_IDENTIFIER INT)
RETURNS REAL
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
    DECLARE 
        full_path varchar;
        load_type varchar;
    BEGIN
        LOOP

            -- Variables assignation
            SELECT 
                full_path, type
                INTO :full_path, :load_type 
            FROM 
                execution_queue 
            WHERE 
                task_in_charge = :TASK_IDENTIFIER 
            LIMIT 1;

            DELETE FROM execution_queue WHERE full_path = :full_path;
            
            -- Validation of empty value
            IF (full_path IS NULL) THEN
                BREAK;
            -- B stands for both load types
            ELSEIF (load_type = 'B') THEN
                CALL full_load(:full_path);
                CALL incremental_load(:full_path);
            ELSEIF (load_type = 'F') THEN
                CALL full_load(:full_path);
            ELSEIF (load_type = 'I') THEN
                CALL incremental_load(:full_path);
            END IF;
            
        END LOOP;
    END
$$
;

--Procedure that loads data from the stage into the tables.
--This procedure according to the (schema and table) loads the data
--from a predefined stage. 
--This procedure needs more work, such as using a stage from a parameter.
--The idea of this procedure is to copy the files into a stage, based on the 
--schema and database table. This combination will create a regex pattern
--for the COPY INTO command, specifying where to load the data from. Note that the
--pattern has a `LOAD.*` format. This is because FULL LOAD files on s3 appear with a 
--LOAD prefix.
--RETURNS: Number of rows affected (inserted).
CREATE OR REPLACE PROCEDURE control_migration.full_load(full_path varchar)
RETURNS string
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var metadataTxt = `SELECT db_schema, db_table, stage FROM control_migration.dms_metadata WHERE full_path = ?`
    var metadataCMD = snowflake.createStatement({ sqlText: metadataTxt, binds: [FULL_PATH] });
    metadataRS = metadataCMD.execute();

    snowflake.execute({sqlText: `alter session set query_tag='DMS_${FULL_PATH}'`});
    
    var db_table;
    var db_schema;
    
    if(metadataRS.next()) {
        db_table = metadataRS.getColumnValue('DB_TABLE');
        db_schema = metadataRS.getColumnValue('DB_SCHEMA');
        stage = metadataRS.getColumnValue('STAGE');
        
        var pattern = ".*\\/" + db_schema + "/" + db_table + "/LOAD[0-9]+\\.csv";
        var table_id = db_schema + "." + db_table;

        // Before copying into, table MUST be truncated.
        // This is because Snowflake keeps a track of files loaded, and if deleted from table
        // it will assume that the files have already been loaded.

        var truncateCMD = `TRUNCATE TABLE ` + table_id
        var copyIntoCMD = `COPY INTO ` + table_id + ` FROM @` + stage + ` pattern = '` + pattern + `'`;
        var updateFieldTXT = `UPDATE CONTROL_MIGRATION.DMS_METADATA SET LAST_FULL_LOAD_DATE = SYSDATE(), LAST_INCREMENTAL_FILE = '0' WHERE full_path = ?`;

        var truncate = snowflake.execute({ 
            sqlText: truncateCMD
        });
        var copy_into = snowflake.execute({ 
            sqlText: copyIntoCMD
        });
        var updateFieldCMD = snowflake.createStatement({ sqlText: updateFieldTXT, binds: [FULL_PATH] });
        var update_field = updateFieldCMD.execute();
        
        return copy_into.getNumRowsAffected();
    } else {
        return -1;
    }
$$
;

-- Procedure that loads data from an external table generated dynamically and then into 
-- the final table after making a query to obtain only new data as defined on the 
-- dms_metadata table.
-- This procedure creates an external table based on the metadata of the full_path
-- table and then queries its stage to get the data only after the last_incremental_file
-- processed. It then creates a merge statement, using as a source a query based on 
-- primary keys and generates a row_number to obtain the latest record and create a 
-- obtain the latest state of the table.
-- RETURNS: Number of rows affected (inserted/updated).
CREATE OR REPLACE PROCEDURE control_migration.incremental_load(full_path varchar)
RETURNS string
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var metadataTxt = `SELECT db_schema, db_table, stage, last_incremental_file, primary_keys FROM control_migration.dms_metadata WHERE full_path = ?`
    var metadataCMD = snowflake.createStatement({ sqlText: metadataTxt, binds: [FULL_PATH] });
    metadataRS = metadataCMD.execute();

    snowflake.execute({sqlText: `alter session set query_tag='DMS_${FULL_PATH}'`});
    var db_table;
    var db_schema;
    var last_incremental_file;
    var primary_keys;

    var comma_columns_arr = [];
    var column_names_src_prefix_arr = [];
    var tgt_src_equalized_non_pks_comma_sep_arr = [];
    var tgt_src_equalized_pks_and_sep_arr = [];
    var stage_query_columns_arr = ["$1::varchar(1) as op"];
    
    if(metadataRS.next()) {
        db_table = metadataRS.getColumnValue('DB_TABLE');
        db_schema = metadataRS.getColumnValue('DB_SCHEMA');
        stage = metadataRS.getColumnValueAsString('STAGE');
        last_incremental_file = metadataRS.getColumnValue('LAST_INCREMENTAL_FILE');
        primary_keys = metadataRS.getColumnValueAsString('PRIMARY_KEYS').replace(new RegExp(' ', 'g'), '');
        primary_keys_arr = primary_keys.split(',');

        var pattern = ".*\\/" + db_schema + "/" + db_table + "/2.*\\.csv";

        if(stage === 'null' || primary_keys === 'null') {
            return `The fields stage and primary_keys can't be null`;
        }

        var columnsCMD = `
            SELECT
                column_name, data_type, character_maximum_length, character_octet_length
                , numeric_precision, numeric_precision_radix, numeric_scale
            FROM
                information_schema.columns 
            WHERE 
                LOWER(table_name) = LOWER('${db_table}')
                AND LOWER(table_schema) = LOWER('${db_schema}')
            ORDER BY 
                ordinal_position
            ;
        `;

        columsRS = snowflake.execute({sqlText: columnsCMD});
        var columns = [];
        var col_cnt = 2;
        while(columsRS.next()) {
            var col = {
                column_name: columsRS.getColumnValue('COLUMN_NAME')
                , data_type: columsRS.getColumnValue('DATA_TYPE')
                , character_maximum_length: columsRS.getColumnValue('CHARACTER_MAXIMUM_LENGTH')
                , character_octet_length: columsRS.getColumnValue('CHARACTER_OCTET_LENGTH')
                , numeric_precision: columsRS.getColumnValue('NUMERIC_PRECISION')
                , numeric_precision_radix: columsRS.getColumnValue('NUMERIC_PRECISION_RADIX')
                , numeric_scale: columsRS.getColumnValue('NUMERIC_SCALE')
            }
            comma_columns_arr.push(col.column_name);
            column_names_src_prefix_arr.push(`S.${col.column_name}`);
            if(primary_keys_arr.includes(col.column_name)) {
                tgt_src_equalized_pks_and_sep_arr.push(`T.${col.column_name} = S.${col.column_name}`);
            } else {
                tgt_src_equalized_non_pks_comma_sep_arr.push(`T.${col.column_name} = S.${col.column_name}`);
            }
            var column_precision = '';
            if (col.data_type === 'NUMBER') {
                column_precision = `(${col.numeric_precision}, ${col.numeric_scale})`;
            } else if (col.data_type === 'TEXT') {
                column_precision = `(${col.character_maximum_length})`
            }
            stage_query_columns_arr.push('$' + `${col_cnt}::${col.data_type}${column_precision} as ${col.column_name}`)
            col_cnt += 1;
        }

        var external_table_name = `__dms_tmp_${db_table}_external`;
        var comma_columns = comma_columns_arr.join(', ');
        var column_names_src_prefix = column_names_src_prefix_arr.join(', ');
        var tgt_src_equalized_non_pks_comma_sep = tgt_src_equalized_non_pks_comma_sep_arr.join(', ');
        var tgt_src_equalized_pks_and_sep = tgt_src_equalized_pks_and_sep_arr.join(' AND ');
        var stage_query_columns = stage_query_columns_arr.join(', ');

        var max_file_stmt = snowflake.createStatement({ 
            sqlText: `select max(metadata$filename) as MAX_FILE from @${stage} (pattern => '${pattern}') where metadata$filename > ?`
            , binds: [last_incremental_file] 
        });

        var max_fileRS = max_file_stmt.execute();

        if(max_fileRS.next() && max_fileRS.getColumnValue('MAX_FILE') !== null)
        {
            var last_file_processed = max_fileRS.getColumnValueAsString('MAX_FILE');

            var mergeCMD = `
                MERGE INTO 
                    ${db_schema}.${db_table} AS T
                USING
                (
                    select
                        op, 
                        ${comma_columns}
                    from
                    (
                        select
                            *
                            -- Rank based on primary keys and order top to bottom by filename and row number.
                            , rank() over (partition by ${primary_keys} order by metadata$filename desc, _dms_file_control_rownum desc) as _dms_control_rank
                        from
                        (
                            select
                                ${stage_query_columns}
                                -- Row number to easily determine the latest row within the file
                                , metadata$file_row_number as _dms_file_control_rownum
                                , metadata$filename
                            from
                                @${stage} (pattern => '${pattern}')
                            where
                                metadata$filename > '${last_incremental_file}'
                        )
                    )
                    where 
                        _dms_control_rank = 1
                ) AS S
                ON 
                    ${tgt_src_equalized_pks_and_sep}
                WHEN MATCHED AND OP = 'D' THEN 
                    DELETE
                WHEN MATCHED AND OP <> 'D' THEN 
                    UPDATE SET ${tgt_src_equalized_non_pks_comma_sep}
                WHEN NOT MATCHED AND OP <> 'D' THEN
                    INSERT (${comma_columns})
                    VALUES (${column_names_src_prefix})
                ;
            `;

            var merge_result = snowflake.execute({sqlText: mergeCMD});
            var update_metadataCMD = snowflake.createStatement({
                sqlText: `UPDATE control_migration.dms_metadata SET last_incremental_file = ? WHERE full_path = ?;`
                , binds: [last_file_processed, FULL_PATH]
            }); 
            update_metadataCMD.execute();
            
            var rows_processed = merge_result.getNumRowsAffected();
                  
            return `Rows affected: ${rows_processed}.`;
        } else {
            return 'No files to process.';
        }
    } else {
        return `Specified full_path doesn't exist in dms_metadata table.`;
    }
$$
;

-- Procedure that queries the stage looking for tables metadata information
CREATE OR REPLACE PROCEDURE control_migration.fill_dms_metadata(stage varchar)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$ 
    var listTxt = `LIST @` + STAGE + `;`;
    var listCMD = snowflake.createStatement({ sqlText: listTxt });
    var queryTxt = `
        SELECT
            distinct 
            REGEXP_SUBSTR( "name", '((.*))\/',1, 1, 'e', 1) as full_path
            , REGEXP_SUBSTR( "name", '([[:alnum:]]*):\/\/(.*?)\/(.*)\/(.*?)\/(.*?)\/.*',1, 1, 'e', 1) as cloud_provider
            , REGEXP_SUBSTR( "name", '([[:alnum:]]*):\/\/(.*?)\/(.*)\/(.*?)\/(.*?)\/.*',1, 1, 'e', 2) as bucket
            , REGEXP_SUBSTR( "name", '([[:alnum:]]*):\/\/(.*?)\/(.*)\/(.*?)\/(.*?)\/.*',1, 1, 'e', 3) as db_prefix
            , REGEXP_SUBSTR( "name", '([[:alnum:]]*):\/\/(.*?)\/(.*)\/(.*?)\/(.*?)\/.*',1, 1, 'e', 4) as db_schema
            , REGEXP_SUBSTR( "name", '([[:alnum:]]*):\/\/(.*?)\/(.*)\/(.*?)\/(.*?)\/.*',1, 1, 'e', 5) as db_table
        FROM
            table(result_scan(last_query_id()))
        ;
    `
    var queryCMD = snowflake.createStatement({ sqlText: queryTxt });

    try { 
        listCMD.execute();
        var rs = queryCMD.execute();
        recordsInserted = 0;
        while(rs.next())
        {  
            var cloudProvider = rs.getColumnValue('CLOUD_PROVIDER');
            var fullPath      = rs.getColumnValue('FULL_PATH');
            var dbTable       = rs.getColumnValue('DB_TABLE');
            var dbSchema      = rs.getColumnValue('DB_SCHEMA');
            var bucket        = rs.getColumnValue('BUCKET');
            var dbPrefix      = rs.getColumnValue('DB_PREFIX');
            
            var insertMetadataStmt = snowflake.createStatement({
               sqlText: `INSERT INTO control_migration.dms_metadata (full_path, db_table, db_schema, bucket, db_prefix, cloud_provider, stage) VALUES (?,?,?,?,?,?,?);`
               , binds:[fullPath, dbTable, dbSchema, bucket, dbPrefix, cloudProvider, STAGE]
            });
            var insertRS = insertMetadataStmt.execute();
            recordsInserted += 1;
        }
        
        return recordsInserted;
    }
    catch (err) { 
        return "Failed: " + err;
    }
$$
;

/*
    ==========================================
    Control Migration Tasks
    ==========================================
*/

-- This main task calls the Prepare_migration_queue procedure, which checks
-- the metadata control and determines if a full load is required and inserts records
-- into the execution queue table. This will in turn create 5 children tasks, which
-- will call a procedure that iterates the execution queue looking for new tables to
-- load.
CREATE OR REPLACE TASK CONTROL_MIGRATION.DATA_MIGRATION_MAIN_TASK
  WAREHOUSE = <warehouse>
  SCHEDULE = '<set schedule here>' -- Options: 1 minute, 1 hour, etc. Mode details on schedule can be found here: 
AS                                 -- https://docs.snowflake.com/en/sql-reference/sql/create-task.html#optional-parameters
  CALL CONTROL_MIGRATION.PREPARE_MIGRATION_QUEUE(0)
;

-- Suspend main task. By default they're suspended, nevertheless this is here
-- to let know that before adding children tasks to it, the main task must be suspended.
ALTER TASK CONTROL_MIGRATION.DATA_MIGRATION_MAIN_TASK SUSPEND;

-- Children task that calls the execution queue iteration procedure. Notice the `AFTER`
-- keyword. This is used to specify that it will trigger after a successful execution of
-- the main task.
-- These have a SYSTEM$WAIT to avoid them repeating the load of a the same table and increasing
-- resource consumption.
CREATE OR REPLACE TASK CONTROL_MIGRATION.DATA_MIGRATION_CHILD_TASK_1
  WAREHOUSE = <warehouse>
  AFTER CONTROL_MIGRATION.DATA_MIGRATION_MAIN_TASK
AS
  CALL LOAD_TABLE(1);

CREATE OR REPLACE TASK CONTROL_MIGRATION.DATA_MIGRATION_CHILD_TASK_2
  WAREHOUSE = <warehouse>
  AFTER CONTROL_MIGRATION.DATA_MIGRATION_MAIN_TASK
AS
  CALL LOAD_TABLE(2);

CREATE OR REPLACE TASK CONTROL_MIGRATION.DATA_MIGRATION_CHILD_TASK_3
  WAREHOUSE = <warehouse>
  AFTER CONTROL_MIGRATION.DATA_MIGRATION_MAIN_TASK
AS
  CALL LOAD_TABLE(3);

CREATE OR REPLACE TASK CONTROL_MIGRATION.DATA_MIGRATION_CHILD_TASK_4
  WAREHOUSE = <warehouse>
  AFTER CONTROL_MIGRATION.DATA_MIGRATION_MAIN_TASK
AS
  CALL LOAD_TABLE(4);

CREATE OR REPLACE TASK CONTROL_MIGRATION.DATA_MIGRATION_CHILD_TASK_5
  WAREHOUSE = <warehouse>
  AFTER CONTROL_MIGRATION.DATA_MIGRATION_MAIN_TASK
AS
  CALL LOAD_TABLE(5);

ALTER TASK DATA_MIGRATION_CHILD_TASK_1 SET QUERY_TAG = 'DMS_TASK_1';
ALTER TASK DATA_MIGRATION_CHILD_TASK_2 SET QUERY_TAG = 'DMS_TASK_2';
ALTER TASK DATA_MIGRATION_CHILD_TASK_3 SET QUERY_TAG = 'DMS_TASK_3';
ALTER TASK DATA_MIGRATION_CHILD_TASK_4 SET QUERY_TAG = 'DMS_TASK_4';
ALTER TASK DATA_MIGRATION_CHILD_TASK_5 SET QUERY_TAG = 'DMS_TASK_5';

-- Tasks activation. The main task need to be suspended before creating other tasks
ALTER TASK CONTROL_MIGRATION.DATA_MIGRATION_CHILD_TASK_1 resume;
ALTER TASK CONTROL_MIGRATION.DATA_MIGRATION_CHILD_TASK_2 resume;
ALTER TASK CONTROL_MIGRATION.DATA_MIGRATION_CHILD_TASK_3 resume;
ALTER TASK CONTROL_MIGRATION.DATA_MIGRATION_CHILD_TASK_4 resume;
ALTER TASK CONTROL_MIGRATION.DATA_MIGRATION_CHILD_TASK_5 resume;
ALTER TASK CONTROL_MIGRATION.DATA_MIGRATION_MAIN_TASK    resume;