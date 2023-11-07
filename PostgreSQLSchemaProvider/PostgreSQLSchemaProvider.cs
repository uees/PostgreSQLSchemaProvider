using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;

namespace SchemaExplorer
{
    public class PostgreSQLSchemaProvider : IDbSchemaProvider, IDbConnectionStringEditor
    {
        public string Name => "PostgreSQLSchemaProvider";

        public string Description => "PostgreSQL Schema Provider";

        public string ConnectionString => string.Empty;

        public bool EditorAvailable => false;

        public bool ShowEditor(string currentConnectionString)
        {
            return false;
        }

        public TableSchema[] GetTables(string connectionString, DatabaseSchema database)
        {
            var tableSchemas = new List<TableSchema>();
            using (var npgsqlConnection = new NpgsqlConnection(connectionString))
            {
                npgsqlConnection.Open();
                using (var npgsqlCommand = new NpgsqlCommand(
                    @"SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                        AND table_name <> 'CODESMITH_EXTENDED_PROPERTIES'
                    ORDER BY table_name", npgsqlConnection))
                {
                    using (var npgsqlDataReader = npgsqlCommand.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (npgsqlDataReader.Read())
                        {
                            string tabelName = npgsqlDataReader.GetValueOrDefault<string>("table_name");
                            string tableSchema = npgsqlDataReader.GetValueOrDefault<string>("table_schema");
                            tableSchemas.Add(new TableSchema(database,
                                    tabelName,
                                    tableSchema, 
                                    DateTime.MinValue));
                        }
                    }
                }

                if (npgsqlConnection.State != 0)
                {
                    npgsqlConnection.Close();
                }
            }
            return tableSchemas.ToArray();
        }

        public IndexSchema[] GetTableIndexes(string connectionString, TableSchema table)
        {
            string sql = $@"SELECT n.nspname AS table_schema, 
                    c.relname AS table_name, 
                    i.relname AS index_name,
                    a.attname AS column_name, 
                    x.indisunique AS is_unique, 
                    x.indisprimary AS is_primary,
                    x.indisclustered AS is_clustered 
                FROM pg_catalog.pg_index x 
                JOIN pg_catalog.pg_class C ON C.oid = x.indrelid 
                JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
                JOIN pg_catalog.pg_attribute a ON a.attrelid = i.relfilenode
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = C.relnamespace
                LEFT JOIN pg_catalog.pg_tablespace t ON t.oid = i.reltablespace 
                WHERE C.relkind = 'r' AND i.relkind = 'i' 
                    AND n.nspname = '{table.Owner}' 
                    AND c.relname  = '{table.Name}' 
                ORDER BY i.relname";
            var indexsMap = new Dictionary<string, IndexSchema>();
            using (var npgsqlConnection = new NpgsqlConnection(connectionString))
            {
                npgsqlConnection.Open();
                using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                {
                    using (var npgsqlDataReader = npgsqlCommand.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (npgsqlDataReader.Read())
                        {
                            string indexName = npgsqlDataReader.GetValueOrDefault<string>("index_name");
                            string tableSchema = npgsqlDataReader.GetValueOrDefault<string>("table_schema");
                            string tableName = npgsqlDataReader.GetValueOrDefault<string>("table_name");
                            string columnName = npgsqlDataReader.GetValueOrDefault<string>("column_name");
                            bool isPrimary = npgsqlDataReader.GetValueOrDefault<bool>("is_primary");
                            bool isUnique = npgsqlDataReader.GetValueOrDefault<bool>("is_unique");
                            bool isClustered = npgsqlDataReader.GetValueOrDefault<bool>("is_clustered");
                            string key = IndexSchema.FormatFullName(tableSchema, tableName, indexName);
                            
                            if (!indexsMap.TryGetValue(key, out IndexSchema indexSchema))
                            {
                                indexSchema = new IndexSchema(table, indexName, isPrimary, isUnique, isClustered);
                                indexsMap.Add(key, indexSchema);
                            }
                            var columnSchema = new MemberColumnSchema(table.Columns[columnName]);
                            indexSchema.MemberColumns.Add(columnSchema);
                        }
                    }
                }

                if (npgsqlConnection.State != 0)
                {
                    npgsqlConnection.Close();
                }
            }
                
            return indexsMap.Values.ToArray();
        }

        public ColumnSchema[] GetTableColumns(string connectionString, TableSchema table)
        {
            string sql = $@"SELECT table_schema, 
                    table_name, 
                    column_name, 
                    ordinal_position, 
                    column_default, 
                    is_nullable, 
                    data_type, 
                    character_maximum_length, 
                    numeric_precision, 
                    numeric_scale, 
                    udt_name 
                FROM information_schema.columns 
                WHERE table_schema = '{table.Owner}' AND table_name = '{table.Name}' 
                ORDER BY ordinal_position";
            var columnSchemas = new List<ColumnSchema>();
            var extendedProperties = new List<ExtendedProperty>();
            using (var npgsqlConnection = new NpgsqlConnection(connectionString))
            {
                npgsqlConnection.Open();
                using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                {
                    using (var npgsqlDataReader = npgsqlCommand.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (npgsqlDataReader.Read())
                        {
                            bool isNullable = npgsqlDataReader.GetValueOrDefault<string>("is_nullable") != "NO";
                            byte numericPrecision = npgsqlDataReader.GetValueOrDefault<byte>("numeric_precision");
                            int characterMaximumLength = npgsqlDataReader.GetValueOrDefault<int>("character_maximum_length");
                            int numericScale = npgsqlDataReader.GetValueOrDefault<int>("numeric_scale");
                            string columnName = npgsqlDataReader.GetValueOrDefault<string>("column_name");
                            string udtName = npgsqlDataReader.GetValueOrDefault<string>("udt_name");
                            string dataType = npgsqlDataReader.GetValueOrDefault<string>("data_type");
                            string columnDefault = npgsqlDataReader.GetValueOrDefault<string>("column_default");
                            bool setDefault = columnDefault == $"nextval('{table.Name}_{columnName}_seq'::regclass)" 
                                    || columnDefault == $"nextval('\"{table.Name}_{columnName}_seq\"'::regclass)";
                            
                            if (setDefault)
                                columnDefault = "";

                            if (udtName.StartsWith("_"))
                                udtName = udtName.Substring(1) + "[]";

                            extendedProperties.Clear();
                            extendedProperties.Add(new ExtendedProperty("CS_Default", columnDefault, 
                                DbType.String, PropertyStateEnum.ReadOnly));
                            extendedProperties.Add(new ExtendedProperty("CS_IsIdentity", setDefault, 
                                DbType.Boolean, PropertyStateEnum.ReadOnly));
                            extendedProperties.Add(new ExtendedProperty("CS_SystemType", dataType, 
                                DbType.String, PropertyStateEnum.ReadOnly));
                            extendedProperties.Add(new ExtendedProperty("CS_UserDefinedType", dataType, 
                                DbType.String, PropertyStateEnum.ReadOnly));

                            var item = new ColumnSchema(table, columnName, GetDbType(udtName), 
                                udtName, characterMaximumLength, numericPrecision, numericScale, 
                                isNullable, extendedProperties.ToArray());

                            columnSchemas.Add(item);
                        }
                    }
                }

                if (npgsqlConnection.State != 0)
                {
                    npgsqlConnection.Close();
                }
            }
           
            return columnSchemas.ToArray();
        }

        public TableKeySchema[] GetTableKeys(string connectionString, TableSchema table)
        {
            string text = $@"SELECT px.conname AS constraint_name,
                    fn.nspname AS table_schema,
                    fc.relname AS table_name,
                    fa.attname AS column_name,
                    rn.nspname AS reference_table_schema,
                    rc.relname AS reference_table_name,
                    ra.attname AS reference_column_name,
                    px.confupdtype AS on_update,
                    px.confdeltype AS on_delete 
                FROM pg_constraint px
                LEFT JOIN pg_catalog.pg_class fc ON (fc.oid = px.conrelid)
                LEFT JOIN pg_catalog.pg_class rc ON (rc.oid = px.confrelid)
                LEFT JOIN pg_catalog.pg_namespace fn ON (fn.oid = fc.relnamespace)
                LEFT JOIN pg_catalog.pg_namespace rn ON (rn.oid = rc.relnamespace)
                LEFT JOIN pg_catalog.pg_attribute fa ON (fa.attrelid = px.conrelid
                    AND fa.attnum = ANY (px.conkey))
                LEFT JOIN pg_catalog.pg_attribute ra ON (ra.attrelid = px.confrelid
                    AND ra.attnum = ANY (px.confkey))
                WHERE px.contype = 'f' AND fn.nspname = '{table.Owner}' 
                    AND fc.relname = '{table.Name}' 
                ORDER BY px.conname";
            var tableKeyMap = new Dictionary<string, TableKeySchema>();
            var extendedProperties = new List<ExtendedProperty>();
            var database = table.Database;
            using (var connection = new NpgsqlConnection(connectionString))
            {
                connection.Open();
                using (var npgsqlCommand = new NpgsqlCommand(text, connection))
                {
                    using (var dataReader = npgsqlCommand.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dataReader.Read())
                        {
                            var constraintName = dataReader.GetValueOrDefault<string>("constraint_name");
                            var referenceTableSchema = dataReader.GetValueOrDefault<string>("reference_table_schema");
                            var referenceTableName = dataReader.GetValueOrDefault<string>("reference_table_name");
                            var referenceColumnName = dataReader.GetValueOrDefault<string>("reference_column_name");
                            var tableSchemaName = dataReader.GetValueOrDefault<string>("table_schema");
                            var tableName = dataReader.GetValueOrDefault<string>("table_name");
                            var columnName = dataReader.GetValueOrDefault<string>("column_name");
                            bool cascadeDelete = dataReader.GetValueOrDefault<char>("on_delete") == 'c';
                            bool cascadeUpdate = dataReader.GetValueOrDefault<char>("on_update") == 'c';

                            var key = TableKeySchema.FormatFullName(referenceTableSchema, referenceTableName, constraintName) 
                                + "." + SchemaObjectBase.FormatFullName(tableSchemaName, tableName);
                            var tableSchema = database.Tables[referenceTableSchema, referenceTableName];
                            if (tableSchema != null)
                            {
                                if (!tableKeyMap.TryGetValue(key, out TableKeySchema tableKeySchema))
                                {
                                    extendedProperties.Clear();
                                    extendedProperties.Add(new ExtendedProperty("CS_CascadeDelete", 
                                        cascadeDelete, DbType.Boolean, PropertyStateEnum.ReadOnly));
                                    extendedProperties.Add(new ExtendedProperty("CS_CascadeUpdate", 
                                        cascadeUpdate, DbType.Boolean, PropertyStateEnum.ReadOnly));
                                    tableKeySchema = new TableKeySchema(constraintName, table, tableSchema, 
                                        extendedProperties.ToArray());
                                    tableKeyMap.Add(key, tableKeySchema);
                                }

                                var columnSchema = new MemberColumnSchema(tableSchema.Columns[referenceColumnName]);
                                tableKeySchema.PrimaryKeyMemberColumns.Add(columnSchema);

                                var columnSchema1 = new MemberColumnSchema(table.Columns[columnName]);
                                tableKeySchema.ForeignKeyMemberColumns.Add(columnSchema1);
                            }
                        }
                    }
                }
                
                if (connection.State != 0)
                {
                    connection.Close();
                }
            }
            
            return tableKeyMap.Values.ToArray();
        }

        public PrimaryKeySchema GetTablePrimaryKey(string connectionString, TableSchema table)
        {
            PrimaryKeySchema primaryKeySchema = null;
            foreach (var item in table.Indexes)
            {
                if (!item.IsPrimaryKey)
                {
                    continue;
                }
                primaryKeySchema = new PrimaryKeySchema(table, item.Name);
                foreach (MemberColumnSchema item2 in item.MemberColumns)
                {
                    primaryKeySchema.MemberColumns.Add(item2);
                }
                break;
            }
            return primaryKeySchema;
        }

        public DataTable GetTableData(string connectionString, TableSchema table)
        {
            string text = $"SELECT * FROM \"{table.Owner}\".\"{table.Name}\"";
            var dataTable = new DataTable(table.Name);
            using (var npgsqlConnection = new NpgsqlConnection(connectionString))
            {
                npgsqlConnection.Open();
                using (var npgsqlDataAdapter = new NpgsqlDataAdapter(text, npgsqlConnection))
                {
                    npgsqlDataAdapter.Fill(dataTable);
                }
                
                if (npgsqlConnection.State != 0)
                {
                    npgsqlConnection.Close();
                }
            }
           
            return dataTable;
        }

        public ExtendedProperty[] GetExtendedProperties(string connectionString, SchemaObjectBase schemaObject)
        {
            return new ExtendedProperty[0];
        }

        public void SetExtendedProperties(string connectionString, SchemaObjectBase schemaObject)
        {
            throw new NotImplementedException();
        }

        public ViewColumnSchema[] GetViewColumns(string connectionString, ViewSchema view)
        {
            string sql = $@"SELECT table_schema, 
                    table_name, 
                    column_name, 
                    ordinal_position,
                    column_default, 
                    is_nullable, 
                    data_type, 
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale, 
                    udt_name 
                FROM information_schema.columns 
                WHERE table_schema = '{view.Owner}' AND table_name = '{view.Name}' 
                ORDER BY ordinal_position";
            var viewColumns = new List<ViewColumnSchema>();
            var extendedProperties = new List<ExtendedProperty>();
            using (var npgsqlConnection = new NpgsqlConnection(connectionString))
            {
                npgsqlConnection.Open();
                using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                {
                    using (var dataReader = npgsqlCommand.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dataReader.Read())
                        {
                            bool isNullable = dataReader.GetValueOrDefault<string>("is_nullable") != "NO";
                            byte numericPrecision = dataReader.GetValueOrDefault<byte>("numeric_precision");
                            int characterMaximumLength = dataReader.GetValueOrDefault<int>("character_maximum_length");
                            int numericScale = dataReader.GetValueOrDefault<int>("numeric_scale");
                            string columnName = dataReader.GetValueOrDefault<string>("column_name");
                            string udtName = dataReader.GetValueOrDefault<string>("udt_name");
                            string dataType = dataReader.GetValueOrDefault<string>("data_type");

                            extendedProperties.Clear();
                            extendedProperties.Add(new ExtendedProperty("CS_SystemType",
                                dataType, DbType.String, PropertyStateEnum.ReadOnly));
                            extendedProperties.Add(new ExtendedProperty("CS_UserDefinedType",
                                dataType, DbType.String, PropertyStateEnum.ReadOnly));
                            viewColumns.Add(new ViewColumnSchema(view, columnName, 
                                GetDbType(udtName),
                                udtName,
                                characterMaximumLength,
                                numericPrecision,
                                numericScale, 
                                isNullable, 
                                extendedProperties.ToArray()));
                        }
                    }
                }
                
                if (npgsqlConnection.State != 0)
                {
                    npgsqlConnection.Close();
                }
            }

            return viewColumns.ToArray();
        }

        public DataTable GetViewData(string connectionString, ViewSchema view)
        {
            string text = $"SELECT * FROM \"{view.Owner}\".\"{view.Name}\"";
            var dataTable = new DataTable(view.Name);
            using (var npgsqlConnection = new NpgsqlConnection(connectionString))
            {
                npgsqlConnection.Open();
                
                using (var dataAdapter = new NpgsqlDataAdapter(text, npgsqlConnection))
                {
                    dataAdapter.Fill(dataTable);
                }

                if (npgsqlConnection.State != 0)
                {
                    npgsqlConnection.Close();
                }
            }

            return dataTable;
        }

        public ViewSchema[] GetViews(string connectionString, DatabaseSchema database)
        {
            string sql = @"SELECT table_schema, table_name 
                FROM information_schema.views 
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema') 
                ORDER BY table_name";

            var viewSchemas = new List<ViewSchema>();

            using (var npgsqlConnection = new NpgsqlConnection(connectionString))
            {
                npgsqlConnection.Open();
                using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                {
                    using (var dataReader = npgsqlCommand.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dataReader.Read())
                        {
                            string tableName = dataReader.GetValueOrDefault<string>("table_name");
                            string tableSchema = dataReader.GetValueOrDefault<string>("table_schema");
                            viewSchemas.Add(new ViewSchema(database, tableName, tableSchema, DateTime.MinValue));
                        }
                    }
                }

                if (npgsqlConnection.State != 0)
                {
                    npgsqlConnection.Close();
                }
            }

            return viewSchemas.ToArray();
        }

        public string GetViewText(string connectionString, ViewSchema view)
        {
            string sql = $@"SELECT view_definition 
                FROM information_schema.views 
                WHERE table_schema = '{view.Owner}' AND table_name = '{view.Name}'";
            
            string result;
            using (var npgsqlConnection = new NpgsqlConnection(connectionString))
            {
                npgsqlConnection.Open();

                using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                {
                    result = (string)npgsqlCommand.ExecuteScalar();
                }

                if (npgsqlConnection.State != 0)
                {
                    npgsqlConnection.Close();
                }
            }

            return result;
        }

        public CommandSchema[] GetCommands(string connectionString, DatabaseSchema database)
        {
            string sql = @" SELECT   specific_schema,   
                    specific_name,   
                    routine_schema,   
                    routine_name,   
                    routine_type,   
                    data_type,   
                    type_udt_name 
                FROM information_schema.routines 
                WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')";

            List<CommandSchema> commandSchemas = new List<CommandSchema>();
            List<ExtendedProperty> extendedProperties = new List<ExtendedProperty>();
            using (var npgsqlConnection = new NpgsqlConnection(connectionString))
            {
                npgsqlConnection.Open();
                using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                {
                    using (var dataReader = npgsqlCommand.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dataReader.Read())
                        {
                            string dataType = dataReader.GetValueOrDefault<string>("data_type");
                            bool isVoid = dataType.Equals("void", StringComparison.InvariantCultureIgnoreCase);
                            if (!isVoid || database.IncludeFunctions)
                            {
                                string specificName = dataReader.GetValueOrDefault<string>("specific_name");
                                string specificSchema = dataReader.GetValueOrDefault<string>("specific_schema");
                                string routineName = dataReader.GetValueOrDefault<string>("routine_name");
                                string routineSchema = dataReader.GetValueOrDefault<string>("routine_schema");

                                extendedProperties.Clear();
                                extendedProperties.Add(new ExtendedProperty("CS_IsScalarFunction", isVoid, 
                                    DbType.Boolean, PropertyStateEnum.ReadOnly));
                                extendedProperties.Add(new ExtendedProperty("CS_IsProcedure", isVoid, 
                                    DbType.Boolean, PropertyStateEnum.ReadOnly));
                                extendedProperties.Add(new ExtendedProperty("specific_name", specificName, 
                                    DbType.String, PropertyStateEnum.ReadOnly));
                                extendedProperties.Add(new ExtendedProperty("specific_schema", specificSchema, 
                                    DbType.String, PropertyStateEnum.ReadOnly));

                                commandSchemas.Add(new CommandSchema(database, 
                                    routineName, 
                                    routineSchema, 
                                    DateTime.MinValue, 
                                    extendedProperties.ToArray()));
                            }
                        }
                    }

                }

                if (npgsqlConnection.State != 0)
                {
                    npgsqlConnection.Close();
                }
            }

            return commandSchemas.ToArray();
        }

        public ParameterSchema[] GetCommandParameters(string connectionString, CommandSchema commandSchema)
        {
            string specificName = commandSchema.ExtendedProperties["specific_name"].Value as string;
            string specificSchema = commandSchema.ExtendedProperties["specific_schema"].Value as string;
            string sql = $@"SELECT specific_schema, 
                    specific_name, 
                    ordinal_position, 
                    parameter_mode,   
                    parameter_name, 
                    data_type, 
                    udt_name, 
                    character_maximum_length,
                    numeric_precision, 
                    numeric_scale 
                FROM information_schema.parameters 
                WHERE specific_schema = '{specificSchema}' AND specific_name = '{specificName}' 
                ORDER BY ordinal_position";

            var parameterSchemas = new List<ParameterSchema>();
            var extendedProperties = new List<ExtendedProperty>();
            using (var npgsqlConnection = new NpgsqlConnection(connectionString))
            {
                npgsqlConnection.Open();
                using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                {
                    using (var dataReader = npgsqlCommand.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dataReader.Read())
                        {
                            string parameterName = dataReader.GetValueOrDefault<string>("parameter_name");
                            byte numericPrecision = dataReader.GetValueOrDefault<byte>("numeric_precision");
                            int characterMaximumLength = dataReader.GetValueOrDefault<int>("character_maximum_length");
                            int numericScale = dataReader.GetValueOrDefault<int>("numeric_scale");
                            string udtName = dataReader.GetValueOrDefault<string>("udt_name");
                            string dataType = dataReader.GetValueOrDefault<string>("data_type");
                            // assert parameterName == dataReader.GetString(4)
                            // var direction = GetParameterDirection(dataReader.GetString(4));
                            var direction = GetParameterDirection(parameterName);
                            var dbType = GetDbType(udtName);

                            extendedProperties.Clear();
                            extendedProperties.Add(new ExtendedProperty("CS_SystemType",
                                dataType, DbType.String, (PropertyStateEnum)4));
                            extendedProperties.Add(new ExtendedProperty("CS_UserDefinedType",
                                dataType, DbType.String, (PropertyStateEnum)4));
                            parameterSchemas.Add(new ParameterSchema(commandSchema,
                                parameterName,
                                direction,
                                dbType,
                                udtName,
                                characterMaximumLength,
                                numericPrecision,
                                numericScale,
                                false,
                                extendedProperties.ToArray()));
                        }
                    }
                }

                if (npgsqlConnection.State != 0)
                {
                    npgsqlConnection.Close();
                }
            }

            return parameterSchemas.ToArray();
        }

        public CommandResultSchema[] GetCommandResultSchemas(string connectionString, CommandSchema command)
        {
            return new CommandResultSchema[0];
        }

        public string GetCommandText(string connectionString, CommandSchema commandSchema)
        {
            string text = $@"SELECT routine_definition 
                FROM information_schema.routines 
                WHERE routine_schema = '{commandSchema.Owner}' AND routine_name = '{commandSchema.Name}'";

            string result;
            using (NpgsqlConnection npgsqlConnection = new NpgsqlConnection(connectionString))
            {
                npgsqlConnection.Open();
                using (NpgsqlCommand npgsqlCommand = new NpgsqlCommand(text, npgsqlConnection))
                {
                    result = (string)npgsqlCommand.ExecuteScalar();
                }

                if (npgsqlConnection.State != 0)
                {
                    npgsqlConnection.Close();
                }
            }

            return result;
        }

        public string GetDatabaseName(string connectionString)
        {
            Regex regex = new Regex("Database\\W*=\\W*(?<database>[^;]*)", RegexOptions.IgnoreCase);
            if (regex.IsMatch(connectionString))
            {
                return regex.Match(connectionString).Groups["database"].ToString();
            }
            return connectionString;
        }

        private static DbType GetDbType(string type)
        {
            string text = type.ToLowerInvariant();
            if (text.EndsWith("[]"))
            {
                text = text.Substring(0, text.Length - 2);
            }
            switch (text)
            {
                case "bit":
                case "bool":
                case "boolean":
                    return DbType.Boolean;
                case "bytea":
                    return DbType.Binary;
                case "bpchar":
                case "char":
                case "character":
                case "text":
                case "varchar":
                case "character varying":
                case "jsonb":
                case "json":
                    return DbType.String;
                case "date":
                    return DbType.Date;
                case "float4":
                case "single precision":
                case "real":
                    return DbType.Single;
                case "float8":
                case "double precision":
                    return DbType.Double;
                case "int2":
                case "smallint":
                    return DbType.Int16;
                case "int4":
                case "integer":
                    return DbType.Int32;
                case "int8":
                case "bigint":
                    return DbType.Int64;
                case "money":
                case "numeric":
                    return DbType.Decimal;
                case "time":
                case "timetz":
                case "time without time zone":
                case "time without timezone":
                case "time with time zone":
                case "time with timezone":
                    return DbType.Time;
                case "interval":
                case "timestamp":
                case "timestamp without time zone":
                case "timestamp without timezone":
                    return DbType.DateTime;
                case "timestamptz":
                case "timestamp with time zone":
                case "timestamp with timezone":
                    return DbType.DateTimeOffset;
                case "uuid":
                    return DbType.Guid;
                case "box":
                case "circle":
                case "inet":
                case "line":
                case "lseg":
                case "path":
                case "point":
                case "polygon":
                case "refcursor":
                    return DbType.Object;
                case "xml":
                    return DbType.Xml;
                default:
                    return DbType.Object;
            }
        }

        private static ParameterDirection GetParameterDirection(string direction)
        {
            if (!(direction == "IN"))
            {
                if (direction == "OUT")
                {
                    return ParameterDirection.Output;
                }
                return ParameterDirection.InputOutput;
            }
            return ParameterDirection.Input;
        }
    }
}
