using System.Data;
using Microsoft.Data.SqlClient;

namespace Be.Vlaanderen.MsSqlServer.Cdc
{
    public static class ChangeDataCapture
    {
        private static IDbConnection OpenConnection(string connectionString)
        {
            var conn = new SqlConnection(connectionString);
            conn.Open();

            return conn;
        }

        private static bool ReadBoolResult(IDbCommand command)
        {
            using var reader = command.ExecuteReader() as SqlDataReader;
            if (reader == null)
            {
                return false;
            }

            if (reader.HasRows)
            {
                reader.Read();
            }

            return (bool)reader[0];
        }

        private static int ReadIntResult(IDbCommand command)
        {
            using var reader = command.ExecuteReader() as SqlDataReader;
            if (reader == null)
            {
                return 0;
            }

            if (reader.HasRows)
            {
                reader.Read();
            }

            return (int)reader[0];
        }

        public static bool TableExists(IDbConnection connection, string tableName)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = $"select case when object_id('{tableName}', 'U') is not null then 1 else 0 end";
            var result = cmd.ExecuteScalar();

            return !(result == null || (int)result == 0);
        }

        public static bool IsCdcEnabledOnDatabase(IDbConnection connection)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = "select is_cdc_enabled from sys.databases where name=db_name()";
            return ReadBoolResult(cmd);
        }

        public static void EnableCdcOnDatabase(IDbConnection connection, bool enable)
        {
            var condition = enable
                ? IsCdcEnabledOnDatabase(connection)
                : !IsCdcEnabledOnDatabase(connection);
            if (condition)
            {
                return;
            }

            var cmd = connection.CreateCommand();
            cmd.CommandText = enable
                ? "exec sys.sp_cdc_enable_db"
                : "exec sys.sp_cdc_disable_db";
            _ = cmd.ExecuteNonQuery();
        }

        public static void EnableCdcOnDatabase(string connectionString)
        {
            var connection = OpenConnection(connectionString);
            EnableCdcOnDatabase(connection, true);
        }

        public static bool IsCdcEnabledOnTable(IDbConnection connection, string tableName, string schemaName = null)
        {
            if (!TableExists(connection, $"{tableName}"))
            {
                return false;
            }

            if (!TableExists(connection, "cdc.change_tables"))
            {
                return false;
            }

            var cmd = connection.CreateCommand();
            cmd.CommandText = $"select count(*) from cdc.change_tables where capture_instance = '{schemaName ?? "dbo"}_{tableName}'";
            return ReadIntResult(cmd) > 0;
        }

        public static void EnableCdcOnTable(IDbConnection connection, bool enable, string tableName, string schemaName = null)
        {
            EnableCdcOnDatabase(connection, enable);

            var condition = enable
                ? IsCdcEnabledOnTable(connection, tableName, schemaName)
                : !IsCdcEnabledOnTable(connection, tableName, schemaName);
            if (condition)
            {
                return;
            }

            var cmd = connection.CreateCommand();
            cmd.CommandText = enable
                ? $"exec sys.sp_cdc_enable_table @source_schema = '{schemaName ?? "dbo"}', @source_name = '{tableName}', @role_name = null"
                : $"exec sys.sp_cdc_disable_table @source_schema = '{schemaName ?? "dbo"}', @source_name = '{tableName}', @capture_instance = '{schemaName ?? "dbo"}_{tableName}'";
            cmd.ExecuteNonQuery();
        }

        public static void EnableCdcOnTable(string connectionString, bool enable, string tableName, string schemaName = null)
        {
            var connection = OpenConnection(connectionString);
            EnableCdcOnTable(connection, enable, tableName, schemaName);
        }
    }
}
