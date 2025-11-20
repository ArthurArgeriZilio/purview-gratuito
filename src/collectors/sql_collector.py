import logging
import pyodbc
from azure.identity import DefaultAzureCredential

class SQLCollector:
    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.logger = logging.getLogger(__name__)

    def get_connection_string(self, server_name, database_name):
        """
        Creates connection string for Azure SQL with Azure AD authentication.
        Example server_name: 'myserver.database.windows.net'
        """
        # Get access token for Azure SQL
        token = self.credential.get_token("https://database.windows.net/.default")
        
        conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server_name};DATABASE={database_name};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
        return conn_str, token.token

    def scan_database_schema(self, server_name, database_name):
        """
        Scans a SQL database and extracts schema metadata (tables, columns, keys).
        """
        self.logger.info(f"Scanning database: {database_name} on {server_name}")
        schema_data = {
            "tables": [],
            "columns": [],
            "relationships": []
        }

        try:
            conn_str, token = self.get_connection_string(server_name, database_name)
            
            # Azure SQL requires token-based authentication
            token_bytes = token.encode("UTF-16-LE")
            token_struct = pyodbc.SQL_COPT_SS_ACCESS_TOKEN
            
            conn = pyodbc.connect(conn_str, attrs_before={token_struct: token_bytes})
            cursor = conn.cursor()

            # 1. Get all tables
            tables_query = """
                SELECT 
                    s.name AS schema_name,
                    t.name AS table_name,
                    t.object_id
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                ORDER BY s.name, t.name
            """
            cursor.execute(tables_query)
            tables = cursor.fetchall()

            for table in tables:
                schema_name, table_name, object_id = table
                full_table_name = f"{schema_name}.{table_name}"
                
                schema_data["tables"].append({
                    "id": f"{server_name}/{database_name}/{full_table_name}",
                    "schema": schema_name,
                    "name": table_name,
                    "full_name": full_table_name,
                    "object_id": object_id
                })

                # 2. Get columns for this table
                columns_query = """
                    SELECT 
                        c.name AS column_name,
                        t.name AS data_type,
                        c.max_length,
                        c.is_nullable,
                        c.is_identity
                    FROM sys.columns c
                    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                    WHERE c.object_id = ?
                    ORDER BY c.column_id
                """
                cursor.execute(columns_query, object_id)
                columns = cursor.fetchall()

                for col in columns:
                    col_name, data_type, max_length, is_nullable, is_identity = col
                    schema_data["columns"].append({
                        "id": f"{server_name}/{database_name}/{full_table_name}/{col_name}",
                        "table_id": f"{server_name}/{database_name}/{full_table_name}",
                        "name": col_name,
                        "data_type": data_type,
                        "max_length": max_length,
                        "is_nullable": bool(is_nullable),
                        "is_identity": bool(is_identity)
                    })

            # 3. Get Foreign Key relationships
            fk_query = """
                SELECT 
                    fk.name AS fk_name,
                    OBJECT_SCHEMA_NAME(fk.parent_object_id) + '.' + OBJECT_NAME(fk.parent_object_id) AS parent_table,
                    OBJECT_SCHEMA_NAME(fk.referenced_object_id) + '.' + OBJECT_NAME(fk.referenced_object_id) AS referenced_table
                FROM sys.foreign_keys fk
            """
            cursor.execute(fk_query)
            fks = cursor.fetchall()

            for fk in fks:
                fk_name, parent_table, referenced_table = fk
                schema_data["relationships"].append({
                    "name": fk_name,
                    "from_table": f"{server_name}/{database_name}/{parent_table}",
                    "to_table": f"{server_name}/{database_name}/{referenced_table}"
                })

            cursor.close()
            conn.close()

        except Exception as e:
            self.logger.error(f"Failed to scan database {database_name}: {e}")

        return schema_data
