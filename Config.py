from sqlalchemy import create_engine
import urllib
# Database connection configuration for Norms database
DATABASE_CONFIG_NORMS = {
    'DRIVER': '{SQL Server}',
    'SERVER': 'test-server',
    'DATABASE': 'Norms',
}

# Database connection configuration for eresOCSPL database
DATABASE_CONFIG_ERES = {
    'DRIVER': '{SQL Server}',
    'SERVER': 'test-server',
    'DATABASE': 'eresOCSPL',
}

# Encode connection strings
connection_string_norms = urllib.parse.quote_plus(
    f"DRIVER={DATABASE_CONFIG_NORMS['DRIVER']};"
    f"SERVER={DATABASE_CONFIG_NORMS['SERVER']};"
    f"DATABASE={DATABASE_CONFIG_NORMS['DATABASE']};"
    "Trusted_Connection=yes;"
)

connection_string_eres = urllib.parse.quote_plus(
    f"DRIVER={DATABASE_CONFIG_ERES['DRIVER']};"
    f"SERVER={DATABASE_CONFIG_ERES['SERVER']};"
    f"DATABASE={DATABASE_CONFIG_ERES['DATABASE']};"
    "Trusted_Connection=yes;"
)

# Create SQLAlchemy engines
engine_norms = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string_norms}")
engine_eres = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string_eres}")