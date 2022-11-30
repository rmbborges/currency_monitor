class SqlQueries:
    CREATE_CURRENCY_SCHEMA = """
        CREATE SCHEMA IF NOT EXISTS currency;
    """

    CREATE_DATA_TABLE = """
        CREATE TABLE IF NOT EXISTS currency.data(
            code VARCHAR(10),
            codein VARCHAR(10),
            name VARCHAR(100),
            high NUMERIC(10, 8),
            low NUMERIC(10, 8),
            varBid NUMERIC(10, 8),
            pctChange NUMERIC(10, 8),
            bid NUMERIC(10, 8),
            ask NUMERIC(10, 8),
            timestamp TIMESTAMP(1),
            created_date VARCHAR(10)
        );
    """

    CREATE_CLOSING_REFERENCE_TABLE = """
        CREATE TABLE IF NOT EXISTS currency.closing_reference(
            code VARCHAR(10),
            value NUMERIC(10, 8),
            timestamp TIMESTAMP(1)
        );
    """
    