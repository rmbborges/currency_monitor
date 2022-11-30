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
            varBid NUMERIC(10, 6),
            pctChange NUMERIC(10, 6),
            bid NUMERIC(10, 6),
            ask NUMERIC(10, 6),
            timestamp TIMESTAMP(1),
            created_date VARCHAR(20)
        );
    """

    DROP_DATA_TABLE = """
        DROP TABLE currency.data;
    """

    CREATE_CLOSING_REFERENCE_TABLE = """
        CREATE TABLE IF NOT EXISTS currency.closing_reference(
            code VARCHAR(10),
            value NUMERIC(10, 8),
            timestamp TIMESTAMP(1)
        );
    """

    DROP_CLOSING_REFERENCE_TABLE = """
        DROP TABLE currency.closing_reference;
    """
    