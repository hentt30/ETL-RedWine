"""
Script para criar as tabelas da nossa base de dados
"""
import psycopg2


def acidity_table_query() -> str:
    """
    Retorna a query que cria a tabela acidity
    """
    return """
    CREATE TABLE acidity (
    acidityId INTEGER PRIMARY KEY,
    fixedAcidity FLOAT NOT NULL,
    volatileAcidity FLOAT NOT NULL,
    citricAcid FLOAT NOT NULL
    );
    """


def chemicals_table_query() -> str:
    """
    Retorna a query que cria a tabela chemicals
    """
    return """
    CREATE TABLE chemicals(
    chemicalId INTEGER PRIMARY KEY,
    residualSugars FLOAT NOT NULL,
    chlorides FLOAT NOT NULL,
    sulphates FLOAT NOT NULL
    );
    """


def sulfurDioxide_table_query() -> str:
    """
    Retorna a query que cria a tabela sulfurDioxide
    """
    return """
    CREATE TABLE sulfurDioxide (
    sulfurId INTEGER PRIMARY KEY,
    freeSulfurDioxide FLOAT NOT NULL,
    totalSulfurDioxide FLOAT NOT NULL
    );
    """


def wines_table_query() -> str:
    """
    Retorna a query que cria a tabela wines
    """
    return """
    CREATE TABLE wines (
    density FLOAT NOT NULL,
    ph FLOAT NOT NULL,
    alcoholLevel FLOAT NOT NULL,
    quality INTEGER NOT NULL,
    wineId INTEGER PRIMARY KEY,
    chemicalId INTEGER NOT NULL,
    sulfurId INTEGER NOT NULL,
    acidityId INTEGER NOT NULL,
    FOREIGN KEY (chemicalId)
    REFERENCES  chemicals (chemicalId)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (sulfurId)
    REFERENCES  sulfurDioxide (sulfurId)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (acidityId)
    REFERENCES  acidity (acidityId)
    ON UPDATE CASCADE ON DELETE CASCADE
    );
    """


def create_tables():
    """
    Cria as tabelas
    """
    queries = [acidity_table_query(), chemicals_table_query(), sulfurDioxide_table_query(), wines_table_query()]

    connection = None
    try:
        # read the connection parameters
        params = "dbname='test' user='postgres' host='localhost' password='ces30'"
        # connect to the PostgreSQL server
        connection = psycopg2.connect(params)
        current = connection.cursor()
        # create table one by one
        for query in queries:
            current.execute(query)
        # close communication with the PostgreSQL database server
        current.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    create_tables()