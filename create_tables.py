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


def generate_csv_query() -> str:
    """
    Retorna a query que produz o csv
    """
    return """
    select ac.fixedacidity as "fixed acidity", ac.volatileacidity as "volatile acidity", ac.citricacid as "citric acid",
    ch.residualsugars as "residual sugar", ch.chlorides as "chlorides", sd.freesulfurdioxide as "free sulfur dioxide",
    sd.totalsulfurdioxide as "total sulfur dioxide", wi.density as "density", wi.ph as "pH", ch.sulphates as "sulphates",
    wi.alcohollevel as "alcohol", wi.quality as "quality"
    from acidity ac, chemicals ch, wines wi, sulfurdioxide sd
    where wi.acidityid = ac.acidityid
    and wi.sulfurid = sd.sulfurid
    and wi.chemicalid = ch.chemicalid
    """


def generate_csv():
    """
    Cria csv com todas as features
    """
    connection = None
    try:
        # read the connection parameters
        params = "dbname='test' user='test' host='localhost' password='ces30'"
        # connect to the PostgreSQL server
        connection = psycopg2.connect(params)
        current = connection.cursor()

        # psql command
        SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(generate_csv_query())
        csv_output = "wines.csv"

        # Trap errors for opening the file
        try:
            with open(csv_output, 'w') as f_output:
                current.copy_expert(SQL_for_file_output, f_output)
        except psycopg2.Error as e:
            print(e)

        # close communication with the PostgreSQL database server
        current.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()


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
    # generate_csv()