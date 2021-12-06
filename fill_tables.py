"""
Preencher as tabelas
"""
import psycopg2


def fill(csv_name, table_name):
    conn = psycopg2.connect("dbname='test' user='postgres' host='localhost' password='ces30'")
    cur = conn.cursor()
    with open(csv_name, 'r') as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, table_name, sep=',')
    conn.commit()


if __name__ == '__main__':
    tables = ['acidity', 'chemicals', 'sulfurdioxide', 'wines']
    csvs = ['./data/winequality_red_acidity.csv', './data/winequality_red_chemicals.csv',
            './data/winequality_red_sulfur.csv', './data/winequality_red_new.csv']
    #
    # 
    for table, csv in zip(tables, csvs):
        fill(csv, table)
