import pandas as pd


def extract(df):
    df['wineid'] = df.index
    df['chemicalid'] = df.index + 1600
    df['sulfurid'] = df.index + 3200
    df['acidityid'] = df.index + 4800

    df_chemicals = df.filter(['chemicalid', 'residual sugar', 'chlorides', 'sulphates'], axis=1)
    df = df.drop(['residual sugar', 'chlorides', 'sulphates'], axis=1)

    df_acidity = df.filter(['acidityid', 'fixed acidity', 'volatile acidity', 'citric acid'], axis=1)
    df = df.drop(['fixed acidity', 'volatile acidity', 'citric acid'], axis=1)

    df_sulfur = df.filter(['sulfurid', 'free sulfur dioxide', 'total sulfur dioxide'], axis=1)
    df = df.drop(['free sulfur dioxide', 'total sulfur dioxide'], axis=1)

    df.to_csv("data/winequality_red_new.csv", index=None)
    df_chemicals.to_csv("data/winequality_red_chemicals.csv", index=None)
    df_sulfur.to_csv("data/winequality_red_sulfur.csv", index=None)
    df_acidity.to_csv("data/winequality_red_acidity.csv", index=None)


if __name__ == '__main__':
    df = pd.read_csv("data/winequality-red.csv")
    extract(df)
