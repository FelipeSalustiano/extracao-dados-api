import pandas as pd
import pyarrow.dataset as ds
from time import sleep
from main import modularization

# Pegar dados das pastas bronze
def catch_path(path):
    """
    Essa função busca pegar todas os arquivos parquet em uma pasta referênciada específica.
    Para realização correta da função, é necessário passar como parâmetros: o caminho da pasta 
    onde estão os arquivos.
    """

    print(f'Buscando dados do caminho {path}...')
    sleep(2)

    datasets = ds.dataset(path, format='parquet')
    table = datasets.to_table()
    df = table.to_pandas()

    print('Dados coletados!')
    return df

# Tratamento de dados
def data_processing(df):
    """
    Essa função realiza um tratamendo de dados básico, como preenchimento de nulos e exclusão de duplicatas.
    Para realização correta da função, é necessário passar como parâmetros: o dataframe.
    """

    print('Inciando tratamento de dados...')
    sleep(2)
    print('Preenchendo dados nulos...')
    sleep(2)

    # Preenchimento de dados nulos
    for col in df.columns:
        if df[col].dtype in ['object', 'str']:
            df[col] = df[col].fillna('Not informed')

        elif df[col].dtype in ['float64', 'int64', 'float32','int32']:
            df[col] = df[col].fillna(0)

        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].fillna(pd.Timestamp("1900-01-01"))

        elif pd.api.types.is_bool_dtype(df[col]):
            df[col] = df[col].fillna(False)

        else:
            df[col] = df[col].fillna("Unknown")

    print('Eliminando duplicatasa...')
    sleep(2)

    # Eliminando duplicatas
    df.drop_duplicates()

    print('Tratamento concluído!')
    return df

if __name__ == '__main__':
    
    print('============== INICIANDO PROGRAMA  ==============')
    sleep(2)

    df = catch_path('dataset/bronze')
    df = data_processing(df)
    modularization(df, 'dataset/silver')
