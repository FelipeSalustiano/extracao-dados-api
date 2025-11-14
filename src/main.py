import requests
import pandas as pd
from time import sleep

# Extração dos dados da API
def data_api(url, token, paginas):
    headers = {'Authorization': f'Token {token}'}

    dados = []
    pagina = 1

    print('Inciando extração de dados...')

    while pagina <= paginas:
        print(f'Baixando página {pagina}')

        response = requests.get(url, headers=headers)

        if response.status_code == 429:
            print('Muitas requisições — aguardando 30 segundos...')
            sleep(30)
            continue

        if response.status_code != 200:
            print(f'Erro {response.status_code}')
            break

        data = response.json()
        resultados = data.get('results', [])
        dados.extend(resultados)
        url = data.get('next')

        json = pd.DataFrame(resultados)
        json.to_json(f'dataset/raw/pagina_{pagina}.json', orient='records')

        pagina += 1

    print(f'Extração concluída! Arquivos salvos em: dataset/raw')
    print(f'Total registrado: {len(dados)} itens')
    
    df = pd.DataFrame(dados)
    return df


# Tranfomando para parquet e modularizando por ano e mes
def modularization(df, path):
    print('Iniciando modularização por ano e mês...')

    df.to_parquet(path, index=False, partition_cols=['ano', 'mes'])

    print('Modularização concluída!')
    print('Arquivos salvos em: dataset/bronze')
    
# Main (Iniciar programa)
if __name__ == '__main__': 
    
    print('============== INICIANDO PROGRAMA  ==============')
    sleep(2)

    data = data_api('https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data', '017d9c6aff5c7e3f7ee79edbeb518c8fe0507d10', paginas=1000)
    modularization(data, 'dataset\bronze')