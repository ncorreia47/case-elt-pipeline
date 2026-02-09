import requests
import os
import json
from datetime import datetime
from pathlib import Path
from api.enum_endpoints import Endpoints

BASE_DIR = Path(__file__).resolve().parent.parent
BASE_URL = 'http://api:8000'
DATE = datetime.now()
YEAR = DATE.strftime("%Y")
MONTH = ANO = DATE.strftime("%m")
CREATED_AT = DATE.strftime("%Y%m%d%H%M%S")
START_TIME = DATE.isoformat()


def api_get_data(endpoint : Endpoints):

    URL = f'{BASE_URL}/{endpoint.value}'
    BUCKET_FOLDER = f'{BASE_DIR}/bucket/{endpoint.value}/{YEAR}/{MONTH}'

    # Cria a pasta caso ela ainda nao exista
    try:
        os.makedirs(BUCKET_FOLDER, exist_ok=True)
    except OSError as folder_error:
        raise OSError(f'Ocorreu um erro ao criar a seguinte pasta: {BUCKET_FOLDER}: {folder_error}')

    all_data = []  # Lista para acumular os dados de todas as paginas
    page = 1

    # Conjunto de endpoints que aceitam o parâmetro start_time
    ENDPOINTS_WITH_START_TIME = {Endpoints.TICKETS, Endpoints.TICKET_METRICS, Endpoints.TICKET_SLA_EVENTS}

    # Inicializa os parâmetros da requisição
    params = {'page': page, 'page_size': 1000}

    # Adiciona start_time caso o endpoint precise dele
    if endpoint in ENDPOINTS_WITH_START_TIME:
        params['start_time'] = START_TIME

    while True:

        # Faz a requisicao considerando a pagina atual
        response = requests.get(URL, params={'page': page, 'page_size': 1000}, timeout=30)
        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError as json_error:
            raise ValueError(f'Resposta do JSON em formato inválido: {json_error}')
        
        # Acumula os dados de cada pagina na lista
        all_data.extend(data.get('data', []))

        # Verifica se existe proxima pagina, caso contrario, termina o loop
        if not data.get('has_next'):
            break
        page += 1

    # Cria o arquivo final contendo todos os dados
    if all_data:

        file_name = f'{endpoint.value}_{CREATED_AT}.json'
        path_file = os.path.join(BUCKET_FOLDER, file_name)

        try:
            with open(path_file, 'w', encoding='utf-8') as file:
                json.dump(all_data, file, ensure_ascii=False, indent=2)
                print(f'Arquivo salvo com sucesso em: {path_file}!')
        except TypeError as type_error:
            raise TypeError(f'Erro ao serializar os dados no formato JSON: {type_error}')
        except OSError as file_error:
            raise OSError(f'Erro ao salvar o arquivo {path_file}: {file_error}')
