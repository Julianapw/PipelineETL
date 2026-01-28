import os
import requests
import pandas as pd
import json

def extract_data(endpoint):
    response = requests.get(endpoint)
    if response.status_code == 200:
        return response.json()
    else:
        (print(f"Error to extract API data: {response.status_code} "))
        return None
    

def loop_load_data(endpoint):
    url = "https://dummyjson.com/" + endpoint
    i = 1
    limit = 10
    while True:
        data = extract_data(url + "/" + str(i))
        if data or i<limit:
            load_data(data,"raw/" + endpoint)
        elif data and i>limit:
            break
        else:
            print(f"Error to extract API data: {data}")
            break
        i += 1


def load_data(data, path):
    """
    Salva um JSON em raw/{endpoint}/{id}.json
    """
    os.makedirs(path, exist_ok=True)
    _id = data.get("id")
    if _id is None:
        # Caso algum endpoint não traga 'id' na raiz:
        raise ValueError("Objeto JSON não possui o campo 'id'.")
    
    filename = os.path.join(path, f"{_id}.json")
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    print(f"[raw] Gravado: {filename}")


def loop_load_data(endpoint):
    """
    Baixa registros individuais em:
      https://dummyjson.com/{endpoint}/{i}
    por padrão de 1 até 10 (ajuste a variável 'limit' se quiser).
    """
    url = "https://dummyjson.com/" + endpoint
    i = 1
    limit = 10

    while i <= limit:
        data = extract_data(url + "/" + str(i))
        if data:
            load_data(data, os.path.join("raw", endpoint))
        else:
            # Se não houver dados para este i, paramos cedo
            print(f"[loop] Sem dados para {endpoint}/{i}. Encerrando.")
            break
        i += 1


def transform_data(endpoint, i):
    """
    Lê o JSON bruto salvo em raw/{endpoint}/{i}.json e retorna o dict carregado.
    """
    path = os.path.join("raw", endpoint, f"{i}.json")
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data


def transform_data_json_to_parquet(endpoint, i):
    """
    Lê raw/{endpoint}/{i}.json, normaliza (flatten) em DataFrame e salva em:
        silver/{endpoint}/{i}.parquet
    - Compressão: snappy
    - Engine: pyarrow (certifique-se de instalar: pip install pyarrow)
    """
    # 1) Ler JSON bruto
    try:
        record = transform_data(endpoint, i)
    except FileNotFoundError:
        print(f"[transform] Arquivo não encontrado: raw/{endpoint}/{i}.json")
        return None
    except Exception as e:
        print(f"[transform] Erro lendo JSON {endpoint}/{i}: {e}")
        return None

    # 2) Normalizar o JSON (flatten)
    try:
        # Se vier um dict (caso normal), vira 1 linha.
        # Se viesse lista, json_normalize também funciona, mas aqui esperamos dict.
        df = pd.json_normalize(record, sep="_")
    except Exception as e:
        print(f"[transform] Falha ao normalizar JSON {endpoint}/{i}: {e}")
        return None

    # 3) Metadados úteis
    df["endpoint"] = endpoint
    df["id_arquivo"] = i

    # 4) Salvar Parquet
    out_dir = os.path.join("silver", endpoint)
    os.makedirs(out_dir, exist_ok=True)
    file_parquet = os.path.join(out_dir, f"{i}.parquet")

    try:
        df.to_parquet(file_parquet, index=False, engine="pyarrow", compression="snappy")
        print(f"[parquet] Gravado: {file_parquet} (linhas={len(df)})")
        return file_parquet
    except Exception as e:
        print(f"[parquet] Erro ao gravar {file_parquet}: {e}")
        return None


if __name__ == "__main__":
    endpoints = ["users", "products"]

    # 1) Extrair e salvar JSONs brutos
    for endpoint in endpoints:
        loop_load_data(endpoint)

    # 2) Transformar cada JSON em um Parquet (um arquivo por id)
    for endpoint in endpoints:
        for i in range(1, 11):  # mesmo limite do loop_load_data
            transform_data_json_to_parquet(endpoint, i)
