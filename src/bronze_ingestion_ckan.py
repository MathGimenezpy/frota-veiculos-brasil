from pathlib import Path
import requests
import zipfile
import io
import pandas as pd
import time
import hashlib

# =========================
# Configurações do projeto
# =========================

PACKAGE_ID = "registro-nacional-de-veiculos-automotores-renavam"
CKAN_PACKAGE_SHOW_URL = "https://dados.transportes.gov.br/api/3/action/package_show"

BASE_DIR = Path(__file__).resolve().parents[1]

BRONZE_DIR = BASE_DIR / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

METADATA_DIR = BASE_DIR / "data" / "metadata"
METADATA_DIR.mkdir(parents=True, exist_ok=True)

HASH_PATH = METADATA_DIR / "renavam_hash.txt"

# =========================
# Utilidades — Hash
# =========================

def calcular_hash_bytes(conteudo: bytes) -> str:
    return hashlib.md5(conteudo).hexdigest()

def ler_hash_anterior():
    if not HASH_PATH.exists():
        return None
    return HASH_PATH.read_text().strip()

def salvar_hash(hash_valor: str):
    HASH_PATH.write_text(hash_valor)

# =========================
# Bloco 1 — CKAN
# =========================

def buscar_resources_ckan():
    for tentativa in range(3):
        try:
            r = requests.get(
                CKAN_PACKAGE_SHOW_URL,
                params={"id": PACKAGE_ID},
                timeout=30
            )
            r.raise_for_status()
            data = r.json()

            if not data.get("success"):
                raise RuntimeError("Resposta CKAN sem sucesso")

            return data["result"]["resources"]

        except Exception:
            if tentativa == 2:
                raise
            time.sleep(5)

# =========================
# Bloco 2 — Seleção do resource
# =========================

def selecionar_resource_mais_recente(resources):
    validos = [r for r in resources if r.get("url") and r.get("format")]
    if not validos:
        raise RuntimeError("Nenhum resource válido encontrado")
    return max(validos, key=lambda r: r.get("created", ""))

# =========================
# Bloco 3 — Download, hash e leitura
# =========================

def baixar_e_ler_resource(resource):
    url = resource["url"]

    for tentativa in range(3):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()

            conteudo = r.content
            hash_atual = calcular_hash_bytes(conteudo)
            hash_anterior = ler_hash_anterior()

            if hash_atual == hash_anterior:
                raise RuntimeError("Dado não sofreu alteração")

            # ===== LEITURA COM CORREÇÃO DE ENCODING =====
            if resource["format"].lower() == "zip":
                with zipfile.ZipFile(io.BytesIO(conteudo)) as z:
                    nome_csv = z.namelist()[0]
                    with z.open(nome_csv) as f:
                        df = pd.read_csv(
                            f,
                            sep=";",
                            encoding="latin1"
                        )
            else:
                df = pd.read_csv(
                    io.BytesIO(conteudo),
                    sep=";",
                    encoding="latin1"
                )

            # 🔧 CORREÇÃO DEFINITIVA DO MOJIBAKE NOS NOMES DAS COLUNAS
            df.columns = (
                df.columns
                .str.encode("latin1", errors="ignore")
                .str.decode("utf-8", errors="ignore")
            )

            salvar_hash(hash_atual)
            return df

        except RuntimeError:
            raise

        except Exception as e:
            if tentativa == 2:
                raise RuntimeError("Falha ao baixar resource") from e
            time.sleep(5)

# =========================
# Bloco 4 — Tratamento mínimo (Bronze)
# =========================

def tratar_minimo_bronze(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df

# =========================
# Bloco 5 — Persistência
# =========================

def salvar_bronze(df, resource):
    nome = resource.get("name", "frota_renavam")
    nome = nome.replace(" ", "_").lower()

    caminho = BRONZE_DIR / f"{nome}.parquet"
    df.to_parquet(caminho, index=False)

    return nome

# =========================
# Pipeline Bronze completo
# =========================

def executar_bronze():
    resources = buscar_resources_ckan()
    resource = selecionar_resource_mais_recente(resources)

    df = baixar_e_ler_resource(resource)
    df = tratar_minimo_bronze(df)

    nome_bronze = salvar_bronze(df, resource)
    return nome_bronze
