from pathlib import Path
import pandas as pd
import unicodedata
import re

# =========================
# Caminhos base
# =========================

BASE_DIR = Path(__file__).resolve().parents[1]

BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
DIM_DIR = BASE_DIR / "data" / "dimensions"

SILVER_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# Schema esperado (contrato)
# =========================

COLUNAS_ESPERADAS = [
    "uf",
    "municipio",
    "montadora",
    "modelo",
    "ano_fabricacao",
    "quantidade_veiculos",
    "idade_veiculo"
]

# =========================
# Utilidades
# =========================

def remover_acentos(valor):
    if pd.isna(valor) or not isinstance(valor, str):
        return valor
    return "".join(
        c for c in unicodedata.normalize("NFKD", valor)
        if not unicodedata.combining(c)
    )

# =========================
# Leitura Bronze
# =========================

def ler_bronze(caminho_parquet: Path) -> pd.DataFrame:
    return pd.read_parquet(caminho_parquet)

# =========================
# Leitura dimensões
# =========================

def ler_dim_uf() -> pd.DataFrame:
    return pd.read_parquet(DIM_DIR / "dim_uf.parquet")

def ler_dim_montadoras() -> pd.DataFrame:
    return pd.read_parquet(DIM_DIR / "dim_montadoras.parquet")

# =========================
# Limpeza estrutural
# =========================

def limpar_estrutura_silver(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .dropna(axis=1, how="all")
        .dropna(axis=0, how="all")
    )

# =========================
# Padronização de textos
# =========================

def padronizar_textos(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include="object").columns:
        df[col] = (
            df[col]
            .where(df[col].notna())
            .astype(str)
            .str.strip()
            .str.upper()
            .apply(remover_acentos)
        )
    return df

# =========================
# Padronização de colunas
# =========================

def padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.upper()
        .map(remover_acentos)
        .str.replace(".", "", regex=False)
    )

    MAP_COLUNAS = {
        "UF": "uf",
        "MUNICIPIO": "municipio",
        "MARCA_MODELO": "marca_modelo",
        "ANO_FABRICACAO_VEICULO_CRV": "ano_fabricacao",
        "QTD_VEICULOS": "quantidade_veiculos"
    }

    df = df.rename(columns=MAP_COLUNAS)

    # validação defensiva de colunas base
    obrigatorias = {"uf", "municipio", "marca_modelo",
                    "ano_fabricacao", "quantidade_veiculos"}

    faltantes = obrigatorias - set(df.columns)
    if faltantes:
        raise RuntimeError(
            f"Falha ao mapear colunas obrigatórias: {faltantes}. "
            f"Colunas atuais: {df.columns.tolist()}"
        )

    return df

# =========================
# Aplicação da dim_uf
# =========================

def aplicar_dim_uf(df: pd.DataFrame, dim_uf: pd.DataFrame) -> pd.DataFrame:
    df = df.merge(
        dim_uf.query("ativo == True"),
        left_on="uf",
        right_on="uf_original",
        how="left",
        validate="many_to_one"
    )

    return (
        df
        .drop(columns=["uf", "uf_original"])
        .rename(columns={"uf_canonica": "uf"})
    )

# =========================
# Classificação de montadora
# =========================

def classificar_montadora(
    df: pd.DataFrame,
    dim_montadoras: pd.DataFrame
) -> pd.DataFrame:

    df["montadora"] = pd.NA

    for _, row in dim_montadoras.iterrows():
        padrao = row["padrao_match"]
        montadora = row["montadora_canonica"]

        mask = df["marca_modelo"].str.contains(
            rf"\b{padrao}\b",
            regex=True,
            na=False
        )

        df.loc[mask, "montadora"] = montadora

    # descarte consciente de não classificados
    return df[df["montadora"].notna()]

# =========================
# Extração de modelo
# =========================

def extrair_modelo(df: pd.DataFrame) -> pd.DataFrame:
    df["modelo"] = df.apply(
        lambda row: (
            re.sub(
                rf"\b{re.escape(row['montadora'])}\b",
                "",
                row["marca_modelo"]
            )
            .replace("/", " ")
            .strip()
        ),
        axis=1
    )

    df["modelo"] = df["modelo"].str.replace(
        r"\s+", " ", regex=True
    )

    # validação semântica forte
    if (df["modelo"].str.contains(df["montadora"], regex=False)).any():
        raise RuntimeError(
            "Contrato violado: coluna 'modelo' ainda contém montadora."
        )

    return df.drop(columns=["marca_modelo"])


# =========================
# Tratamento de tipos
# =========================

def tratar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    df["ano_fabricacao"] = pd.to_numeric(
        df["ano_fabricacao"], errors="coerce"
    )

    df["quantidade_veiculos"] = pd.to_numeric(
        df["quantidade_veiculos"], errors="coerce"
    )

    return df

# =========================
# Enriquecimento leve
# =========================

def enriquecer_silver(df: pd.DataFrame, ano_snapshot: int) -> pd.DataFrame:
    df["idade_veiculo"] = ano_snapshot - df["ano_fabricacao"]
    return df

# =========================
# Validação final de schema
# =========================

def validar_schema(df: pd.DataFrame) -> pd.DataFrame:
    faltantes = set(COLUNAS_ESPERADAS) - set(df.columns)
    if faltantes:
        raise ValueError(f"Colunas obrigatórias ausentes: {faltantes}")

    return df[COLUNAS_ESPERADAS]

# =========================
# Persistência
# =========================

def salvar_silver(df: pd.DataFrame, nome_arquivo: str) -> None:
    caminho = SILVER_DIR / f"{nome_arquivo}.parquet"
    df.to_parquet(caminho, index=False)

# =========================
# Pipeline Silver
# =========================

def executar_silver(
    nome_bronze: str,
    nome_silver: str,
    ano_snapshot: int
) -> None:

    caminho_bronze = BRONZE_DIR / f"{nome_bronze}.parquet"

    df = ler_bronze(caminho_bronze)
    df = limpar_estrutura_silver(df)
    df = padronizar_textos(df)
    df = padronizar_colunas(df)

    dim_uf = ler_dim_uf()
    dim_montadoras = ler_dim_montadoras()

    df = aplicar_dim_uf(df, dim_uf)
    df = classificar_montadora(df, dim_montadoras)
    df = extrair_modelo(df)

    df = tratar_tipos(df)
    df = enriquecer_silver(df, ano_snapshot)
    df = validar_schema(df)

    salvar_silver(df, nome_silver)
