from pathlib import Path
import pandas as pd

# =========================
# Caminhos base
# =========================

BASE_DIR = Path(__file__).resolve().parents[1]
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

GOLD_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# Leitura Silver
# =========================

def ler_silver(nome_arquivo: str) -> pd.DataFrame:
    caminho = SILVER_DIR / f"{nome_arquivo}.parquet"
    return pd.read_parquet(caminho)

# =========================
# 1️ Perfil executivo por UF
# =========================

def perfil_frota_uf(df: pd.DataFrame) -> pd.DataFrame:
    resumo = (
        df
        .groupby("uf", as_index=False)
        .agg(
            total_veiculos=("quantidade_veiculos", "sum"),
            soma_idade_ponderada=(
                "idade_veiculo",
                lambda x: (x * df.loc[x.index, "quantidade_veiculos"]).sum()
            )
        )
    )

    resumo["idade_media"] = (
        resumo["soma_idade_ponderada"] / resumo["total_veiculos"]
    )

    return resumo.drop(columns="soma_idade_ponderada")

# =========================
# 2️ Concentração de modelos (Top N)
# =========================

def concentracao_modelos(df: pd.DataFrame) -> pd.DataFrame:
    resumo = (
        df
        .groupby("modelo", as_index=False)["quantidade_veiculos"]
        .sum()
        .rename(columns={"quantidade_veiculos": "total_veiculos"})
        .sort_values("total_veiculos", ascending=False)
        .reset_index(drop=True)
    )

    # Ranking explícito (ordem do Pareto)
    resumo["ranking_modelo"] = resumo.index + 1

    # Total geral
    total_geral = resumo["total_veiculos"].sum()

    # Participação acumulada (Pareto)
    resumo["participacao_pct"] = (
        resumo["total_veiculos"].cumsum() / total_geral
    )

    return resumo

# =========================
# 3️ Penetração de montadoras
# =========================

def penetracao_montadoras(df: pd.DataFrame) -> pd.DataFrame:
    total = df["quantidade_veiculos"].sum()

    return (
        df
        .groupby("montadora", as_index=False)["quantidade_veiculos"]
        .sum()
        .rename(columns={"quantidade_veiculos": "total_veiculos"})
        .assign(
            participacao_pct=lambda x: x["total_veiculos"] / total
        )
        .sort_values("participacao_pct", ascending=False)
    )

# =========================
# 4️ Penetração de modelos
# =========================

def penetracao_modelos(df: pd.DataFrame) -> pd.DataFrame:
    total = df["quantidade_veiculos"].sum()

    return (
        df
        .groupby("modelo", as_index=False)["quantidade_veiculos"]
        .sum()
        .rename(columns={"quantidade_veiculos": "total_veiculos"})
        .assign(
            participacao_pct=lambda x: x["total_veiculos"] / total
        )
        .sort_values("participacao_pct", ascending=False)
    )

# =========================
# 5️ Maturidade municipal
# =========================

def maturidade_municipal(df: pd.DataFrame) -> pd.DataFrame:
    resumo = (
        df
        .groupby(["uf", "municipio"], as_index=False)
        .agg(
            total_veiculos=("quantidade_veiculos", "sum"),
            soma_idade_ponderada=(
                "idade_veiculo",
                lambda x: (x * df.loc[x.index, "quantidade_veiculos"]).sum()
            )
        )
    )

    resumo["idade_media"] = (
        resumo["soma_idade_ponderada"] / resumo["total_veiculos"]
    )

    return resumo.drop(columns="soma_idade_ponderada") \
                 .sort_values("idade_media", ascending=False)

# =========================
# 6️ Proxy de emplacamento (UF)
# =========================

def proxy_emplacamento(
    df_atual: pd.DataFrame,
    df_anterior: pd.DataFrame
) -> pd.DataFrame:

    atual = (
        df_atual
        .groupby("uf", as_index=False)["quantidade_veiculos"]
        .sum()
        .rename(columns={"quantidade_veiculos": "frota_atual"})
    )

    anterior = (
        df_anterior
        .groupby("uf", as_index=False)["quantidade_veiculos"]
        .sum()
        .rename(columns={"quantidade_veiculos": "frota_anterior"})
    )

    return (
        atual
        .merge(anterior, on="uf", how="left")
        .assign(
            frota_anterior=lambda x: x["frota_anterior"].fillna(0),
            entrada_liquida=lambda x: x["frota_atual"] - x["frota_anterior"]
        )
    )

# =========================
# Persistência Gold
# =========================

def salvar_gold(df: pd.DataFrame, nome: str) -> None:
    caminho = GOLD_DIR / f"{nome}.parquet"
    df.to_parquet(caminho, index=False)

# =========================
# Pipeline Gold completo
# =========================

def executar_gold(
    nome_silver_atual: str,
    nome_silver_anterior: str | None = None
) -> dict:

    df_atual = ler_silver(nome_silver_atual)
    outputs = {}

    perfil = perfil_frota_uf(df_atual)
    salvar_gold(perfil, "perfil_frota_uf")
    outputs["perfil_frota_uf"] = "perfil_frota_uf.parquet"

    concentracao = concentracao_modelos(df_atual)
    salvar_gold(concentracao, "concentracao_modelos")
    outputs["concentracao_modelos"] = "concentracao_modelos.parquet"

    pen_mont = penetracao_montadoras(df_atual)
    salvar_gold(pen_mont, "penetracao_montadoras")
    outputs["penetracao_montadoras"] = "penetracao_montadoras.parquet"

    pen_mod = penetracao_modelos(df_atual)
    salvar_gold(pen_mod, "penetracao_modelos")
    outputs["penetracao_modelos"] = "penetracao_modelos.parquet"

    maturidade = maturidade_municipal(df_atual)
    salvar_gold(maturidade, "maturidade_municipal")
    outputs["maturidade_municipal"] = "maturidade_municipal.parquet"

    if nome_silver_anterior:
        df_anterior = ler_silver(nome_silver_anterior)
        proxy = proxy_emplacamento(df_atual, df_anterior)
        salvar_gold(proxy, "proxy_emplacamento")
        outputs["proxy_emplacamento"] = "proxy_emplacamento.parquet"

    return outputs
