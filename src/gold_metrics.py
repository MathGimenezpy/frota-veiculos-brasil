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

def ler_silver(nome_arquivo):
    caminho = SILVER_DIR / f"{nome_arquivo}.parquet"
    return pd.read_parquet(caminho)

# =========================
# 1️⃣ Perfil executivo por UF
# =========================

def perfil_frota_uf(df):
    return (
        df
        .groupby("uf")
        .agg(
            total_veiculos=("uf", "size"),
            idade_media=("idade_veiculo", "mean")
        )
        .reset_index()
    )

# =========================
# 2️⃣ Concentração de modelos (Top N)
# =========================

def concentracao_modelos(df, top_n=10):
    total = len(df)

    resumo = (
        df
        .groupby("modelo")
        .size()
        .reset_index(name="total_veiculos")
        .sort_values("total_veiculos", ascending=False)
    )

    resumo["participacao_pct"] = resumo["total_veiculos"] / total * 100
    return resumo.head(top_n)

# =========================
# 3️⃣ Penetração de montadoras
# =========================

def penetracao_montadoras(df):
    total = len(df)

    return (
        df
        .groupby("montadora")
        .size()
        .reset_index(name="total_veiculos")
        .assign(
            participacao_pct=lambda x: x["total_veiculos"] / total * 100
        )
        .sort_values("participacao_pct", ascending=False)
    )

# =========================
# 4️⃣ Penetração de modelos
# =========================

def penetracao_modelos(df):
    total = len(df)

    return (
        df
        .groupby("modelo")
        .size()
        .reset_index(name="total_veiculos")
        .assign(
            participacao_pct=lambda x: x["total_veiculos"] / total * 100
        )
        .sort_values("participacao_pct", ascending=False)
    )

# =========================
# 5️⃣ Maturidade municipal
# =========================

def maturidade_municipal(df):
    return (
        df
        .groupby(["uf", "municipio"])
        .agg(
            total_veiculos=("municipio", "size"),
            idade_media=("idade_veiculo", "mean")
        )
        .reset_index()
        .sort_values("idade_media", ascending=False)
    )

# =========================
# 6️⃣ Proxy de emplacamento
# =========================

def proxy_emplacamento(df_atual, df_anterior):
    atual = (
        df_atual
        .groupby("uf")
        .size()
        .reset_index(name="frota_atual")
    )

    anterior = (
        df_anterior
        .groupby("uf")
        .size()
        .reset_index(name="frota_anterior")
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

def salvar_gold(df, nome):
    caminho = GOLD_DIR / f"{nome}.parquet"
    df.to_parquet(caminho, index=False)

# =========================
# Pipeline Gold completo
# =========================

def executar_gold(nome_silver_atual, nome_silver_anterior=None):
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

    municipios = maturidade_municipal(df_atual)
    salvar_gold(municipios, "maturidade_municipal")
    outputs["maturidade_municipal"] = "maturidade_municipal.parquet"

    if nome_silver_anterior:
        df_anterior = ler_silver(nome_silver_anterior)
        proxy = proxy_emplacamento(df_atual, df_anterior)
        salvar_gold(proxy, "proxy_emplacamento")
        outputs["proxy_emplacamento"] = "proxy_emplacamento.parquet"

    return outputs
