from datetime import datetime
from pathlib import Path

from .bronze_ingestion_ckan import executar_bronze
from .silver_processing import executar_silver
from .gold_metrics import executar_gold

# =========================
# Paths
# =========================

BASE_DIR = Path(__file__).resolve().parents[1]
SILVER_DIR = BASE_DIR / "data" / "silver"

# =========================
# Utilidades
# =========================

def gerar_nome_silver(base: str, ano_snapshot: int) -> str:
    mes_execucao = datetime.now().strftime("%m")
    return f"{base}_{ano_snapshot}_{mes_execucao}"

def descobrir_silver_anterior(silver_atual: str) -> str | None:
    arquivos = sorted(p.stem for p in SILVER_DIR.glob("*.parquet"))
    anteriores = [a for a in arquivos if a < silver_atual]
    return anteriores[-1] if anteriores else None

def validar_reexecucao(silver_nome: str):
    caminho = SILVER_DIR / f"{silver_nome}.parquet"
    if caminho.exists():
        raise RuntimeError(
            f"Snapshot já existe: {silver_nome}. Reexecução bloqueada."
        )

# =========================
# Orquestrador
# =========================

def main(
    ano_snapshot: int,
    silver_base: str
):
    # 1️⃣ Bronze (hash decide)
    try:
        bronze_nome = executar_bronze()

    except RuntimeError as e:
        if "Dado não sofreu alteração" in str(e):
            print("Dado não mudou. Pipeline encerrado.")
            return
        raise

    # 2️⃣ Nome do snapshot Silver
    silver_nome_atual = gerar_nome_silver(
        base=silver_base,
        ano_snapshot=ano_snapshot
    )

    # 3️⃣ Proteção secundária
    validar_reexecucao(silver_nome_atual)

    # 4️⃣ Snapshot anterior
    silver_nome_anterior = descobrir_silver_anterior(silver_nome_atual)

    # 5️⃣ Silver
    executar_silver(
        nome_bronze=bronze_nome,
        nome_silver=silver_nome_atual,
        ano_snapshot=ano_snapshot
    )

    # 6️⃣ Gold
    outputs_gold = executar_gold(
        nome_silver_atual=silver_nome_atual,
        nome_silver_anterior=silver_nome_anterior
    )

    return {
        "ano_snapshot": ano_snapshot,
        "bronze_nome": bronze_nome,
        "silver_atual": silver_nome_atual,
        "silver_anterior": silver_nome_anterior,
        "outputs_gold": outputs_gold,
        "executado_em": datetime.now().isoformat()
    }

# =========================
# Execução
# =========================

if __name__ == "__main__":
    main(
        ano_snapshot=datetime.now().year,
        silver_base="frota_renavam"
    )
