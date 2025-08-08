import os
import sys
import time
import json
import requests
import pandas as pd

API_KEY = os.getenv("CONTIFICO_API_KEY")  # <- la tomamos del Secret
if not API_KEY:
    print("ERROR: CONTIFICO_API_KEY no está definido como secret.")
    sys.exit(1)

HEADERS = {
    "Authorization": API_KEY,
    "Content-Type": "application/json"
}

URL_ASIENTOS = "https://api.contifico.com/sistema/api/v2/contabilidad/asiento?page={}"
URL_CUENTAS  = "https://api.contifico.com/sistema/api/v1/contabilidad/cuenta-contable/"

def fetch_asientos_paginado(sleep_secs=0.2, max_pages=5000):
    """Descarga todas las páginas hasta que 'results' venga vacío o falle."""
    page = 1
    dfs = []
    while page <= max_pages:
        url = URL_ASIENTOS.format(page)
        r = requests.get(url, headers=HEADERS, timeout=60)
        if r.status_code != 200:
            print(f"[WARN] Página {page} status={r.status_code} -> detengo.")
            break

        data = r.json()
        # data esperado: {"count":..., "next":..., "previous":..., "results":[...]}
        results = data.get("results", [])
        if not results:
            print(f"[INFO] Página {page} sin resultados -> fin.")
            break

        expanded_df = pd.json_normalize(results)

        # Expandir detalles y combinarlos
        if "detalles" in expanded_df.columns:
            df_detalles = expanded_df[["id", "fecha", "detalles"]].explode("detalles").dropna().reset_index(drop=True)
            det_norm = pd.json_normalize(df_detalles["detalles"])
            det_full = pd.concat([df_detalles[["id", "fecha"]], det_norm], axis=1)
            base = expanded_df.drop(columns=["detalles"])
            combined_df = pd.merge(base, det_full, on=["id", "fecha"], how="left")
        else:
            combined_df = expanded_df

        dfs.append(combined_df)
        print(f"[OK] Página {page} filas={len(combined_df)}")
        page += 1
        time.sleep(sleep_secs)  # respeta la API

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def fetch_cuentas():
    r = requests.get(URL_CUENTAS, headers=HEADERS, timeout=60)
    if r.status_code != 200:
        print(f"[WARN] Cuentas status={r.status_code} -> devuelvo vacío.")
        return pd.DataFrame()
    df = pd.DataFrame(r.json())
    if "id" in df.columns:
        df = df.rename(columns={"id": "cuenta_id"})
    return df

def main():
    asientos = fetch_asientos_paginado()
    cuentas  = fetch_cuentas()

    if asientos.empty:
        print("[ERROR] No se obtuvieron asientos. Abortando.")
        sys.exit(2)

    if not cuentas.empty:
        cols = [c for c in ["cuenta_id", "nombre", "codigo"] if c in cuentas.columns]
        asientos = asientos.merge(cuentas[cols], on="cuenta_id", how="left")

    # Ordena y limpia tipos mínimos
    if "fecha" in asientos.columns:
        # No convertir dtype para evitar problemas; Power BI lo puede inferir.
        asientos = asientos.sort_values("fecha", ascending=True)

    # Asegura carpeta data/
    os.makedirs("data", exist_ok=True)
    out_path = "data/salida.csv"
    asientos.to_csv(out_path, index=False)
    print(f"[DONE] Guardado: {out_path} filas={len(asientos)}")

if __name__ == "__main__":
    main()
