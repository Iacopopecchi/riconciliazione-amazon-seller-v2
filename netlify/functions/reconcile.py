"""
Amazon Seller Central Reconciliation – Netlify Function (Python 3.11)

Riceve file CSV e PDF via multipart/form-data e restituisce JSON
con 4 check di riconciliazione + settlement periods.
"""

import json
import base64
import io
import re
import traceback
from email.parser import BytesParser


# ──────────────────────────────────────────────────────────────────────────────
# CORS / helpers
# ──────────────────────────────────────────────────────────────────────────────

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}

MESI_IT = {
    "gen": 1, "feb": 2, "mar": 3, "apr": 4,
    "mag": 5, "giu": 6, "lug": 7, "ago": 8,
    "set": 9, "ott": 10, "nov": 11, "dic": 12,
}

MESI_NOMI = {
    1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
    5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
    9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre",
}

NUMERIC_COLS = [
    "Vendite",
    "imposta sulle vendite dei prodotti",
    "Commissioni di vendita",
    "Altri costi relativi alle transazioni",
    "Altro",
    "totale",
]


def _resp(data, status=200):
    return {
        "statusCode": status,
        "headers": {**CORS_HEADERS, "Content-Type": "application/json; charset=utf-8"},
        "body": json.dumps(data, ensure_ascii=False, default=str),
    }


def _err(msg, status=400):
    return _resp({"error": msg}, status)


# ──────────────────────────────────────────────────────────────────────────────
# HANDLER
# ──────────────────────────────────────────────────────────────────────────────

def handler(event, context):
    try:
        if event.get("httpMethod") == "OPTIONS":
            return {"statusCode": 204, "headers": CORS_HEADERS, "body": ""}

        raw_headers = event.get("headers") or {}
        headers = {k.lower(): v for k, v in raw_headers.items()}
        content_type = headers.get("content-type", "")

        body = event.get("body") or ""
        if event.get("isBase64Encoded", False):
            body_bytes = base64.b64decode(body)
        elif isinstance(body, str):
            body_bytes = body.encode("latin-1")
        else:
            body_bytes = body

        files = _parse_multipart(body_bytes, content_type)

        if not files.get("csv_target"):
            return _err("CSV del mese target obbligatorio non caricato.")
        if not files.get("pdf_summary"):
            return _err("PDF Summary obbligatorio non caricato.")

        import pandas as pd  # noqa – imported here to surface import errors clearly
        import pdfplumber    # noqa

        df_target = _parse_csv(files["csv_target"])
        df_prev   = _parse_csv(files["csv_prev"])   if files.get("csv_prev")   else None
        df_next   = _parse_csv(files["csv_next"])   if files.get("csv_next")   else None

        pdf_sum = _parse_pdf_summary(files["pdf_summary"])
        if pdf_sum is None:
            return _err(
                "Impossibile leggere il PDF Summary. "
                "Assicurati che sia il file scaricato da Report → Pagamenti → Archivio Report."
            )

        pdf_ads_eur = None
        if files.get("pdf_ads"):
            pdf_ads_eur = _parse_pdf_ads(files["pdf_ads"])

        mese_target = _month_label(df_target)
        checks      = _compute_checks(df_target, pdf_sum, pdf_ads_eur)
        periods     = _compute_periods(df_target, df_prev, df_next)

        ricavi = checks["ricavi"]["csv"]
        spese  = checks["spese"]["csv"]
        trasf  = checks["trasferimenti"]["csv"]
        imposte = pdf_sum.get("imposte") or 0.0
        saldo  = round(ricavi + imposte + spese + trasf, 2)

        pass_list = [
            checks["ricavi"]["pass"],
            checks["spese"]["pass"],
            checks["trasferimenti"]["pass"],
        ]
        if checks["ads"]["disponibile"]:
            pass_list.append(checks["ads"]["pass"])

        if all(pass_list):
            status_globale = "OK"
        elif any(p is False for p in pass_list):
            status_globale = "ATTENZIONE"
        else:
            status_globale = "ERRORE"

        open_periods = [p for p in periods if p.get("transfer_amount") is None]

        warnings = []
        if df_prev is None and periods:
            first = periods[0]
            if first.get("note") == "Aperto nel mese precedente" or first.get("transfer_amount") is None:
                warnings.append(
                    f"Carica anche il CSV del mese precedente per verificare "
                    f"il settlement period {first['periodo_id']}"
                )

        result = {
            "mese_target": mese_target,
            "status_globale": status_globale,
            "warnings": warnings,
            "checks": checks,
            "saldo_residuo": {
                "importo": saldo,
                "ricavi": round(ricavi, 2),
                "imposte": round(imposte, 2),
                "spese": round(spese, 2),
                "trasferimenti": round(trasf, 2),
                "spiegazione": _saldo_explanation(saldo, open_periods, ricavi, imposte, spese, trasf),
            },
            "settlement_periods": periods,
            "pdf_summary_raw": {
                "ricavi": pdf_sum.get("ricavi"),
                "spese": pdf_sum.get("spese"),
                "imposte": pdf_sum.get("imposte"),
                "trasferimenti": pdf_sum.get("trasferimenti"),
            },
            "transazioni": _df_records(df_target),
        }

        return _resp(result)

    except Exception as exc:
        return _resp({"error": str(exc), "traceback": traceback.format_exc()}, 500)


# ──────────────────────────────────────────────────────────────────────────────
# MULTIPART PARSING
# ──────────────────────────────────────────────────────────────────────────────

def _parse_multipart(body_bytes, content_type):
    """Parse multipart/form-data → {field_name: bytes}"""
    m = re.search(r"boundary=([^\s;]+)", content_type, re.IGNORECASE)
    if not m:
        return {}
    boundary = m.group(1).strip("\"'")

    prefix = (
        f"MIME-Version: 1.0\r\n"
        f'Content-Type: multipart/form-data; boundary="{boundary}"\r\n'
        f"\r\n"
    ).encode()

    msg = BytesParser().parsebytes(prefix + body_bytes)
    result = {}
    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue
        cd = part.get("Content-Disposition", "")
        nm = re.search(r'name=["\']?([^;"\']+)["\']?', cd)
        if nm:
            data = part.get_payload(decode=True)
            if data:
                result[nm.group(1)] = data
    return result


# ──────────────────────────────────────────────────────────────────────────────
# CSV PARSING
# ──────────────────────────────────────────────────────────────────────────────

def _it_num(val):
    """Convert Italian number string to float. '1.548,61' → 1548.61"""
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s or s in ("-", "—", "nan", ""):
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _it_date(s):
    """Parse Italian dates: '14 gen 2026' or '14/01/2026'"""
    import pandas as pd
    if not s or str(s).strip() in ("", "nan", "NaT"):
        return None
    s = str(s).strip()
    parts = s.split()
    if len(parts) == 3:
        key = parts[1].lower()[:3]
        if key in MESI_IT:
            try:
                return pd.Timestamp(year=int(parts[2]), month=MESI_IT[key], day=int(parts[0]))
            except Exception:
                pass
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y"):
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            pass
    try:
        return pd.to_datetime(s, dayfirst=True)
    except Exception:
        return None


def _parse_csv(file_bytes):
    """
    Parse Amazon Italy CSV:
      - utf-8-sig (BOM) encoding
      - Righe 0-6: descrittive (skip)
      - Riga 7: intestazione colonne
      - Numeri in formato italiano (virgola decimale, punto migliaia)
    """
    import pandas as pd

    raw = None
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            raw = file_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if raw is None:
        raise ValueError("Impossibile decodificare il CSV. Salvalo in UTF-8 e riprova.")

    lines = raw.splitlines()
    if len(lines) < 9:
        raise ValueError(f"CSV troppo corto ({len(lines)} righe). Verifica il file.")

    header_line = lines[7]
    sep = "\t" if header_line.count("\t") >= header_line.count(",") else ","

    data_str = "\n".join(lines[7:])
    df = pd.read_csv(io.StringIO(data_str), sep=sep, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].apply(_it_num)
        else:
            df[col] = 0.0

    date_col = next(
        (c for c in df.columns if c.lower() in ("data", "date")), None
    )
    if date_col:
        df["_date"]     = df[date_col].apply(_it_date)
        df["_date_str"] = df[date_col]
    else:
        df["_date"]     = None
        df["_date_str"] = ""

    if "Numero pagamento" in df.columns:
        df["Numero pagamento"] = df["Numero pagamento"].apply(
            lambda x: str(x).strip().split(".")[0] if str(x) != "nan" else ""
        )

    return df


# ──────────────────────────────────────────────────────────────────────────────
# PDF PARSING
# ──────────────────────────────────────────────────────────────────────────────

def _pdf_text(pdf_bytes):
    import pdfplumber
    chunks = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                chunks.append(t)
    return "\n".join(chunks)


def _parse_pdf_summary(pdf_bytes):
    """
    Estrae Ricavi, Spese, Imposte, Trasferimenti dal PDF Summary Amazon Italy.
    Strategia: cerca parola-chiave sulla riga, prende l'ultimo numero sulla stessa riga.
    """
    try:
        text = _pdf_text(pdf_bytes)
    except Exception as e:
        raise ValueError(f"Errore lettura PDF: {e}")

    if not text.strip():
        return None

    keywords = {
        "ricavi":         ["ricavi", "vendite.*accrediti"],
        "spese":          ["spese", "costi.*inclusivi"],
        "imposte":        ["imposte.*nette", "imposte"],
        "trasferimenti":  ["trasferimenti", "versamenti.*prelievi"],
    }

    result = {}
    lines = text.splitlines()

    for key, kw_list in keywords.items():
        for kw in kw_list:
            for line in lines:
                if re.search(kw, line, re.IGNORECASE):
                    # Find last number on the line
                    nums = re.findall(r"[+-]?\s*[\d.,]+", line)
                    if nums:
                        # Pick the last number that contains , or is >3 chars (looks like currency)
                        for num in reversed(nums):
                            n = num.replace(" ", "")
                            if "," in n or len(n) > 3:
                                result[key] = _it_num(n)
                                break
                if key in result:
                    break
            if key in result:
                break

    # Fallback: regex across full text (number at end of line after keyword)
    fallback = {
        "ricavi":        r"Ricavi[^\n]*?([\d.,]+)\s*\n",
        "spese":         r"Spese[^\n]*?([\d.,]+)\s*\n",
        "imposte":       r"Imposte[^\n]*?([\d.,]+)\s*\n",
        "trasferimenti": r"Trasferimenti[^\n]*?([\d.,]+)\s*\n",
    }
    for key, pat in fallback.items():
        if key not in result:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                result[key] = _it_num(m.group(1))

    for key in ["ricavi", "spese", "imposte", "trasferimenti"]:
        result.setdefault(key, None)

    if all(v is None for v in result.values()):
        return None

    return result


def _parse_pdf_ads(pdf_bytes):
    """Estrae l'importo EUR Italy da una fattura ADS Amazon."""
    try:
        text = _pdf_text(pdf_bytes)
    except Exception:
        return None

    patterns = [
        r"Italy\s+Subtotal[^\n]*?[\d,]*\.?\d+\s+USD\s*/\s*([\d,]*\.?\d+)\s+EUR",
        r"Italy[^\n]*Total[^\n]*?[\d,]*\.?\d+\s+USD\s*/\s*([\d,]*\.?\d+)\s+EUR",
        r"Total\s+Invoice\s+Amount[^\n]*?([\d,]*\.?\d+)\s+EUR",
        r"([\d,]*\.?\d+)\s+EUR",
    ]

    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE | re.DOTALL)
        if m:
            try:
                return -abs(float(m.group(1).replace(",", "")))
            except ValueError:
                continue

    return None


# ──────────────────────────────────────────────────────────────────────────────
# 4 CHECK
# ──────────────────────────────────────────────────────────────────────────────

def _compute_checks(df_target, pdf_sum, pdf_ads_eur):
    import pandas as pd

    tipo_col = "Tipo" if "Tipo" in df_target.columns else None

    # CHECK 1 — Ricavi
    ricavi_csv = round(df_target["Vendite"].sum(), 2)
    ricavi_pdf = pdf_sum.get("ricavi")
    d1 = round(ricavi_csv - ricavi_pdf, 2) if ricavi_pdf is not None else None

    # CHECK 2 — Spese
    non_tr = df_target[df_target[tipo_col] != "Trasferimento"].copy() if tipo_col else df_target.copy()
    spese_csv = round(
        df_target["Commissioni di vendita"].sum()
        + df_target["Altri costi relativi alle transazioni"].sum()
        + non_tr["Altro"].sum(),
        2,
    )
    spese_pdf = pdf_sum.get("spese")
    d2 = round(spese_csv - spese_pdf, 2) if spese_pdf is not None else None

    # CHECK 3 — Trasferimenti
    if tipo_col:
        tr_df = df_target[df_target[tipo_col] == "Trasferimento"]
    else:
        tr_df = pd.DataFrame(columns=df_target.columns)

    trasf_csv = round(tr_df["totale"].sum(), 2) if not tr_df.empty else 0.0
    trasf_pdf = pdf_sum.get("trasferimenti")
    d3 = round(trasf_csv - trasf_pdf, 2) if trasf_pdf is not None else None

    detail = []
    if not tr_df.empty:
        for _, row in tr_df.iterrows():
            d = row.get("_date")
            ds = row.get("_date_str", "")
            date_fmt = d.strftime("%d/%m/%Y") if hasattr(d, "strftime") else str(ds)
            detail.append({"data": date_fmt, "importo": round(float(row["totale"]), 2)})

    # CHECK 4 — ADS
    if tipo_col and "Descrizione" in df_target.columns:
        ads_df = df_target[
            df_target["Descrizione"].astype(str).str.strip() == "Costo della pubblicità"
        ]
    else:
        ads_df = pd.DataFrame(columns=df_target.columns)

    ads_csv = round(ads_df["totale"].sum(), 2) if not ads_df.empty else 0.0
    d4 = round(ads_csv - pdf_ads_eur, 2) if pdf_ads_eur is not None else None

    return {
        "ricavi": {
            "csv": ricavi_csv, "pdf": ricavi_pdf, "differenza": d1,
            "pass": bool(abs(d1) <= 0.05) if d1 is not None else False,
        },
        "spese": {
            "csv": spese_csv, "pdf": spese_pdf, "differenza": d2,
            "pass": bool(abs(d2) <= 0.05) if d2 is not None else False,
        },
        "trasferimenti": {
            "csv": trasf_csv, "pdf": trasf_pdf, "differenza": d3,
            "pass": bool(abs(d3) <= 0.05) if d3 is not None else False,
            "dettaglio": detail,
        },
        "ads": {
            "csv": ads_csv, "pdf": pdf_ads_eur, "differenza": d4,
            "pass": bool(abs(d4) <= 1.00) if d4 is not None else None,
            "disponibile": pdf_ads_eur is not None,
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# SETTLEMENT PERIODS
# ──────────────────────────────────────────────────────────────────────────────

def _compute_periods(df_target, df_prev, df_next):
    import pandas as pd

    frames = []
    if df_prev is not None:
        tmp = df_prev.copy(); tmp["_src"] = "prev"; frames.append(tmp)
    tgt = df_target.copy(); tgt["_src"] = "target"; frames.append(tgt)
    if df_next is not None:
        tmp = df_next.copy(); tmp["_src"] = "next"; frames.append(tmp)

    all_df = pd.concat(frames, ignore_index=True)

    if "Numero pagamento" not in all_df.columns:
        return []

    tipo_col = "Tipo" if "Tipo" in all_df.columns else None
    periods = {}

    for _, row in all_df.iterrows():
        pid = str(row["Numero pagamento"]).strip()
        if not pid or pid in ("nan", "", "0"):
            continue

        is_tr = tipo_col and str(row.get(tipo_col, "")).strip() == "Trasferimento"

        if pid not in periods:
            periods[pid] = {
                "periodo_id": pid,
                "dates": [],
                "tx_sum": 0.0,
                "transfer_amount": None,
                "transfer_date": None,
                "n_transazioni": 0,
                "first_src": row.get("_src", "target"),
            }

        d = row.get("_date")
        if d is not None and pd.notna(d):
            periods[pid]["dates"].append(d)

        if is_tr:
            periods[pid]["transfer_amount"] = round(float(row["totale"]), 2)
            periods[pid]["transfer_date"]   = d
        else:
            periods[pid]["tx_sum"] = round(periods[pid]["tx_sum"] + float(row["totale"]), 2)
            periods[pid]["n_transazioni"] += 1

    def _first(p):
        return min(p["dates"]) if p["dates"] else pd.Timestamp("2099-01-01")

    result = []
    for p in sorted(periods.values(), key=_first):
        dates = p["dates"]
        di = min(dates).strftime("%d/%m/%Y") if dates else ""
        df_ = max(dates).strftime("%d/%m/%Y") if dates else ""
        td = p["transfer_date"]
        td_str = td.strftime("%d/%m/%Y") if hasattr(td, "strftime") and td is not None and pd.notna(td) else ""

        tx = round(p["tx_sum"], 2)
        ta = p["transfer_amount"]
        diff = round(tx + ta, 2) if ta is not None else None

        note = ""
        if p["first_src"] == "prev":
            note = "Aperto nel mese precedente"
        elif ta is None:
            note = "Si chiude nel mese successivo"

        result.append({
            "periodo_id":     p["periodo_id"],
            "data_inizio":    di,
            "data_fine":      df_,
            "n_transazioni":  p["n_transazioni"],
            "tx_sum":         tx,
            "transfer_amount": ta,
            "transfer_date":  td_str,
            "differenza":     diff,
            "note":           note,
        })

    return result


# ──────────────────────────────────────────────────────────────────────────────
# ALTRI HELPER
# ──────────────────────────────────────────────────────────────────────────────

def _month_label(df):
    if "_date" not in df.columns:
        return "Mese target"
    dates = df["_date"].dropna()
    if dates.empty:
        return "Mese target"
    counts = {}
    for d in dates:
        k = (d.year, d.month)
        counts[k] = counts.get(k, 0) + 1
    year, month = max(counts, key=counts.get)
    return f"{MESI_NOMI.get(month, '')} {year}"


def _saldo_explanation(saldo, open_periods, ricavi, imposte, spese, trasf):
    parts = []
    if abs(saldo) < 0.10:
        parts.append(
            "Saldo su conto Amazon sostanzialmente zero: "
            "tutti i pagamenti del mese sono stati trasferiti."
        )
    else:
        parts.append(
            f"A fine mese rimangono {saldo:,.2f}€ ancora sul conto Amazon "
            "(non ancora trasferiti). Questo è normale."
        )
        for p in open_periods:
            parts.append(
                f"Il periodo {p['periodo_id']} si chiude il mese successivo: "
                f"il bonifico di {p['tx_sum']:,.2f}€ è atteso."
            )
    return " ".join(parts)


def _df_records(df):
    cols = [
        "_date_str", "Tipo", "Numero pagamento", "Numero ordine",
        "Descrizione", "Vendite", "Commissioni di vendita",
        "Altri costi relativi alle transazioni", "Altro", "totale",
    ]
    avail = [c for c in cols if c in df.columns]
    records = []
    for _, row in df[avail].iterrows():
        rec = {}
        for c in avail:
            display = "Data" if c == "_date_str" else c
            v = row[c]
            rec[display] = "" if str(v) in ("nan", "NaT", "None") else v
        records.append(rec)
    return records
