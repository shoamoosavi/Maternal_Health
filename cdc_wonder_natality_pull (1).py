"""
CDC WONDER Natality Data Pull — Multi-Scenario
Database: Natality 2016-2024 Expanded (D149)
Group By 1 (fixed): County of Residence
Group By 2 (varies): Defined per scenario in SCENARIOS list below
Filters by: Year (one pull per year, 2016-2024)
Output: One CSV per scenario, saved to the OUTPUT_DIR folder

CDC API docs: https://wonder.cdc.gov/wonder/help/wonder-api.html
NOTE: CDC asks that automated queries be spaced ~2 minutes apart.

────────────────────────────────────────────────────────────────
HOW TO ADD A NEW SCENARIO
────────────────────────────────────────────────────────────────
1. Look up the parameter code for your variable:
     a. Go to https://wonder.cdc.gov/natality-expanded-current.html
     b. Build a query using that variable as "And By" (Group By 2)
     c. Click Send → then click "API Options" on the results page
     d. Find the <value> inside the <n>B_2</n> parameter block
        It will look like "D149.Vxxx"

2. Add an entry to the SCENARIOS list below:
     {
       "label":    "short_name_for_filename",   # used in output filename
       "b2_code":  "D149.Vxxx",                 # the code from step 1d
       "b2_name":  "Human Readable Name",       # for log output only
     }
────────────────────────────────────────────────────────────────

KNOWN PARAMETER CODES (D149 — Natality 2016-2024 Expanded)
  County of Residence         → D149.V9    (always used as B_1)
  Year                        → D149.V20   (used as year filter)
  Delivery Method Expanded    → D149.V116
  Payment Method              → D149.V143  (source of payment / insurance)
  Gestational Diabetes        → D149.V130
  Gestational Age (weeks)     → D149.V13
  Mother's Age                → D149.V3
  Mother's Race (6 cats)      → D149.V2
  Mother's Hispanic Origin    → D149.V274
  Maternal Education          → D149.V5
  Tobacco Use                 → D149.V142
  Pre-pregnancy BMI           → D149.V209
  Plurality                   → D149.V12
  Sex of Infant               → D149.V11

  ⚠️  Always verify codes via "API Options" before running —
      codes can shift when CDC releases a new data vintage.
"""

import requests
import time
import csv
import os
import xml.etree.ElementTree as ET

# ── Global Configuration ──────────────────────────────────────────────────────
DATABASE_ID = "D149"
API_URL     = f"https://wonder.cdc.gov/controller/datarequest/{DATABASE_ID}"
YEARS       = list(range(2016, 2025))   # 2016–2024
OUTPUT_DIR  = "natality_outputs"        # folder where CSVs are saved
DELAY_SECS  = 120                       # 2 min between requests (CDC guidance)

# ── Scenario Definitions ──────────────────────────────────────────────────────
# Add, remove, or comment out rows to control which pulls are executed.
# Each scenario produces one CSV: natality_{label}_2016_2024.csv
SCENARIOS = [
    {
        "label":   "delivery_method_expanded",
        "b2_code": "D149.V116",
        "b2_name": "Delivery Method Expanded",
    },
    {
        "label":   "payment_method",
        "b2_code": "D149.V143",
        "b2_name": "Payment Method (Insurance)",
    },
    {
        "label":   "gestational_diabetes",
        "b2_code": "D149.V130",
        "b2_name": "Gestational Diabetes",
    },
    # ── Add more scenarios below this line ────────────────────────────────────
    # {
    #     "label":   "gestational_age",
    #     "b2_code": "D149.V13",
    #     "b2_name": "Gestational Age (weeks)",
    # },
    # {
    #     "label":   "mothers_age",
    #     "b2_code": "D149.V3",
    #     "b2_name": "Mother's Age",
    # },
    # {
    #     "label":   "tobacco_use",
    #     "b2_code": "D149.V142",
    #     "b2_name": "Tobacco Use",
    # },
]

# ── XML Builder ───────────────────────────────────────────────────────────────
def build_xml(year: int, b2_code: str) -> str:
    """Build the XML POST body for one (year, group-by) combination."""
    return f"""<request-parameters>
  <parameter>
    <n>accept_datause_restrictions</n>
    <value>true</value>
  </parameter>
  <parameter>
    <n>B_1</n>
    <value>D149.V9</value>
  </parameter>
  <parameter>
    <n>B_2</n>
    <value>{b2_code}</value>
  </parameter>
  <parameter>
    <n>B_3</n>
    <value>*None*</value>
  </parameter>
  <parameter>
    <n>B_4</n>
    <value>*None*</value>
  </parameter>
  <parameter>
    <n>F_D149.V20</n>
    <value>{year}</value>
  </parameter>
  <parameter>
    <n>O_V9_fmode</n>
    <value>frGroupBy</value>
  </parameter>
  <parameter>
    <n>O_show_totals</n>
    <value>false</value>
  </parameter>
  <parameter>
    <n>O_show_suppressed</n>
    <value>true</value>
  </parameter>
  <parameter>
    <n>M_1</n>
    <value>D149.M1</value>
  </parameter>
</request-parameters>"""


# ── Response Parser ───────────────────────────────────────────────────────────
def parse_response(xml_text: str, year: int, scenario_label: str) -> list[dict]:
    """Parse CDC WONDER XML response into a list of row dicts."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"    [!] XML parse error ({scenario_label}, {year}): {e}")
        return []

    # Surface any API-level error messages
    for m in root.findall(".//m"):
        text = m.text or ""
        if m.get("c", "").startswith("D") or "error" in text.lower():
            print(f"    [!] API message ({scenario_label}, {year}): {text}")

    # Column headers
    headers = [h.get("l", h.text or "") for h in root.findall(".//h-rows/r/th")]
    if not headers:
        print(f"    [!] No headers found ({scenario_label}, {year}). Check parameter codes.")
        _save_debug(xml_text, scenario_label, year)
        return []

    # Data rows
    rows = []
    for row in root.findall(".//data-table/r"):
        record = {"Year": year}
        for i, cell in enumerate(row.findall("c")):
            col = headers[i] if i < len(headers) else f"col_{i}"
            record[col] = cell.get("l") or cell.get("v") or ""
        rows.append(record)

    return rows


def _save_debug(xml_text: str, label: str, year: int):
    """Save raw XML to a debug file for inspection."""
    path = os.path.join(OUTPUT_DIR, f"debug_{label}_{year}.xml")
    with open(path, "w", encoding="utf-8") as f:
        f.write(xml_text)
    print(f"    Raw response saved → {path}")


# ── Per-Scenario Runner ───────────────────────────────────────────────────────
def run_scenario(scenario: dict, total_pull_count: list) -> None:
    """
    Execute all yearly pulls for one scenario and write a combined CSV.
    total_pull_count is a mutable list used to track cross-scenario
    request counts for rate-limiting purposes.
    """
    label   = scenario["label"]
    b2_code = scenario["b2_code"]
    b2_name = scenario["b2_name"]
    out_csv = os.path.join(OUTPUT_DIR, f"natality_{label}_2016_2024.csv")

    print(f"\n{'═'*60}")
    print(f"  Scenario: {b2_name}  [{b2_code}]")
    print(f"  Output  : {out_csv}")
    print(f"{'═'*60}")

    all_rows    = []
    all_columns = set()

    for i, year in enumerate(YEARS):
        # Rate-limit: always pause before every request except the very first
        if total_pull_count[0] > 0:
            print(f"  Waiting {DELAY_SECS}s (CDC rate limit)...")
            time.sleep(DELAY_SECS)

        total_pull_count[0] += 1
        print(f"  [{total_pull_count[0]:02d}] Pulling {year}  ({i+1}/{len(YEARS)})...", end=" ", flush=True)

        try:
            response = requests.post(
                API_URL,
                data={
                    "request_xml": build_xml(year, b2_code),
                    "accept_datause_restrictions": "true",
                },
                timeout=60,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"FAILED — {e}")
            continue

        rows = parse_response(response.text, year, label)
        if rows:
            print(f"✓ {len(rows)} rows")
            all_rows.extend(rows)
            all_columns.update(rows[0].keys())
        else:
            print("no data")
            _save_debug(response.text, label, year)

    # Write CSV
    if all_rows:
        col_order = ["Year"] + sorted(c for c in all_columns if c != "Year")
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=col_order, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\n  ✅ Saved {len(all_rows)} rows → {out_csv}")
    else:
        print(f"\n  ❌ No data collected for scenario '{label}'.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    total_requests = len(SCENARIOS) * len(YEARS)
    est_minutes    = (total_requests * DELAY_SECS) // 60
    print(f"CDC WONDER Natality Pull — Multi-Scenario")
    print(f"  Scenarios : {len(SCENARIOS)}")
    print(f"  Years     : {YEARS[0]}–{YEARS[-1]}  ({len(YEARS)} pulls each)")
    print(f"  Total     : {total_requests} API requests")
    print(f"  Est. time : ~{est_minutes} minutes  (2 min/request, CDC rate limit)")
    print(f"  Output dir: {OUTPUT_DIR}/")

    pull_counter = [0]   # mutable so run_scenario can update it
    for scenario in SCENARIOS:
        run_scenario(scenario, pull_counter)

    print(f"\n{'═'*60}")
    print(f"All done! {pull_counter[0]} requests made.")
    print(f"CSVs saved in: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
