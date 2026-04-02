"""
CDC WONDER Natality Data Pull — Selected Variables
Database: Natality 2016-2024 Expanded (D149)

County-level (B_1 = County of Residence, D149.V21-level2):
  All variables except the four STI variables below.

State-level (B_1 = State of Residence, D149.V21-level1):
  gonorrhea, syphilis, chlamydia, hepatitis_c
  (county-level suppression is too heavy for these rare infections)

One year per request, 2016-2024.
Writes per-variable CSVs + combined CSV.
Prints suppression rate per variable; flags any >25%.

Run with:
    /Users/shoamoosavi/opt/anaconda3/bin/python3 cdc_wonder_natality_pull.py
"""

import csv
import os
import re
import time
import sys

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
    sys.exit("Run: pip install playwright && playwright install chromium")

# ── Configuration ─────────────────────────────────────────────────────────────
OUTPUT_DIR   = "natality_outputs"
YEARS        = list(range(2016, 2025))   # 2016–2024
DELAY_SECS   = 10
PAGE_TIMEOUT = 60_000
RESULT_WAIT  = 30_000

B1_COUNTY = "D149.V21-level2"
B1_STATE  = "D149.V21-level1"

# ── Scenarios ─────────────────────────────────────────────────────────────────
# b1: B_1 value (county or state); omit = county
SCENARIOS = [
    # Maternal characteristics
    {"label": "mothers_race_6cat",          "b2_value": "D149.V42",  "b2_name": "Mother's Single Race 6"},
    {"label": "mothers_hispanic",           "b2_value": "D149.V43",  "b2_name": "Mother's Hispanic Origin"},
    {"label": "mothers_age_9groups",        "b2_value": "D149.V1",   "b2_name": "Age of Mother 9"},
    # Pregnancy history & prenatal care
    {"label": "interval_last_other_preg",   "b2_value": "D149.V61",  "b2_name": "Interval Since Last Other Pregnancy Outcome"},
    {"label": "live_birth_order",           "b2_value": "D149.V28",  "b2_name": "Live Birth Order"},
    {"label": "prenatal_visits_count",      "b2_value": "D149.V64",  "b2_name": "Number of Prenatal Visits"},
    {"label": "prenatal_care_month",        "b2_value": "D149.V8",   "b2_name": "Month Prenatal Care Began"},
    # Maternal risk factors
    {"label": "prepreg_bmi",                "b2_value": "D149.V71",  "b2_name": "Mother's Pre-pregnancy BMI"},
    {"label": "weight_gain_recode",         "b2_value": "D149.V73",  "b2_name": "Mother's Weight Gain Recode"},
    {"label": "tobacco_use",                "b2_value": "D149.V10",  "b2_name": "Tobacco Use"},
    # Pregnancy risk factors
    {"label": "gestational_diabetes",       "b2_value": "D149.V75",  "b2_name": "Gestational Diabetes"},
    {"label": "gestational_hypertension",   "b2_value": "D149.V17",  "b2_name": "Gestational Hypertension"},
    {"label": "eclampsia",                  "b2_value": "D149.V18",  "b2_name": "Eclampsia"},
    {"label": "prev_preterm_birth",         "b2_value": "D149.V76",  "b2_name": "Previous Preterm Birth"},
    {"label": "infertility_treatment",      "b2_value": "D149.V77",  "b2_name": "Infertility Treatment Used"},
    {"label": "fertility_drugs",            "b2_value": "D149.V78",  "b2_name": "Fertility Enhancing Drugs"},
    {"label": "art",                        "b2_value": "D149.V79",  "b2_name": "Assistive Reproductive Technology"},
    {"label": "prev_cesarean",              "b2_value": "D149.V80",  "b2_name": "Previous Cesarean Delivery"},
    # Maternal infections — state level (too suppressed at county)
    {"label": "gonorrhea",                  "b2_value": "D149.V83",  "b2_name": "Gonorrhea",  "b1": B1_STATE},
    {"label": "syphilis",                   "b2_value": "D149.V84",  "b2_name": "Syphilis",   "b1": B1_STATE},
    {"label": "chlamydia",                  "b2_value": "D149.V85",  "b2_name": "Chlamydia",  "b1": B1_STATE},
    {"label": "hepatitis_c",                "b2_value": "D149.V87",  "b2_name": "Hepatitis C","b1": B1_STATE},
    # Labor characteristics
    {"label": "induction_of_labor",         "b2_value": "D149.V91",  "b2_name": "Induction of Labor"},
    {"label": "augmentation_of_labor",      "b2_value": "D149.V92",  "b2_name": "Augmentation of Labor"},
    {"label": "steroids_fetal_lung",        "b2_value": "D149.V93",  "b2_name": "Steroids"},
    # chorioamnionitis and anesthesia skipped — fully suppressed at county level
    # Delivery — where & method
    {"label": "birthplace",                 "b2_value": "D149.V45",  "b2_name": "Birthplace"},
    {"label": "fetal_presentation",         "b2_value": "D149.V98",  "b2_name": "Fetal Presentation"},
    {"label": "final_route_method",         "b2_value": "D149.V99",  "b2_name": "Final Route and Delivery Method"},
    {"label": "delivery_method_expanded",   "b2_value": "D149.V101", "b2_name": "Delivery Method Expanded"},
    {"label": "delivery_method_3cat",       "b2_value": "D149.V31",  "b2_name": "Delivery Method"},
    {"label": "payment_5cat",               "b2_value": "D149.V110", "b2_name": "Source of Payment for Delivery"},
    # Maternal morbidity
    {"label": "maternal_transfusion",       "b2_value": "D149.V102", "b2_name": "Maternal Transfusion"},
    {"label": "perineal_laceration",        "b2_value": "D149.V103", "b2_name": "Perineal Laceration"},
    {"label": "ruptured_uterus",            "b2_value": "D149.V104", "b2_name": "Ruptured Uterus"},
    {"label": "icu_admission",              "b2_value": "D149.V106", "b2_name": "Admission to Intensive Care Unit"},
    # Gestational age
    {"label": "gest_age_11g_oe",            "b2_value": "D149.V33",  "b2_name": "OE Gestational Age Recode 11"},
    # Infant characteristics
    {"label": "plurality",                  "b2_value": "D149.V7",   "b2_name": "Plurality"},
    {"label": "birth_weight_12groups",      "b2_value": "D149.V9",   "b2_name": "Infant Birth Weight 12"},
    # apgar_5min_single and apgar_10min_single skipped per user request
    {"label": "breastfed_at_discharge",     "b2_value": "D149.V138", "b2_name": "Infant Breastfed at Discharge"},
]

TOTAL = len(SCENARIOS)


# ── HTML Parser ───────────────────────────────────────────────────────────────
def strip_tags(html: str) -> str:
    return re.sub(r'<[^>]+>', '', html).strip()


def parse_results_html(html: str, label: str, b2_name: str, year: int) -> list[dict]:
    data_tbody = None
    for m in re.finditer(r'<tbody[^>]*>(.*?)</tbody>', html, re.DOTALL | re.IGNORECASE):
        content = m.group(1)
        if re.search(r'class=["\']v["\']', content, re.I):
            data_tbody = content
            break
    if not data_tbody:
        return []

    rows = []
    current_geo = ""
    current_subcol = ""

    for tr in re.finditer(r'<tr[^>]*>(.*?)</tr>', data_tbody, re.DOTALL | re.IGNORECASE):
        tr_content = tr.group(1)
        if re.search(r'class=["\']t["\']', tr_content, re.I):
            continue

        th_vals = [strip_tags(th) for th in
                   re.findall(r'<th[^>]*class=["\']v["\'][^>]*>(.*?)</th>',
                               tr_content, re.DOTALL | re.IGNORECASE)]
        td_vals = [strip_tags(td) for td in
                   re.findall(r'<td[^>]*>(.*?)</td>',
                               tr_content, re.DOTALL | re.IGNORECASE)]
        if not td_vals:
            continue

        if len(th_vals) == 3:
            current_geo   = th_vals[0]
            current_subcol = th_vals[1]
            category       = th_vals[2]
        elif len(th_vals) == 2:
            if current_geo and current_subcol:
                current_subcol = th_vals[0]
                category       = th_vals[1]
            else:
                current_geo = th_vals[0]
                category    = th_vals[1]
        elif len(th_vals) == 1:
            category = th_vals[0]
        else:
            continue

        births = td_vals[0].replace(",", "").strip()
        rows.append({
            "variable_label": label,
            "variable_name":  b2_name,
            "year":           str(year),
            "geo":            current_geo,
            "category":       category,
            "births":         births,
        })

    return rows


def suppression_rate(rows: list[dict]) -> float:
    """Return fraction of data rows where births == 'Suppressed'."""
    if not rows:
        return 0.0
    suppressed = sum(1 for r in rows if r["births"].lower() == "suppressed")
    return suppressed / len(rows)


# ── Single-year puller ────────────────────────────────────────────────────────
def pull_one_year(browser, b1_val: str, b2_val: str, year: int) -> list[dict]:
    context = browser.new_context()
    page    = context.new_page()
    rows    = []
    try:
        page.goto("https://wonder.cdc.gov/natality-expanded-current.html",
                  wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
        time.sleep(2)

        agree_btn = page.locator("input[value='I Agree']")
        if agree_btn.count() > 0:
            agree_btn.click()
            time.sleep(5)

        if page.locator(f"select[name='B_2'] option[value='{b2_val}']").count() == 0:
            return []

        page.locator("select[name='B_1']").select_option(value=b1_val)
        time.sleep(0.3)

        try:
            page.locator("select[name='B_2']").select_option(value=b2_val, timeout=15_000)
        except PWTimeout:
            return []
        time.sleep(0.3)

        year_ctrl = page.locator("select[name='V_D149.V20']")
        if year_ctrl.count() > 0:
            year_ctrl.evaluate(f"""sel => {{
                for (let o of sel.options) {{
                    o.selected = (o.value === '{year}');
                }}
            }}""")
            time.sleep(0.2)

        page.locator("#submit-button1").click()
        try:
            page.wait_for_function(
                "() => document.title.includes('Results') || document.title.includes('Error')",
                timeout=RESULT_WAIT
            )
        except PWTimeout:
            return []

        time.sleep(3)
        if "Results" not in page.title():
            return []

        rows = parse_results_html(page.content(), "", "", year)

    except Exception:
        rows = []
    finally:
        context.close()

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    done = {
        f.replace("natality_", "").replace(".csv", "")
        for f in os.listdir(OUTPUT_DIR)
        if f.startswith("natality_") and f.endswith(".csv")
        and f != "natality_ALL_combined.csv"
        and os.path.getsize(os.path.join(OUTPUT_DIR, f)) > 200
    }
    # Only count as done if file has a 'geo' column (current schema)
    valid_done = set()
    for label in done:
        path = os.path.join(OUTPUT_DIR, f"natality_{label}.csv")
        if os.path.exists(path):
            with open(path) as f:
                header = f.readline()
            if "geo" in header or "county" in header:
                valid_done.add(label)

    todo = [s for s in SCENARIOS if s["label"] not in valid_done]

    total_pulls = len(todo) * len(YEARS)
    est_min     = (total_pulls * (DELAY_SECS + 20)) // 60  # ~20s avg request time
    print("=" * 65)
    print("CDC WONDER Natality Pull — Selected Variables")
    print(f"  Total scenarios     : {TOTAL}")
    print(f"  Already done        : {len(valid_done)}")
    print(f"  Remaining           : {len(todo)}")
    print(f"  Years per scenario  : {len(YEARS)}  ({YEARS[0]}–{YEARS[-1]})")
    print(f"  Total requests      : {total_pulls}")
    print(f"  Estimated time      : ~{est_min} min  (~{est_min//60}h {est_min%60}m)")
    print(f"  Output dir          : {OUTPUT_DIR}/")
    print("=" * 65)

    suppression_flags: list[str] = []
    all_combined: list[dict] = []
    request_count = 0

    # Load already-done rows into combined
    for label in valid_done:
        path = os.path.join(OUTPUT_DIR, f"natality_{label}.csv")
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            all_combined.extend(reader)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)

        for s_idx, scenario in enumerate(todo):
            label   = scenario["label"]
            b2_val  = scenario["b2_value"]
            b2_name = scenario["b2_name"]
            b1_val  = scenario.get("b1", B1_COUNTY)
            geo_level = "state" if b1_val == B1_STATE else "county"
            out_csv = os.path.join(OUTPUT_DIR, f"natality_{label}.csv")

            print(f"\n[{s_idx+1}/{len(todo)}] {b2_name}  ({b2_val})  [{geo_level}]")

            scenario_rows: list[dict] = []

            for y_idx, year in enumerate(YEARS):
                if request_count > 0:
                    print(f"  Waiting {DELAY_SECS}s …", flush=True)
                    time.sleep(DELAY_SECS)
                request_count += 1

                print(f"  [{request_count:04d}] {year} ({y_idx+1}/{len(YEARS)}) … ",
                      end="", flush=True)

                try:
                    rows = pull_one_year(browser, b1_val, b2_val, year)
                except Exception as e:
                    print(f"ERROR: {e}")
                    rows = []

                if rows:
                    print(f"✓ {len(rows)} rows")
                    for r in rows:
                        r["variable_label"] = label
                        r["variable_name"]  = b2_name
                    scenario_rows.extend(rows)
                else:
                    print("no data / skipped")

            if scenario_rows:
                sup_rate = suppression_rate(scenario_rows)
                sup_pct  = f"{sup_rate*100:.1f}%"
                flag     = " ⚠️  HIGH SUPPRESSION" if sup_rate > 0.25 else ""
                if sup_rate > 0.25:
                    suppression_flags.append(f"{label} ({b2_name}): {sup_pct} suppressed")

                fieldnames = ["variable_label", "variable_name", "year",
                              "geo", "category", "births"]
                with open(out_csv, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(scenario_rows)
                print(f"  ✅ Saved {len(scenario_rows):,} rows → {out_csv}  "
                      f"[suppression: {sup_pct}]{flag}")
                all_combined.extend(scenario_rows)
            else:
                print(f"  ❌ No data for {label}")

        browser.close()

    # Combined CSV
    combined_path = os.path.join(OUTPUT_DIR, "natality_ALL_combined.csv")
    if all_combined:
        fields = ["variable_label", "variable_name", "year", "geo", "category", "births"]
        with open(combined_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_combined)
        print(f"\n✅ Combined CSV: {len(all_combined):,} rows → {combined_path}")

    print(f"\n{'='*65}")
    print(f"Done. {request_count} requests made.")

    if suppression_flags:
        print(f"\n⚠️  Variables with >25% suppressed rows ({len(suppression_flags)}):")
        for s in suppression_flags:
            print(f"  • {s}")
    else:
        print("\n✅ No variables exceeded 25% suppression threshold.")

    print(f"{'='*65}")


if __name__ == "__main__":
    main()
