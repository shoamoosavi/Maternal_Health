"""
One-off: pull Number of Prenatal Visits at state level, all years 2016-2024.
Overwrites natality_outputs/natality_prenatal_visits_count.csv with state-level data.
"""
import csv, os, re, time, sys
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

OUTPUT_DIR   = "natality_outputs"
YEARS        = list(range(2016, 2025))
DELAY_SECS   = 10
PAGE_TIMEOUT = 60_000
RESULT_WAIT  = 30_000
B1_STATE     = "D149.V21-level1"
B2_VALUE     = "D149.V64"
B2_NAME      = "Number of Prenatal Visits"
LABEL        = "prenatal_visits_count"

def strip_tags(html):
    return re.sub(r'<[^>]+>', '', html).strip()

def parse_results_html(html, year):
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
        th_vals = [strip_tags(th) for th in re.findall(r'<th[^>]*class=["\']v["\'][^>]*>(.*?)</th>', tr_content, re.DOTALL | re.IGNORECASE)]
        td_vals = [strip_tags(td) for td in re.findall(r'<td[^>]*>(.*?)</td>', tr_content, re.DOTALL | re.IGNORECASE)]
        if not td_vals:
            continue
        if len(th_vals) == 3:
            current_geo, current_subcol, category = th_vals
        elif len(th_vals) == 2:
            if current_geo and current_subcol:
                current_subcol, category = th_vals
            else:
                current_geo, category = th_vals
        elif len(th_vals) == 1:
            category = th_vals[0]
        else:
            continue
        rows.append({"variable_label": LABEL, "variable_name": B2_NAME,
                     "year": str(year), "geo": current_geo,
                     "category": category, "births": td_vals[0].replace(",","").strip()})
    return rows

def pull_one_year(browser, year):
    context = browser.new_context()
    page    = context.new_page()
    rows    = []
    try:
        page.goto("https://wonder.cdc.gov/natality-expanded-current.html",
                  wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
        time.sleep(2)
        agree = page.locator("input[value='I Agree']")
        if agree.count() > 0:
            agree.click()
            time.sleep(5)
        if page.locator(f"select[name='B_2'] option[value='{B2_VALUE}']").count() == 0:
            return []
        page.locator("select[name='B_1']").select_option(value=B1_STATE)
        time.sleep(0.3)
        try:
            page.locator("select[name='B_2']").select_option(value=B2_VALUE, timeout=15_000)
        except PWTimeout:
            return []
        time.sleep(0.3)
        year_ctrl = page.locator("select[name='V_D149.V20']")
        if year_ctrl.count() > 0:
            year_ctrl.evaluate(f"sel => {{ for(let o of sel.options) o.selected=(o.value==='{year}'); }}")
            time.sleep(0.2)
        page.locator("#submit-button1").click()
        try:
            page.wait_for_function(
                "() => document.title.includes('Results') || document.title.includes('Error')",
                timeout=RESULT_WAIT)
        except PWTimeout:
            return []
        time.sleep(3)
        if "Results" not in page.title():
            return []
        rows = parse_results_html(page.content(), year)
    except Exception:
        rows = []
    finally:
        context.close()
    return rows

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_rows = []
    print(f"Pulling {B2_NAME} at STATE level, {YEARS[0]}-{YEARS[-1]}")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        for i, year in enumerate(YEARS):
            if i > 0:
                print(f"  Waiting {DELAY_SECS}s...")
                time.sleep(DELAY_SECS)
            print(f"  [{i+1}/9] {year} ... ", end="", flush=True)
            rows = pull_one_year(browser, year)
            if rows:
                print(f"✓ {len(rows)} rows")
                all_rows.extend(rows)
            else:
                print("no data")
        browser.close()

    out = os.path.join(OUTPUT_DIR, f"natality_{LABEL}.csv")
    if all_rows:
        fields = ["variable_label","variable_name","year","geo","category","births"]
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\n✅ Saved {len(all_rows):,} rows → {out}")
    else:
        print("\n❌ No data collected.")

if __name__ == "__main__":
    main()
