"""
scraper.py — Afro Automation: Cost Data Scraper
================================================
Scrapes project-cost journal entries from the Afro ERP system,
downloads one CSV per project, and returns a combined DataFrame.

Public API
----------
    df = scrape_cost_data(date_from="01/01/2026", date_to="02/28/2026")

Environment variables (override hard-coded defaults)
-----------------------------------------------------
    AFRO_USER      ERP username          (default: motaha)
    AFRO_PASS      ERP password          (default: G00gleG00gle)
    AFRO_ACCESS    Access-code prompt    (default: 123456)
"""

# ── Imports ───────────────────────────────────────────────────────────────────
import logging
import os
import shutil
import time
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
ERP_USER   = os.environ.get("AFRO_USER",   "motaha")
ERP_PASS   = os.environ.get("AFRO_PASS",   "G00gleG00gle")
ERP_ACCESS = os.environ.get("AFRO_ACCESS", "123456")

LOGIN_URL = (
    "https://sys.afro-group.com/Afroerp/Layout/Dashboard/operation/"
    "work%20order/Work_Order.php"
)

WAIT_TIMEOUT      = 60    # seconds — default WebDriverWait timeout
TABLE_WAIT        = 120   # seconds — extra patience for the results table to render
DOWNLOAD_TIMEOUT  = 30    # seconds — max wait for a CSV to appear on disk
PAGE_LOAD_PAUSE   = 5     # seconds — pause after heavy page actions


# ── Internal helpers ──────────────────────────────────────────────────────────

def _wait_for_file(path: Path, timeout: int = DOWNLOAD_TIMEOUT) -> bool:
    """
    Poll until *path* exists on disk AND has no matching .part file
    (Firefox writes filename.csv.part while downloading).
    Returns True when the file is fully written, False on timeout.
    """
    part_file = path.with_suffix(path.suffix + ".part")
    for _ in range(timeout):
        if path.exists() and not part_file.exists():
            return True
        time.sleep(1)
    return False


def _login(
    driver: webdriver.Firefox,
    wait: WebDriverWait,
    username: str,
    password: str,
    access_code: str,
) -> None:
    """
    Step 1 — Log in to the ERP and enter the access code.
    Credentials are passed explicitly so the caller controls them
    (UI, env vars, or CLI — scraper does not decide).
    """
    log.info("Navigating to login page …")
    driver.get(LOGIN_URL)

    wait.until(EC.presence_of_element_located((By.NAME, "user"))).send_keys(username)
    driver.find_element(By.NAME, "pass").send_keys(password)
    driver.find_element(By.NAME, "login").click()

    # ERP redirects to the previous URL after login; go back to get the access form
    driver.back()

    wait.until(EC.presence_of_element_located((By.NAME, "accesspass"))).send_keys(access_code)
    driver.find_element(By.NAME, "access").click()

    log.info("Login successful.")


def _navigate_to_project_cost(driver: webdriver.Firefox, wait: WebDriverWait) -> None:
    """
    Step 2 — Open Budget ▸ Project Cost from the sidebar.
    Tries text-based XPath first (robust), falls back to position-based
    XPath from the original code if the menu text differs.
    """
    log.info("Opening Budget menu …")

    # Try text-match first, then positional fallback from original code
    budget_xpath = (
        "//ul/li/a[contains(text(),'Budget') or contains(text(),'budget')] | "
        "//div/ul/li[8]/a"
    )
    budget_menu = wait.until(EC.element_to_be_clickable((By.XPATH, budget_xpath)))
    budget_menu.click()
    time.sleep(1)

    log.info("Clicking Project Cost …")
    cost_xpath = (
        "//ul/li/a[contains(text(),'Project Cost') or contains(text(),'project cost')] | "
        "//div/ul/li[8]/ul/li[2]/a"
    )
    project_cost = wait.until(EC.element_to_be_clickable((By.XPATH, cost_xpath)))
    project_cost.click()
    time.sleep(PAGE_LOAD_PAUSE)


def _select_all_projects_and_dates(
    driver: webdriver.Firefox,
    wait: WebDriverWait,
    date_from: str,
    date_to: str,
) -> None:
    """
    Step 3 — Select ALL projects in the multi-select dropdown,
    fill in the date range, and execute the query.
    """
    log.info("Selecting all projects …")

    # Open the bootstrap-select dropdown — try multiple selector patterns
    dropdown_xpath = (
        "//form//div[contains(@class,'bootstrap-select')]//button | "
        "//form//div[contains(@class,'dropdown')]//button | "
        "/html/body/div[3]/div/div[2]/div[2]/div[1]/div[2]/form/div[1]/div[1]/div/div/button"
    )
    dropdown_btn = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath)))
    dropdown_btn.click()
    time.sleep(1)

    # Click "Select All" — first item in the open dropdown list
    select_all_xpath = (
        "//ul[contains(@class,'dropdown-menu')]//li[1]//label | "
        "//div[contains(@class,'dropdown-menu')]//li[1]//label | "
        "/html/body/div[3]/div/div[2]/div[2]/div[1]/div[2]/form/div[1]/div[1]/div/div/ul/li[1]/a/label"
    )
    select_all = wait.until(EC.element_to_be_clickable((By.XPATH, select_all_xpath)))
    select_all.click()
    time.sleep(1)

    log.info("Setting date range: %s → %s", date_from, date_to)
    from_field = driver.find_element(By.NAME, "from")
    from_field.clear()
    from_field.send_keys(date_from)

    to_field = driver.find_element(By.NAME, "to")
    to_field.clear()
    to_field.send_keys(date_to)

    log.info("Executing query …")
    execute_btn = driver.find_element(By.NAME, "import")
    execute_btn.click()
    # Brief pause then let _show_100_rows wait for the actual table


def _show_100_rows(driver: webdriver.Firefox, wait: WebDriverWait) -> None:
    """
    Step 4 — Wait for the results table to appear, then switch page-length
    to 100 rows so all project links are visible.

    Uses TABLE_WAIT (120s) for the initial table appearance because the ERP
    server can take 60-90 seconds to render query results.
    """
    log.info("Waiting for results table to load (up to %ds) …", TABLE_WAIT)

    # Use a dedicated longer-wait object just for this step
    long_wait = WebDriverWait(driver, TABLE_WAIT)

    # Wait for table to appear — try exact ID then CSS fallback
    try:
        long_wait.until(EC.presence_of_element_located((By.ID, "DataTables_Table_0")))
    except TimeoutException:
        log.warning("DataTables_Table_0 not found by ID — trying CSS fallback.")
        long_wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "table[id*='DataTables']")
            )
        )

    log.info("Table is present — switching to 100 rows per page …")

    # Now run the JS to expand to 100 rows
    driver.execute_script("""
        var sel = document.querySelector('select[name="DataTables_Table_0_length"]');
        if (sel) {
            sel.value = '100';
            sel.dispatchEvent(new Event('change', { bubbles: true }));
        }
    """)

    # Wait for DataTables processing spinner to clear (if it exists)
    try:
        wait.until(
            EC.invisibility_of_element_located(
                (By.CSS_SELECTOR, "#DataTables_Table_0_processing")
            )
        )
    except TimeoutException:
        pass  # no spinner on this page — that's fine

    time.sleep(2)  # brief settle time after DataTable re-renders
    log.info("DataTable set to 100 rows per page.")


def _collect_link_hrefs(driver: webdriver.Firefox, wait: WebDriverWait) -> list:
    """
    Step 5a — Collect all href URLs from the results table WITHOUT clicking.
    Collecting hrefs first avoids StaleElementReferenceException that would
    occur if we clicked links while still holding references to the row elements.
    """
    # Table is guaranteed present by _show_100_rows — direct find is safe here
    try:
        table = driver.find_element(By.ID, "DataTables_Table_0")
    except NoSuchElementException:
        table = driver.find_element(By.CSS_SELECTOR, "table[id*='DataTables']")
    rows = table.find_elements(By.TAG_NAME, "tr")

    hrefs = []
    for row in rows[1:]:   # skip header
        try:
            cells = row.find_elements(By.TAG_NAME, "td")
            for cell in cells:
                links = cell.find_elements(By.TAG_NAME, "a")
                for link in links:
                    href = link.get_attribute("href")
                    if href and href not in hrefs:
                        hrefs.append(href)
        except StaleElementReferenceException:
            log.warning("Stale row reference — skipping one row during href collection.")
            continue

    log.info("Collected %d unique project link(s).", len(hrefs))
    return hrefs


def _open_project_tabs(driver: webdriver.Firefox, hrefs: list) -> None:
    """
    Step 5b — Open each collected href in a new tab via JavaScript.
    Using window.open() avoids the StaleElementReferenceException that
    clicking stale <a> elements would cause.
    """
    for href in hrefs:
        driver.execute_script("window.open(arguments[0], '_blank');", href)
        time.sleep(0.8)   # brief pause so tabs don't pile up too fast

    log.info("Opened %d tab(s).", len(hrefs))
    time.sleep(3)   # let all tabs finish initial load


def _wait_for_new_csv(
    download_dir: Path,
    existing: set,
    timeout: int = DOWNLOAD_TIMEOUT,
) -> Path | None:
    """
    Wait until a NEW .csv file appears in *download_dir* that was not in
    *existing*, is fully written (no matching .part file), and is not empty.

    This replaces the hard-coded 'AFROerp.csv' wait so it works regardless of
    what filename the ERP chooses for the download.

    Returns the Path of the new file, or None on timeout.
    """
    for _ in range(timeout):
        current = set(download_dir.glob("*.csv"))
        new_files = current - existing
        for f in new_files:
            part = f.with_suffix(f.suffix + ".part")
            if f.exists() and not part.exists() and f.stat().st_size > 0:
                return f
        time.sleep(1)
    return None


def _download_csvs(
    driver: webdriver.Firefox,
    wait: WebDriverWait,
    download_dir: Path,
    output_dir: Path,
) -> list:
    """
    Step 6 — Iterate every tab that was opened, download the CSV,
    rename it to the project name, and move it to *output_dir*.

    FIX (original bug): `if(i==w): break` compared int to list → never broke.
    Now correctly iterates with `range(1, len(handles))`.

    FIX: waits for .part file to disappear before moving (prevents partial reads).
    FIX: detects new CSV by filename diff (not hard-coded 'AFROerp.csv') so the
         ERP's descriptive filenames are handled correctly.
    FIX: skips duplicate saves — if output already exists, discards re-download
         instead of saving _i copy, preventing dataset inflation.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    saved  = []
    failed = []

    handles = driver.window_handles   # snapshot after all tabs are open

    for i in range(1, len(handles)):  # index 0 = main results tab
        try:
            driver.switch_to.window(handles[i])
            time.sleep(2)

            # Read project name from the page header
            try:
                header = driver.find_element(
                    By.XPATH,
                    "/html/body/div[3]/div[1]/div/div[2]/div[1]/div[2]"
                )
                project_name = header.text.strip() or f"project_{i}"
            except NoSuchElementException:
                project_name = f"project_{i}"

            # Sanitise name for use as a filename
            # '.' is allowed so Dep. / G.A. names stay intact and match the category map
            safe_name = "".join(
                c if c.isalnum() or c in " -_()." else "_"
                for c in project_name
            ).strip()

            log.info("  Tab %d/%d — %s", i, len(handles) - 1, safe_name)

            # Click CSV export button — try class-based, then text-based, then absolute XPath
            csv_xpath = (
                "//a[contains(@class,'buttons-csv')] | "
                "//a/span[normalize-space(text())='CSV']/.. | "
                "/html/body/div[3]/div[1]/div/div[2]/div[1]/div[3]/div[1]/div[2]/a[3]/span/.."
            )
            try:
                csv_btn = wait.until(EC.element_to_be_clickable((By.XPATH, csv_xpath)))
                csv_btn.click()
            except TimeoutException:
                log.warning("    CSV button not found on tab %d — skipping.", i)
                failed.append(safe_name)
                continue

            # Wait for the ERP CSV to land in the download folder.
            # The ERP can name the file either 'AFROerp.csv' (old) or a full
            # descriptive name like 'Record of monthly Journal Entry … _.csv'.
            # Strategy: snapshot existing CSVs, then wait for a NEW one to appear.
            existing_csvs = set(download_dir.glob("*.csv"))
            raw_file = _wait_for_new_csv(download_dir, existing_csvs)
            if raw_file is None:
                log.warning("    Download timed out for '%s'.", safe_name)
                failed.append(safe_name)
                continue

            # Move & rename into output folder
            # If a file with the SAME category name already exists, skip the
            # duplicate rather than saving a second copy (_i suffix caused
            # the dataset to inflate on repeated scrape runs).
            dest = output_dir / f"{safe_name}.csv"
            if dest.exists():
                log.info("    Skipping duplicate — '%s' already exists.", dest.name)
                raw_file.unlink(missing_ok=True)   # discard the re-download
                saved.append(str(dest))
                continue
            shutil.move(str(raw_file), str(dest))
            log.info("    Saved → %s", dest.name)
            saved.append(str(dest))

        except Exception as exc:
            log.error("    Unexpected error on tab %d: %s", i, exc)
            failed.append(f"tab_{i}")

    if failed:
        log.warning("Failed to download %d project(s): %s", len(failed), failed)

    return saved


def _load_csvs_to_dataframe(folder: Path) -> pd.DataFrame:
    """
    Step 7 — Read every CSV in *folder* and concatenate into one DataFrame.

    Adds a 'Source.Name' column with the originating filename, then deduplicates
    rows so repeated scrape runs (or browser re-downloads of the same file) never
    inflate the dataset.

    Deduplication strategy
    ----------------------
    When the same category file is downloaded more than once the ERP names the
    second copy with a Windows-style suffix ( (1), (2) … ).  After concatenating
    all frames we drop any row whose (JE No. + Project + JE Date + Debit + Credit)
    combination has already been seen, keeping the first occurrence.
    """
    frames = []
    for csv_file in sorted(folder.glob("*.csv")):
        try:
            df = pd.read_csv(csv_file, encoding="utf-8-sig")
            df["Source.Name"] = csv_file.name
            frames.append(df)
            log.info("  Loaded: %s  (%d rows)", csv_file.name, len(df))
        except Exception as exc:
            log.error("  Could not read %s: %s", csv_file.name, exc)

    if not frames:
        log.warning("No CSV files found in %s", folder)
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    raw_count = len(combined)

    # ── Deduplicate ───────────────────────────────────────────────────────────
    # Key columns that uniquely identify a journal-entry line.
    dedup_cols = [c for c in ["JE No.", "Project", "JE Date", "Debit", "Credit"]
                  if c in combined.columns]
    if dedup_cols:
        combined = combined.drop_duplicates(subset=dedup_cols, keep="first")
        dropped = raw_count - len(combined)
        if dropped:
            log.warning(
                "Removed %d duplicate row(s) — %d unique rows remain. "
                "Tip: clear the output folder before each scrape run.",
                dropped, len(combined),
            )

    log.info("Combined DataFrame: %d rows × %d columns", *combined.shape)
    return combined


# ── Public API ────────────────────────────────────────────────────────────────

def scrape_cost_data(
    date_from:   str = "01/01/2026",
    date_to:     str = "02/28/2026",
    output_dir:  str = None,
    username:    str = None,
    password:    str = None,
    access_code: str = None,
) -> pd.DataFrame:
    """
    Run the full scraping pipeline and return a combined cost DataFrame.

    Parameters
    ----------
    date_from    : str   Date range start  (MM/DD/YYYY)
    date_to      : str   Date range end    (MM/DD/YYYY)
    output_dir   : str   Folder to store CSV files.
                         Defaults to ~/Downloads/Cost_MM-YYYY
    username     : str   ERP username  (falls back to AFRO_USER env var)
    password     : str   ERP password  (falls back to AFRO_PASS env var)
    access_code  : str   ERP access code (falls back to AFRO_ACCESS env var)

    Returns
    -------
    pd.DataFrame  Combined data from all downloaded CSVs, or empty
                  DataFrame if nothing was downloaded.
    """
    # Resolve credentials: parameter → env var → hard-coded default
    _user   = username    or ERP_USER
    _pass   = password    or ERP_PASS
    _access = access_code or ERP_ACCESS

    download_dir = Path.home() / "Downloads"

    if output_dir:
        out_dir = Path(output_dir)
    else:
        try:
            parts = date_from.split("/")           # ["01", "01", "2026"]
            month_tag = f"{parts[0]}-{parts[2]}"   # "01-2026"
        except Exception:
            month_tag = "scraped"
        out_dir = download_dir / f"Cost_{month_tag}"

    log.info("=" * 60)
    log.info("scrape_cost_data  %s → %s", date_from, date_to)
    log.info("Output folder: %s", out_dir)
    log.info("=" * 60)

    driver = webdriver.Firefox()
    wait   = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        _login(driver, wait, _user, _pass, _access)
        _navigate_to_project_cost(driver, wait)
        _select_all_projects_and_dates(driver, wait, date_from, date_to)
        _show_100_rows(driver, wait)
        hrefs = _collect_link_hrefs(driver, wait)   # collect first, then open
        _open_project_tabs(driver, hrefs)            # open all tabs safely
        _download_csvs(driver, wait, download_dir, out_dir)

    except Exception as exc:
        log.error("Scraping failed: %s", exc)
        raise

    finally:
        driver.quit()
        log.info("Browser closed.")

    return _load_csvs_to_dataframe(out_dir)


# ── CLI entry point ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Afro ERP cost scraper")
    parser.add_argument("--from", dest="date_from", default="01/01/2026",
                        help="Start date MM/DD/YYYY (default: 01/01/2026)")
    parser.add_argument("--to",   dest="date_to",   default="02/28/2026",
                        help="End date   MM/DD/YYYY (default: 02/28/2026)")
    parser.add_argument("--output", dest="output_dir", default=None,
                        help="Folder to save CSVs (optional)")
    args = parser.parse_args()

    cost_df = scrape_cost_data(args.date_from, args.date_to, args.output_dir)

    if not cost_df.empty:
        print(f"\nScraping complete — {len(cost_df)} rows loaded.")
        print(cost_df.head())
    else:
        print("\nNo data was scraped.")
