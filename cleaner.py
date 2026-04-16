"""
cleaner.py — Afro Automation: Cost Data Cleaner
================================================
Cleans, enriches, and categorises the raw cost DataFrame
produced by scraper.scrape_cost_data().

Public API
----------
    from cleaner import clean_cost_data

    clean_df = clean_cost_data(raw_df)

Steps performed
---------------
1. Drop unused columns  (#, Details)
2. Extract expense item name from Source.Name
3. Parse JE Date → Year / month / Quarter columns
4. Parse numeric columns (remove commas, cast to float)
5. Fill Credit NaN → 0, compute Cost amount = Debit − Credit
6. Map item → Category  (with LG-number fallback)
7. Map Project → Account
8. Compute % share of total Cost amount
9. Warn about any unmapped items / projects
"""

import logging

import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────
log = logging.getLogger(__name__)

# ── Mappings ──────────────────────────────────────────────────────────────────

CATEGORY_MAP: dict[str, str] = {
    "Accomodation":                          "HR",
    "Bank charge":                           "Finance Cost",
    "Bonus":                                 "HR",
    "Bounce":                                "HR",
    "Car Rent":                              "Fleet",
    "Cars Maintenances":                     "Fleet",
    "Com LG 395496":                         "Finance Cost",
    "Com LG 397110":                         "Finance Cost",
    "Com LG 400244":                         "Finance Cost",
    "Com LG 97775":                          "Finance Cost",
    "Computer Supplies":                     "Tools",
    "Covid-19 test":                         "HR",
    "Crane Expenses":                        "Logistic",
    "CS I- Contracting Social Insurance":    "Finance Cost",
    "Customer Requirements":                 "Customer Expenses",
    "Dep. Installation Tools":               "Tools",
    "Dep. Installalation Tolls":             "Tools",   # typo variant
    "Dep_ Installation Tools":               "Tools",   # scraper-sanitised variant
    "Dep. Safety Tools":                     "Safety",
    "Dep. Safty Tools":                      "Safety",  # typo variant
    "Dep_ Safety Tools":                     "Safety",  # scraper-sanitised variant
    "Dep.Laptop":                            "IT",
    "Dep. laptop":                           "IT",      # ERP variant (space + lowercase)
    "Dep_ laptop":                           "IT",      # scraper-sanitised variant
    "Driver allowance":                      "Fleet",
    "Driver courses":                        "Fleet",
    "Electricity":                           "Other Expenses",
    "Entertainment":                         "G&A",
    "Equipment Rent":                        "Tools",
    "Flat Office rent":                      "Flat Office rent",
    "Fleet fees":                            "Fleet",
    "Freight Expenses":                      "Transportation",
    "G.A-Commission LG 354065":              "Finance Cost",
    "G.A-Commission LG 354066":              "Finance Cost",
    "G.A-Commission LG 362016":              "Finance Cost",
    "G.A-Marketing Expenses":               "G&A",
    "G.A-Medical Insurances Allocation":     "G&A",
    "G.A-Salaries and Wages":               "G&A",
    "G.A-Social Insurances":                "G&A",
    "G.A-Web site design and development":  "G&A",
    "G.A-Printing":                         "G&A",        # new in 2026 data
    "G.A-Transportation":                   "G&A",        # new in 2026 data
    "G_A-Printing":                         "G&A",        # scraper-sanitised variant
    "G_A-Transportation":                   "G&A",        # scraper-sanitised variant
    "Gas":                                   "Other Expenses",
    "Government expenses":                   "Finance Cost",
    "Housekeeping":                          "Other Expenses",
    "Insurance Expenses":                    "Tools",
    "KPIs":                                  "HR",
    "Labour":                                "Logistic",
    "Local workforce":                       "Local workforce",
    "Logistic Fees":                         "Logistic",
    "Lubricant and Fuel":                    "Fleet",
    "Maintenance":                           "Other Expenses",
    "Materials":                             "Material",
    "Meals":                                 "Customer Expenses",
    "Medical Check - Analysis":              "Safety",
    "Medical Expenses":                      "HR",
    "Medical Insurance Allocation":          "HR",
    "Accident insurance":                    "Safety",
    "Airline tickets":                       "Transportation",
    "Mobile Charges":                        "HR",
    "opex expenses":                         "Other Expenses",
    "Operating Supplies":                    "Material",
    "Operation -Other revenue":              "Operation -Other revenue",
    "Operation -afro lab other revenue":     "Operation -Other revenue",
    "Other Expenses":                        "Other Expenses",
    "Parking and Garage":                    "Fleet",
    "Penalties":                             "Penalties",
    "Printing":                              "Other Expenses",
    "Revenue":                               "Operation -Other revenue",
    "Road Toll":                             "Fleet",
    "Safety Expenses":                       "Safety",
    "Salaries Allowance":                    "HR",
    "Salaries and Wages":                    "HR",
    "Shipment Fees":                         "Material",
    "Site Access":                           "Other Expenses",
    "Social Insurances":                     "HR",
    "Staff Training":                        "HR",
    "Stock Adjustment":                      "Stock Adjustment",
    "Subcontractors":                        "Subcontractors",
    "Termination Bonus":                     "HR",
    "Tips":                                  "Tips",
    "Transportation":                        "Transportation",
    "Travelling":                            "Transportation",
    "Truck Expenses":                        "Logistic",
    "WH fees":                               "Warehouse",
    "Warehouse rent":                        "Warehouse",
}

ACCOUNT_MAP: dict[str, str] = {
    "WE - FTTH – Malawi –Hayat Kareema":   "WE",
    "WE - FTTH - Upper":                   "WE",
    "WE-CCTV":                             "WE",
    "OSP":                                 "WE",
    "Raya ICT":                            "Raya",
    "RAYA IP-Core":                        "Raya",
    "Ericsson Vodafone BEP":               "Ericsson",
    "Orange-FTTH":                         "Orange",
    "Orange-B2B":                          "Orange",
    "Orange-MNT":                          "Orange",
    "Vodafone-FTTH":                       "Vodafone",
    "Vodafone-IT":                         "Vodafone",
    "Vodafone VOIS RNO":                   "Vodafone",
    "Fiber-Sales-Marketing-Desti":         "New Account & Sales",
    "OSP-Fiber Supply":                    "New Account & Sales",
    "Huawei-FTTH":                         "Huawei",
    "Fiber -Nokia":                        "Nokia",
    "Nokia Transmission & Monorail":       "Nokia",
    "Fiber -NEC":                          "NEC",
    "Etisalat FTTH":                       "Etisalat",
    "ITS":                                 "Enterprise",
    "Cairo university CCTV":              "Enterprise",
    "Akhnaton project":                    "Enterprise",
    "Al Fayoum Universty project":         "Enterprise",
    "Cairo universty Solar":               "Other",
    "Fiber - Lab":                         "ASIS",
    # encoding-corrupted variant (original bug: Fiber â€" Lab)
    "Fiber \u2013 Lab":                    "ASIS",
    "Asis":                                "ASIS",
}

QUARTER_MAP: dict[int, str] = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}

MONTH_ORDER: list[str] = [
    "January", "February", "March", "April",
    "May", "June", "July", "August",
    "September", "October", "November", "December",
]


# ── Step functions ────────────────────────────────────────────────────────────

def _drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop # and Details — they carry no analytical value. Keep JE No."""
    to_drop = [c for c in ["#", "Details"] if c in df.columns]
    return df.drop(columns=to_drop)


def _extract_item(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract the expense-item name from Source.Name.

    The scraper saves filenames like:
        Record of monthly Journal Entry for Settlement Category _WH fees __.csv

    So we pull the text between the first ' _' and the last ' __'.

    FIX vs original: original used r'\\[(.*?)\\]' which matched brackets that
    no longer appear in the new filenames.
    """
    # Pattern A (new ERP format): _CategoryName _..csv  →  _(.+?) _[_.]
    # Pattern B (old ERP format): _CategoryName __.csv  →  _(.+?)__
    # Both are handled by:  _(.+?) _[_.]  which matches ' _' followed by _ or .
    df["item"] = (
        df["Source.Name"]
        .str.extract(r"_(.+?) _[_\.]", expand=False)
        .str.strip()
    )

    # Fallback 1: old double-underscore format without trailing space
    mask_missing = df["item"].isna()
    if mask_missing.any():
        df.loc[mask_missing, "item"] = (
            df.loc[mask_missing, "Source.Name"]
            .str.extract(r"_(.+?)__", expand=False)
            .str.strip()
        )

    # Fallback 2: original bracket pattern [category]
    mask_missing = df["item"].isna()
    if mask_missing.any():
        df.loc[mask_missing, "item"] = (
            df.loc[mask_missing, "Source.Name"]
            .str.extract(r"\[(.*?)\]", expand=False)
            .str.strip()
        )

    unmapped = df["item"].isna().sum()
    if unmapped:
        log.warning("Could not extract item name for %d rows.", unmapped)

    return df.drop(columns=["Source.Name"])


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse JE Date and derive Year, month (name), Quarter columns.

    The ERP exports dates in '31 Jan 26' format (%d %b %y).
    Falls back to pandas auto-inference if the primary format fails.
    """
    # Primary format from real ERP export: '31 Jan 26'
    df["JE Date"] = pd.to_datetime(df["JE Date"], format="%d %b %y", errors="coerce")

    # Fallback: try auto-inference for any rows that didn't parse
    mask_failed = df["JE Date"].isna()
    if mask_failed.any():
        df.loc[mask_failed, "JE Date"] = pd.to_datetime(
            df.loc[mask_failed, "JE Date"], errors="coerce"
        )

    null_dates = df["JE Date"].isna().sum()
    if null_dates:
        log.warning("%d rows have unparseable JE Date — they will be NaT.", null_dates)

    df["Year"]    = df["JE Date"].dt.year.astype("Int64")   # Int64 handles NaT gracefully
    df["month"]   = df["JE Date"].dt.strftime("%B")
    df["Quarter"] = df["JE Date"].dt.quarter.map(QUARTER_MAP)
    return df


def _parse_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert comma-formatted string columns to float.
    Handles both string inputs (e.g. '1,234.56') and already-numeric values.

    FIX vs original: Credit.fillna(0) was called twice — removed duplicate.
    FIX vs original: % used JE net Amount total — now uses Cost amount.
    """
    for col in ["JE net Amount", "Debit", "Credit"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .replace("nan", pd.NA)
                .astype(float)
            )

    df["Credit"] = df["Credit"].fillna(0)
    df["Debit"]  = df["Debit"].fillna(0)

    df["Cost amount"] = df["Debit"] - df["Credit"]
    return df


def _map_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map item → Category.
    LG-number items (Com LG XXXXXX) that are not in the explicit map
    are caught by a fallback rule rather than left as None.
    """
    def _resolve(item: str) -> str:
        if pd.isna(item):
            return "Unknown"
        if item in CATEGORY_MAP:
            return CATEGORY_MAP[item]
        if "LG" in str(item):
            return "Finance Cost"
        return "Unknown"

    df["Category"] = df["item"].apply(_resolve)

    unknown = (df["Category"] == "Unknown").sum()
    if unknown:
        unmapped_items = df.loc[df["Category"] == "Unknown", "item"].unique()
        log.warning(
            "%d rows have unmapped Category. Items: %s",
            unknown,
            list(unmapped_items),
        )
    return df


def _map_account(df: pd.DataFrame) -> pd.DataFrame:
    """Map Project → Account (client group)."""
    df["Account"] = df["Project"].map(ACCOUNT_MAP)

    unknown = df["Account"].isna().sum()
    if unknown:
        unmapped_projects = df.loc[df["Account"].isna(), "Project"].unique()
        log.warning(
            "%d rows have unmapped Account. Projects: %s",
            unknown,
            list(unmapped_projects),
        )
    return df


def _compute_pct(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add % column = each row's Cost amount / total Cost amount.
    FIX vs original: original used JE net Amount total (incorrect).
    """
    total = df["Cost amount"].sum()
    if total == 0:
        df["%"] = 0.0
        return df

    df["%"] = (df["Cost amount"] / total).round(4)
    df["%"] = df["%"].apply(lambda x: f"{x:.2%}")
    return df


# ── Public API ────────────────────────────────────────────────────────────────

def clean_cost_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the full cleaning pipeline on a raw cost DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame from scraper.scrape_cost_data()

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with columns:
        JE Date, JE net Amount, Debit, Credit, Project,
        item, Year, month, Quarter, Cost amount, Category, Account, %
    """
    if df.empty:
        log.warning("clean_cost_data received an empty DataFrame — returning as-is.")
        return df

    log.info("Cleaning %d raw rows …", len(df))

    df = (
        df
        .pipe(_drop_unused_columns)
        .pipe(_extract_item)
        .pipe(_parse_dates)
        .pipe(_parse_numeric)
        .pipe(_map_category)
        .pipe(_map_account)
        .pipe(_compute_pct)
    )

    log.info(
        "Cleaning complete — %d rows, %d columns. "
        "Category unknowns: %d | Account unknowns: %d",
        len(df),
        len(df.columns),
        (df["Category"] == "Unknown").sum(),
        df["Account"].isna().sum(),
    )
    return df


# ── CLI / quick test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Accept an optional folder path as argument; default to Cost_01-2026
    folder = Path(sys.argv[1]) if len(sys.argv) > 1 else (
        Path.home() / "Downloads" / "Cost_01-2026"
    )

    if not folder.exists():
        print(f"Folder not found: {folder}")
        print("Run scraper.py first, or pass the CSV folder as an argument.")
        sys.exit(1)

    # Load CSVs
    frames = []
    for f in sorted(folder.glob("*.csv")):
        try:
            tmp = pd.read_csv(f, encoding="utf-8-sig")
            tmp["Source.Name"] = f.name
            frames.append(tmp)
        except Exception:
            pass

    if not frames:
        print("No CSVs found.")
        sys.exit(1)

    raw = pd.concat(frames, ignore_index=True)
    print(f"Raw rows: {len(raw)}")

    clean = clean_cost_data(raw)
    print(f"\nClean rows : {len(clean)}")
    print(f"Columns    : {list(clean.columns)}")
    print(f"\nCategory breakdown:\n{clean['Category'].value_counts()}")
    print(f"\nAccount breakdown:\n{clean['Account'].value_counts()}")
    print(f"\nSample:\n{clean.head(3).to_string()}")
