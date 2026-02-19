"""Compare Excel file pairs to verify optimization did not change output."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd


def compare_excel_pair(file_a: Path, file_b: Path) -> list[str]:
    """Compare two Excel files and return a list of difference descriptions."""
    diffs: list[str] = []

    if not file_a.exists():
        diffs.append(f"MISSING: {file_a}")
        return diffs
    if not file_b.exists():
        diffs.append(f"MISSING: {file_b}")
        return diffs

    xl_a = pd.ExcelFile(file_a)
    xl_b = pd.ExcelFile(file_b)

    sheets_a = set(xl_a.sheet_names)
    sheets_b = set(xl_b.sheet_names)

    # Filter out "summary" sheets (case-insensitive)
    def non_summary(names):
        return {s for s in names if s.lower() != "summary"}

    sa = non_summary(sheets_a)
    sb = non_summary(sheets_b)

    only_a = sa - sb
    only_b = sb - sa
    if only_a:
        diffs.append(f"Sheets only in {file_a.name}: {sorted(only_a)}")
    if only_b:
        diffs.append(f"Sheets only in {file_b.name}: {sorted(only_b)}")

    common = sorted(sa & sb)
    print(f"  Common sheets (excl. summary): {len(common)}")

    for sheet in common:
        df_a = pd.read_excel(xl_a, sheet_name=sheet)
        df_b = pd.read_excel(xl_b, sheet_name=sheet)

        prefix = f"  [{sheet}]"

        # Row count
        if len(df_a) != len(df_b):
            diffs.append(f"{prefix} Row count differs: {len(df_a)} vs {len(df_b)}")

        # Column comparison
        cols_a = list(df_a.columns)
        cols_b = list(df_b.columns)
        if cols_a != cols_b:
            only_in_a = set(cols_a) - set(cols_b)
            only_in_b = set(cols_b) - set(cols_a)
            if only_in_a:
                diffs.append(f"{prefix} Columns only in {file_a.name}: {sorted(only_in_a)}")
            if only_in_b:
                diffs.append(f"{prefix} Columns only in {file_b.name}: {sorted(only_in_b)}")
            if set(cols_a) == set(cols_b) and cols_a != cols_b:
                diffs.append(f"{prefix} Column ORDER differs")
                # Reorder for value comparison
                df_b = df_b[cols_a]

        # Value comparison on common columns
        common_cols = [c for c in cols_a if c in set(cols_b)]
        if not common_cols:
            continue

        min_rows = min(len(df_a), len(df_b))
        da = df_a[common_cols].iloc[:min_rows].reset_index(drop=True)
        db = df_b[common_cols].iloc[:min_rows].reset_index(drop=True)

        for col in common_cols:
            a_col = da[col]
            b_col = db[col]

            # Both numeric?
            a_num = pd.to_numeric(a_col, errors="coerce")
            b_num = pd.to_numeric(b_col, errors="coerce")

            # Check where both are NaN - those are equal
            both_nan = a_col.isna() & b_col.isna()

            if a_num.notna().any() or b_num.notna().any():
                # Numeric comparison with tolerance
                mask = a_num.notna() & b_num.notna()
                if mask.any():
                    close = np.isclose(
                        a_num[mask].values.astype(float),
                        b_num[mask].values.astype(float),
                        rtol=1e-6,
                        atol=1e-10,
                        equal_nan=True,
                    )
                    n_diff = int((~close).sum())
                    if n_diff > 0:
                        idx_diff = np.where(~close)[0]
                        sample = idx_diff[:3]
                        mask_indices = mask[mask].index.tolist()
                        examples = []
                        for i in sample:
                            row_idx = mask_indices[i]
                            examples.append(
                                f"row {row_idx}: {a_num.iloc[row_idx]} vs {b_num.iloc[row_idx]}"
                            )
                        diffs.append(
                            f"{prefix} Col '{col}': {n_diff} numeric value diffs. "
                            f"E.g. {'; '.join(examples)}"
                        )

                # Check NaN mismatch
                nan_mismatch = (a_num.isna() != b_num.isna()) & ~both_nan
                n_nan = int(nan_mismatch.sum())
                if n_nan > 0:
                    diffs.append(f"{prefix} Col '{col}': {n_nan} NaN mismatches")
            else:
                # String / object comparison
                neq = (a_col.astype(str) != b_col.astype(str)) & ~both_nan
                n_diff = int(neq.sum())
                if n_diff > 0:
                    idx_diff = neq[neq].index[:3]
                    examples = [
                        f"row {i}: '{a_col.iloc[i]}' vs '{b_col.iloc[i]}'" for i in idx_diff
                    ]
                    diffs.append(
                        f"{prefix} Col '{col}': {n_diff} string diffs. "
                        f"E.g. {'; '.join(examples)}"
                    )

    xl_a.close()
    xl_b.close()
    return diffs


def main():
    base = Path("/Users/mussina/Repo/TWStockAnalysis")
    pairs = [
        ("alpha_pick.xlsx", "alpha_pick_bk.xlsx"),
        ("alpha_sell.xlsx", "alpha_sell_bk.xlsx"),
    ]

    all_ok = True
    for name_a, name_b in pairs:
        print(f"\n{'='*60}")
        print(f"Comparing: {name_a} vs {name_b}")
        print("=" * 60)
        diffs = compare_excel_pair(base / name_a, base / name_b)
        if diffs:
            all_ok = False
            print(f"\n  DIFFERENCES FOUND ({len(diffs)}):")
            for d in diffs:
                print(f"    {d}")
        else:
            print("\n  IDENTICAL (excluding summary sheets)")

    print(f"\n{'='*60}")
    if all_ok:
        print("RESULT: All file pairs are identical.")
    else:
        print("RESULT: Differences were found.")
    print("=" * 60)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
