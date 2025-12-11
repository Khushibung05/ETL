# run_pipeline.py
"""
Run the full ETL pipeline:
  1) extract
  2) transform
  3) load
  4) analysis

This runner tries to call functions from the modules if available, and
falls back to invoking the scripts with Python if imports fail.
"""

from __future__ import annotations
import importlib
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

ROOT = Path(__file__).resolve().parents[0]
DATA_RAW = ROOT / "data" / "raw"
DATA_STAGED = ROOT / "data" / "staged"
DATA_PROCESSED = ROOT / "data" / "processed"

# Ensure folders exist
DATA_RAW.mkdir(parents=True, exist_ok=True)
DATA_STAGED.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)


def try_import(module_name: str):
    try:
        return importlib.import_module(module_name)
    except Exception:
        return None


def run_subprocess(script: Path) -> int:
    """
    Run 'python <script>' and stream output to console.
    Returns the script exit code.
    """
    print(f"🧪 Running script fallback: python {script}")
    completed = subprocess.run([sys.executable, str(script)], cwd=str(ROOT))
    return completed.returncode


# ---------------------------
# 1) Extract
# ---------------------------
def run_extract() -> List[Dict[str, Any]]:
    """
    Attempts to run extract step.

    Returns a list of result dicts:
      e.g. [{"city":"Delhi","raw_path":"/abs/path","source":"OpenAQ"}, ...]
    or a list of saved raw file paths if that's what the extract returns.
    """
    print("\n=== STEP 1: EXTRACT ===")
    mod = try_import("extract")
    results: List[Dict[str, Any]] = []

    if mod:
        # Try common function names in order of preference
        for fn_name in ("fetch_all_raw", "fetch_all", "fetch_all_cities", "extract_weather_data", "fetch_all_cities", "run_extract"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    print(f"Calling extract.{fn_name}() ...")
                    out = fn()
                    # normalize return types
                    if isinstance(out, list):
                        results = out
                    elif isinstance(out, dict):
                        # single result -> wrap
                        results = [out]
                    elif isinstance(out, str):
                        results = [{"raw_path": out}]
                    else:
                        results = []
                    print("✅ Extract finished (imported function).")
                    return results
                except Exception as e:
                    print(f"⚠️ extract.{fn_name}() raised exception: {e}")
                    # try next fallback
    # fallback: run script
    script = ROOT / "extract.py"
    if script.exists():
        code = run_subprocess(script)
        if code == 0:
            # collect raw files produced in data/raw (newest ones)
            paths = sorted(DATA_RAW.glob("*"), key=lambda p: p.stat().st_mtime)
            results = [{"raw_path": str(p)} for p in paths]
            print(f"✅ Extract fallback produced {len(results)} raw files.")
            return results
        else:
            raise RuntimeError(f"extract.py exited with code {code}")
    raise RuntimeError("No extract method found and extract.py missing or failed.")


# ---------------------------
# 2) Transform
# ---------------------------
def run_transform(extract_results: List[Dict[str, Any]]) -> Optional[str]:
    """
    Attempts to run transform. Returns the path to the staged CSV if found.
    """
    print("\n=== STEP 2: TRANSFORM ===")
    mod = try_import("transform")

    # Prefer transform.transform_data(raw_paths) style
    if mod:
        # try a few function names
        for fn_name in ("transform_data", "transform", "run_transform"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    print(f"Calling transform.{fn_name}() ...")
                    # if transform expects a list of raw paths, provide them
                    raw_paths = []
                    for r in extract_results:
                        if isinstance(r, dict) and r.get("raw_path"):
                            raw_paths.append(r["raw_path"])
                        elif isinstance(r, str):
                            raw_paths.append(r)
                    # Call with raw_paths if function accepts args
                    try:
                        staged = fn(raw_paths) if raw_paths else fn()
                    except TypeError:
                        staged = fn()
                    # If function returned path or list
                    if isinstance(staged, str):
                        print(f"✅ Transform returned staged file: {staged}")
                        return staged
                    if isinstance(staged, list) and staged:
                        # assume first staged file
                        print(f"✅ Transform returned staged files: {staged}")
                        return staged[0]
                    # else continue to fallback
                    print("✅ Transform function completed (no path returned). Will search staged dir.")
                    break
                except Exception as e:
                    print(f"⚠️ transform.{fn_name}() raised exception: {e}")
                    # try next
    # fallback: run script
    script = ROOT / "transform.py"
    if script.exists():
        code = run_subprocess(script)
        if code != 0:
            raise RuntimeError(f"transform.py exited with code {code}")
    # After running transform (imported or fallback), try to locate the latest staged CSV
    staged_files = sorted(DATA_STAGED.glob("*.csv"), key=lambda p: p.stat().st_mtime)
    if staged_files:
        print(f"✅ Found staged CSV: {staged_files[-1]}")
        return str(staged_files[-1].resolve())
    print("⚠️ No staged CSV found in data/staged/. Transform may have failed or produced a differently named file.")
    return None


# ---------------------------
# 3) Load
# ---------------------------
def run_load(staged_csv: Optional[str]):
    print("\n=== STEP 3: LOAD ===")
    if staged_csv is None:
        raise RuntimeError("No staged CSV provided to load step.")

    mod = try_import("load")
    if mod:
        # try create_table_if_not_exists then load_to_supabase or load_to_supabase(staged_csv)
        try:
            create_fn = getattr(mod, "create_table_if_not_exists", None)
            if callable(create_fn):
                print("Calling load.create_table_if_not_exists() ...")
                try:
                    create_fn()
                except Exception as e:
                    print(f"⚠️ create_table_if_not_exists raised: {e}")
            load_fn = getattr(mod, "load_to_supabase", None)
            if callable(load_fn):
                print(f"Calling load.load_to_supabase('{staged_csv}') ...")
                # try signatures with/without args
                try:
                    res = load_fn(staged_csv)
                except TypeError:
                    res = load_fn()
                print("✅ Load function call completed.")
                return res
        except Exception as e:
            print(f"⚠️ load module raised exception: {e}")

    # fallback: run script
    script = ROOT / "load.py"
    if script.exists():
        # attempt to pass staged_csv as env var so script can read it if coded to do so
        env = dict(os.environ)
        env["STAGED_CSV"] = str(staged_csv)
        print(f"🧪 Running fallback loader script: python {script}")
        completed = subprocess.run([sys.executable, str(script)], cwd=str(ROOT), env=env)
        if completed.returncode == 0:
            print("✅ load.py completed (fallback).")
            return True
        else:
            raise RuntimeError(f"load.py exited with code {completed.returncode}")
    raise RuntimeError("No load method found and load.py missing or failed.")


# ---------------------------
# 4) Analysis
# ---------------------------
def run_analysis():
    print("\n=== STEP 4: ANALYSIS ===")
    mod = try_import("etl_analysis")
    if mod:
        fn = getattr(mod, "main", None) or getattr(mod, "run_analysis", None) or getattr(mod, "run", None)
        if callable(fn):
            print("Calling etl_analysis.main() ...")
            try:
                return fn()
            except Exception as e:
                print(f"⚠️ etl_analysis.main() raised: {e}")
    # fallback: run script
    script = ROOT / "etl_analysis.py"
    if script.exists():
        code = run_subprocess(script)
        if code == 0:
            print("✅ etl_analysis.py completed (fallback).")
            return True
        raise RuntimeError(f"etl_analysis.py exited with code {code}")
    raise RuntimeError("No analysis method found and etl_analysis.py missing or failed.")


# ---------------------------
# Orchestration
# ---------------------------
def run_full_pipeline():
    start = time.time()
    try:
        extract_results = run_extract()
    except Exception as e:
        print(f"\n❌ Extract step failed: {e}")
        return

    # small pause
    time.sleep(1)

    try:
        staged_csv = run_transform(extract_results)
    except Exception as e:
        print(f"\n❌ Transform step failed: {e}")
        return

    # small pause
    time.sleep(1)

    try:
        run_load(staged_csv)
    except Exception as e:
        print(f"\n❌ Load step failed: {e}")
        return

    # small pause
    time.sleep(1)

    try:
        run_analysis()
    except Exception as e:
        print(f"\n❌ Analysis step failed: {e}")
        return

    elapsed = time.time() - start
    print(f"\n🎉 Pipeline finished in {elapsed:.1f}s")


if __name__ == "__main__":
    run_full_pipeline()
