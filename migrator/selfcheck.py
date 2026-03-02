from __future__ import annotations
import py_compile
import pathlib
import sys

def main():
    root = pathlib.Path(__file__).resolve().parents[1]
    py_files = list(root.rglob("*.py"))
    bad = []
    for p in py_files:
        try:
            py_compile.compile(str(p), doraise=True)
        except Exception as e:
            bad.append((str(p), repr(e)))
    if bad:
        print("SELF-CHECK FAILED")
        for p,e in bad:
            print(p, e)
        sys.exit(1)
    print("SELF-CHECK OK:", len(py_files), "files")

if __name__ == "__main__":
    main()
