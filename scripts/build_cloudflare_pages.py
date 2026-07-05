from __future__ import annotations

from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    source = root / "src" / "templates" / "dashboard.html"
    output_dir = root / "public"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "index.html").write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
