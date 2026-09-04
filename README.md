# fastEHC

Processes a large Checkmarx SAST OData `Scans` export (`--%24select`/`--%24expand` JSON dump
from `/CxWebInterface/odata/v1/Scans`) into Engineering Health Check metrics: scan volume,
duration/queue timing, results by severity, language and preset mix, concurrency, and
size/volume/severity breakdowns by logical project and by team.

Output can be CSV files, a fully-styled Excel workbook, or both.

## Requirements

```
pip install -r requirements.txt
```

The Excel report is generated entirely from code (`workbook_builder.py` + `cx_theme.py`) --
there is no `.xlsx` template file to keep in sync. The Checkmarx wordmark used in the report
is bundled under `assets/`.

## Usage

Most common: generate just the Excel report.

```
python fastEHC.py <input-file> --excel
```

Full option list:

```
python fastEHC.py <input-file> [--customer NAME] [--csv] [--full-data] [--excel] [--cc-snapshot SECONDS]
```

- `<input-file>`: the OData JSON export.
- `--customer`: optional name used to label the output folder/workbook.
- `--csv`: write one CSV per metric section to `ehc_output_<customer>_<timestamp>/`.
- `--full-data`: also write a CSV of every raw scan record (for ad-hoc analysis).
- `--excel`: build `EHC-<customer>.xlsx` in the same output folder.
- `--cc-snapshot`: interval (seconds) for the scan-concurrency simulation (default 15; smaller is more precise but slower).

At least one of `--csv`, `--full-data`, or `--excel` is required.

## Excel report layout

- **Summary** -- headline metrics, pulled from `Data` via formulas.
- **Projects** / **Teams** -- scan volume, size, and severity (avg/max/min) grouped by
  logical project (branch/suffix normalized off the raw `ProjectName`) and by team, each
  with a Top-15-by-volume and Top-15-by-size cut for charting.
- **Scan Time Analysis** -- scan duration broken down by LOC-size bucket.
- **Charts** -- daily/weekly scan summary, concurrency, language mix, size/origin/preset
  breakdowns, results by severity, and project/team leaderboards.
- **Data** -- the raw per-section tables everything else reads from.

## License

MIT License

Copyright (c) 2024

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
