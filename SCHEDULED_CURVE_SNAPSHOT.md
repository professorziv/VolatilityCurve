# Scheduled Curve Snapshot

## What changed

- Manual save in `web_app.py` is unchanged.
- A standalone script `scheduled_curve_snapshot.py` was added for timed snapshot capture.
- Default save targets now come from `curve_save_config.json`.
- Scheduled snapshots are saved with:
  - `source = "scheduler"`
  - `notes = "scheduled@09:30"` / `scheduled@13:45` / `scheduled@22:00`
- Scheduled snapshots always use `curve_mode = "Mid"`.
- If the same scheduled slot is executed more than once on the same day for the same underlying and curve mode, the old snapshot is replaced.

## Run manually

```powershell
.\\.venv\\Scripts\\python.exe .\\scheduled_curve_snapshot.py --products cu2604,au2606 --slot 09:30
```

Optional parameters:

```powershell
.\\.venv\\Scripts\\python.exe .\\scheduled_curve_snapshot.py `
  --products cu2604,au2606 `
  --slot 13:45 `
  --risk-free 0.05 `
  --dividend 0.05 `
  --otm-range-pct 0.10
```

If `--products` is omitted, the script will use `scheduled_underlyings` from `curve_save_config.json`.

## Windows Task Scheduler

Recommended approach: create 3 separate tasks and pass the slot explicitly.

Program:

```text
D:\5.liujinghua\pythonCode\CTP\.venv\Scripts\python.exe
```

Start in:

```text
D:\5.liujinghua\pythonCode\CTP\ctp_web_project
```

Arguments for 09:30:

```text
scheduled_curve_snapshot.py --products cu2604,au2606 --slot 09:30
```

Arguments for 13:45:

```text
scheduled_curve_snapshot.py --products cu2604,au2606 --slot 13:45
```

Arguments for 22:00:

```text
scheduled_curve_snapshot.py --products cu2604,au2606 --slot 22:00
```

## Notes

- The task must run on a machine that can connect to the CTP market data front and the MySQL database.
- The script does not keep a resident scheduler process alive. Timing is delegated to Windows Task Scheduler.
- `evaluation_date` uses the local current date when the script runs.
