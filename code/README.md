# ERP + DingTalk Flow (Code Folder)

This folder is the consolidated implementation for the current end-to-end flow:

1. Watch new ERP purchase orders
2. Create DingTalk approval instances
3. Poll DingTalk instance status
4. Write approval result back to ERP
5. Store ERP<->DingTalk mapping in SQLite

## Main Files

- `dingtalk_callback_monitor_gui.py`
  - All-in-one GUI entry
  - Start/stop full flow
  - Monitor logs and mapping table
- `purchase_order_new_monitor.py`
  - Detect new ERP purchase orders
  - Create DingTalk approvals
- `dingtalk_erp_bridge.py`
  - Mapping DB operations
  - ERP writeback service
- `dingtalk_approval_callback_server.py`
  - Callback server (optional if using callback mode)
- `simulate_dingtalk_callback.py`
  - Local callback simulation tool
- `query_last_year_sales_orders.py`
  - Shared ERP WebAPI helpers

## Default Runtime Entry

From `D:\ProjectPackage\demo\erp_demo`:

- `start_dingtalk_callback_monitor_gui.bat`

The BAT launchers are now pointed to scripts under `code/`.

## Notes

- Mapping DB default:
  - `D:\ProjectPackage\demo\erp_demo\erp_dingtalk_links.db`
- DingTalk app env default:
  - `D:\ProjectPackage\demo\dingding_demo\code\.env`
- Default DingTalk approver used by PO monitor:
  - `143908412435636200`

