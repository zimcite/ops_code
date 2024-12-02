# ops_code_v2
Ops team codebase (for the update to py311)

## sea_cash_rec
 - reconcile swap unwind performance based on settle date (threshold=USD10)
 - integrated into legacy V2 excel framework 
 - on date T 
   - reconcile and booked cfd unwind performance settled on T for all brokers
   - reconcile and booked cash dividend settled on T for GS/BOAML; T-1 for UBS/JPM/MS

## sea_me_valuation_rec
 - rec positions against zen (CFD & Physical)
## sea_broker_cash_balance
- replace legacy vba procedures to parse and populate cash balance of brokers
- flexible to config

## get bbg trades
 - v2 python 311
