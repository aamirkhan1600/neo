# Premium-Trigger Strategy

A reference write-up of the price-trigger short-options strategy implemented in
`algo-trading-no-redis/src/services/premiumTrigger.service.js` on branch
`uat-main`. Use this when porting the strategy to a different platform
(Kotak Neo, Fyers, mstock, …) or another runtime.

## 1. What it does

A one-sided short-options strategy. Sells the CE leg, the PE leg, or both,
when their PREMIUM rises into a configured window, then buys back to close at
a per-trade target (profit, in points off entry) or stoploss (loss, in points
off entry).

Legs are independent — CE and PE keep separate state machines, separate
counters, separate P&L. The strategy resolves real instrument tokens
automatically: ATM strike of nearest expiry, a fixed strike, or an OTM strike
whose CURRENT premium is closest to your `entryTrigger`.

While running, it consumes a tick stream and fires entries / exits as ticks
arrive — no polling.

## 2. State machine

```
                     ┌──────────────────────────────────────┐
                     │  cfg loaded from settings table       │
                     │  legs CE/PE resolved (token, strike)  │
                     │  legState[*].armed = true              │
start()  ─────────► │  bus subscribed to tick events        │
                     │  market.subscribeTokens(CE, PE, spot) │
                     └──────────────────────────────────────┘
                                       │
                       ┌───────────────┴───────────────┐
                       │       per leg (CE / PE)        │
                       │      position    = NONE         │
                       │      armed       = true         │
                       │      triggerPrice= null         │
                       └───────────────┬───────────────┘
                                       │
   evaluate(side) on every tick:

   1. Re-arm gate (only NONE & !slLocked):
        if  trigger - rearmLower  ≤  ltp  ≤  trigger + rearmUpper
        then armed = true ; clear triggerPrice

   2. Drift watcher (only NONE):
        outsideWindowSince = first tick outside re-arm window
        if outside > restrikeDwellSec  AND  cooldown ok
        AND strikeMode='otm':  resolveOTMLeg() picks a new strike whose
        current premium is closest to entryTrigger and re-subscribes.

   3. Capture (NONE & armed):
        If ltp ∈ re-arm window:        triggerPrice = ltp
        If ltp drops captureFadeThreshold below capture: discard.
        If ltp leaves the re-arm window:                 discard.

   4. Confirmation (NONE & armed & captured):
        If  ltp ≥ triggerPrice + reentryOffset:
              SELL → position = SHORT
              entryPrice = ltp ; armed = false
              currentTradeTarget = (entryCount==0 ? target : reentryTarget)

   5. Exit (only SHORT):
        targetPrice = entryPrice − currentTradeTarget   (≤ ⇒ profit)
        slPrice     = entryPrice + stoploss             (≥ ⇒ loss)
        On hit:  BUY-to-close (reason=target|stoploss)
                 deduct charges, recordTradeStats()
                 position = NONE ; await re-arm.

   ⌬ All entries blocked while  dayLossLocked = true
     (cumulative realised loss ≤ −maxDailyLoss).  Exits still fire.
   ⌬ Saving settings clears dayLossLocked and zeroes session counters.
```

## 3. Per-leg config (`legs.ce`, `legs.pe`)

| Field | Units | Meaning |
|---|---|---|
| `enabled` | bool | Gate — `false` ⇒ no entries fire on this leg, but it still streams LTP. |
| `entryTrigger` | rupees | Premium level at which we want to be short. |
| `target` | points | Profit-take distance off `entryPrice` (entry #1). |
| `stoploss` | points | Stoploss distance off `entryPrice`. |
| `reentryOffset` | points | After capture, LTP must rise this many points further before SELL fires (anti-noise filter). |
| `reentryTarget` | points | Profit target for entries 2..N. |
| `maxEntryDeviation` | points | Hard cap on how far above `entryTrigger` we'll still take an entry. |
| `rearmRange` | points | Symmetric ± window. Legacy field. |
| `rearmLower` / `rearmUpper` | points | Asymmetric overrides; fall back to `rearmRange`, default 5. |
| `captureFadeThreshold` | points | After capture, if LTP drops this many points below the captured price, drop the capture. |

## 4. Top-level config

| Field | Meaning |
|---|---|
| `enabled` | bool master switch. |
| `underlyingName` | `NIFTY` / `BANKNIFTY` / etc. |
| `expiry` | `YYYY-MM-DD` or `null`/`'auto'` ⇒ pick nearest future. |
| `strikeMode` | `atm` / `fixed` / `otm`. |
| `fixedStrike` | Used when `strikeMode='fixed'`. |
| `qty` | Multiplier on top of `lotSize`. |
| `exchange` | Default `NFO`. |
| `maxDailyLoss` | Realised-loss circuit breaker (₹). When breached, both legs lock; resaving settings re-arms. |
| `autoRestrikeOnDrift` | OTM-only — roll the strike when LTP drifts out of re-arm window. |
| `restrikeDwellSec` | Default 60. |
| `restrikeCooldownSec` | Default 300 (per leg). |
| `brokeragePerOrder`, `sttPctSell`, `exchTxnPct`, `sebiPct`, `stampPctBuy`, `gstPct` | Cost model — see `tradeCharges()` in section 8. |

## 5. Per-leg runtime state

```
ce / pe : {
  // resolved at start
  token, symbol, exchange, lotSize, strike, expiry,

  // tick state
  ltp,
  position : 'NONE' | 'SHORT',
  entryPrice, currentTradeTarget,
  pending,                       // order in flight
  armed, triggerPrice,           // capture state
  slLocked,                      // post-SL until re-arm
  entryCount,                    // 0 ⇒ first entry, ≥1 ⇒ re-entry

  // P&L (rupees)
  realizedPnl, tradesPlaced, tradesCompleted, tradesWon, tradesLost,
  grossProfit, grossLoss, maxTradeProfit, maxTradeLoss,
  totalCharges, lastTradeCharges,

  // P&L (points — premium movement, signed)
  realizedPoints, grossProfitPoints, grossLossPoints,
  maxTradeProfitPoints, maxTradeLossPoints,

  // restrike timers
  outsideWindowSince, lastRestrikeAt, restrikeInProgress, restrikeCount
}
```

Session aggregates: `peakRealized`, `maxDrawdown`, `dayLossLocked`,
`signalHistory[≤50]`.

## 6. Strike resolution (`resolveLegs`)

Runs at start (and again on save while running):

1. Validate the option-chain has options for `underlyingName`. If not, lazy-load
   the broker's NFO scrip master.
2. Resolve expiry: configured value if it has options on that date, otherwise
   nearest future.
3. Fetch underlying spot LTP (best-effort).
4. **ATM** (`strikeMode='atm'`): pick strike closest to spot, find the matching
   CE+PE pair, set `legState.{ce,pe}` and arm both.
5. **Fixed** (`strikeMode='fixed'`): same but anchored to `cfg.fixedStrike`.
6. **OTM** (`strikeMode='otm'`): for each side, scan up to 30 OTM candidates
   (CE: strike > spot, PE: strike < spot), one batched `getQuote()`, pick the
   candidate whose LTP is closest to that side's `entryTrigger`. CE and PE may
   resolve to different strikes.

## 7. Order placement

Every BUY/SELL goes through `order.processSignal({ tradingsymbol, exchange,
action, qty, price, orderType:'LIMIT', reason }, { bypassRisk: true })`.
Returns `{ ok, brokerOrderId, reason }`. On failure the leg state rolls back
so a failed order can't leave a phantom SHORT in memory.

## 8. Charges (per round-trip)

```
sellTurn = entry × qty
buyTurn  = exit  × qty
brokerage = 2 × brokeragePerOrder
stt       = sellTurn × sttPctSell  / 100   (sell-side only, options)
exch      = (sellTurn + buyTurn) × exchTxnPct / 100
sebi      = (sellTurn + buyTurn) × sebiPct    / 100
stamp     = buyTurn  × stampPctBuy / 100
gst       = (brokerage + exch + sebi) × gstPct / 100
total     = brokerage + stt + exch + sebi + stamp + gst
netPnl    = (entry − exit) × qty − total
```

`recordTradeStats()` updates per-leg P&L and points aggregates, then checks
the session circuit breaker:

```
if maxDailyLoss != null && totalRealized ≤ -maxDailyLoss:
  dayLossLocked = true   (cleared by next save())
```

## 9. Multi-broker shape

`createPremiumTrigger({ broker, settingsKey, kite, instruments, market })`
returns an isolated instance — its own `cfg`, `legState`, `signalHistory`,
`running`, listening on `bus.EVENTS.TICK:${broker}`.

`forBroker(name)` memoises one instance per broker. The module's default
exports are *active-broker delegates* that route to `appState.activeBroker`.

`onBrokerSwitch()` stops the strategy, clears `cfg.expiry` (cross-broker
dates rarely line up), and resets per-leg state so stale tokens don't get
reused.

## 10. Public API

```
load() / save(patch) / get() / status()
start() / stop() / toggle(on)
exitAll(reason)              // close every open SHORT at latest LTP
restrikeLeg(side)            // OTM-only manual roll; refuses while position!=NONE
activeLegTokens()            // tokens the ticker must keep subscribed (CE, PE, underlying)
onBrokerSwitch()             // see §9

_testing: setCfg, setLeg, attach, detach, resetState   // backtest harness
```

## 11. HTTP routes (`/trade/premium-trigger`)

```
GET  /status                  → status()
GET  /underlyings             → popular ∪ broker-loaded names
GET  /expiries?underlying=    → future expiries for that underlying
POST /reload-instruments      → re-fetch broker's NFO scrip master
POST /settings                → save(patch); accepts both nested {legs:{ce,pe}}
                                and flat ce_entryTrigger= form fields
POST /toggle                  → toggle(on)
POST /exit-all                → exitAll('manual')
POST /restrike?side=ce|pe     → restrikeLeg(side)
GET  /                        → render premiumTrigger.ejs (live dashboard)
GET  /settings                → render premiumTriggerSettings.ejs (form)
GET  /backtest                → render premiumTriggerBacktest.ejs (replay)
```

## 12. Edge cases worth knowing

- **stoploss-locked**: after an SL exit, `slLocked=true` blocks the next entry
  until LTP re-enters the re-arm window. Re-saving settings clears it.
- **Capture fade**: a captured `triggerPrice` is discarded if LTP drops
  `captureFadeThreshold` below it OR if LTP leaves the re-arm window.
- **Concurrency**: `pending=true` is set BEFORE any order call so the next
  tick's `evaluate(side)` returns immediately and we can't double-fire. Always
  reset in `finally`.
- **OTM auto-restrike**: only when `position='NONE'`; gated by
  `restrikeDwellSec` and `restrikeCooldownSec` per leg. On success, unsubscribe
  old token then subscribe new one before counters move.
- **Re-saving settings** wipes session counters AND zeroes `realizedPnl` by
  design — saving means "new session". Open positions are preserved via
  `currentTradeTarget`.
- **Underlying tick**: also subscribed so `spot` stays fresh (used by re-strike).
  Non-matching tokens route to per-leg LTP.

---

# Redevelopment Prompt (paste-ready)

> Build a **premium-trigger options strategy module** for an algorithmic-trading
> platform. The platform already has:
>
> - A live tick stream (one event per market tick with
>   `{instrument_token, last_price}`).
> - A broker SDK with `placeOrder({ tradingsymbol, exchange, side: 'BUY'|'SELL',
>   qty, price, orderType:'LIMIT' })`.
> - An option-chain service with
>   `listOptions(underlying) → [{token, tradingsymbol, exchange, instrumentType:'CE'|'PE', strike, expiry:Date, lotSize}]`
>   and `findStraddle({ name, strike, expiry })`.
> - A key-value `settings` table in MySQL.
>
> ## Goal
>
> Short the CE leg, PE leg, or both when their PREMIUM rises into a configured
> window; close at a per-trade target or stoploss measured in points from the
> fill price. CE and PE are independent: own state, own counters, own P&L. The
> module must run continuously off live ticks, persist its config across
> restarts, and recover deterministically when the operator re-saves settings.
>
> ## Per-leg config
>
> ```
> {
>   enabled: bool,
>   entryTrigger: number,        // ₹ premium level
>   target: number,              // points off entry, profit
>   stoploss: number,            // points off entry, loss
>   reentryOffset: number,       // points above triggerPrice before SELL fires (default 1)
>   reentryTarget: number,       // points; replaces target on entry #2 onwards
>   maxEntryDeviation: number,   // points; refuse entries more than this far above entryTrigger
>   rearmRange: number,          // ± window, default 5
>   rearmLower: number|null,     // overrides rearmRange on lower side
>   rearmUpper: number|null,     // overrides rearmRange on upper side
>   captureFadeThreshold: number // points; discard a capture if LTP drops this far below it
> }
> ```
>
> ## Top-level config
>
> ```
> {
>   enabled, underlyingName, expiry,        // 'YYYY-MM-DD' or null/'auto'
>   strikeMode: 'atm'|'fixed'|'otm',
>   fixedStrike, qty, exchange,
>   maxDailyLoss,                            // realised-loss cap in ₹; null disables
>   autoRestrikeOnDrift, restrikeDwellSec, restrikeCooldownSec,
>   brokeragePerOrder, sttPctSell, exchTxnPct, sebiPct, stampPctBuy, gstPct,
>   legs: { ce: <leg cfg>, pe: <leg cfg> }
> }
> ```
>
> ## State machine (per leg)
>
> States: `NONE → SHORT → NONE`. Per-tick evaluation:
>
> 1. **Re-arm gate** (only when `position=NONE && !slLocked`): if
>    `entryTrigger - rearmLower ≤ ltp ≤ entryTrigger + rearmUpper`, set
>    `armed=true` and clear `triggerPrice`.
> 2. **Drift watcher**: track `outsideWindowSince` while LTP is outside the
>    re-arm window. If outside ≥ `restrikeDwellSec` AND per-leg
>    `restrikeCooldownSec` elapsed AND `strikeMode='otm'`, fire
>    `maybeRestrike(side)` to roll to the strike whose current premium is
>    closest to `entryTrigger` (max ~30 OTM candidates, one batch quote, pick
>    min `|ltp − entryTrigger|`).
> 3. **Capture**: when `position=NONE && armed`, if `triggerPrice` is null and
>    LTP is in the re-arm window, set `triggerPrice = ltp`. If LTP later drops
>    `captureFadeThreshold` below `triggerPrice`, OR leaves the re-arm window,
>    clear `triggerPrice`.
> 4. **Confirmation → SELL**: if `triggerPrice` is set and
>    `ltp ≥ triggerPrice + reentryOffset`, fire SELL. Set `position='SHORT'`,
>    `entryPrice=ltp`, `armed=false`,
>    `currentTradeTarget = (entryCount==0 ? target : reentryTarget)`.
> 5. **Exit**: when `position='SHORT'`, compute
>    `targetPrice = entryPrice - currentTradeTarget` and
>    `slPrice = entryPrice + stoploss`. If `ltp ≤ targetPrice` → BUY
>    (reason='target'); else if `ltp ≥ slPrice` → BUY (reason='stoploss').
>    On fill, deduct charges, record stats, set `position='NONE'`, await
>    re-arm.
>
> All entries (but NOT exits) are blocked while `dayLossLocked = true`.
>
> ## Concurrency
>
> Set per-leg `pending=true` before any broker call. While true,
> `evaluate(side)` returns immediately. Always reset `pending=false` in
> `finally`. On any broker / DB exception during entry or exit, ROLL BACK the
> leg state (restore `position`, `entryPrice`, `currentTradeTarget`) so a
> failed order can't leave a phantom SHORT in memory.
>
> ## Strike resolution (`resolveLegs`)
>
> Runs at start and on every save-while-running:
>
> - Lazy-load the option chain for `underlyingName` if empty.
> - Resolve expiry: `cfg.expiry` if options exist on that date, else nearest
>   future. Surface available expiries in the error if not.
> - Fetch underlying spot via broker LTP API (best-effort).
> - **ATM**: pick strike closest to spot, look up matching CE+PE pair.
> - **Fixed**: pick strike closest to `fixedStrike`.
> - **OTM**: per side, filter strikes that are OTM relative to spot
>   (CE: strike > spot, PE: strike < spot), sort by distance from spot, take
>   first 30, batch `getQuote()`, pick the one whose `last_price` is closest
>   to that side's `entryTrigger`.
>
> ## Charges (per round-trip)
>
> ```
> sellTurn = entry × qty
> buyTurn  = exit  × qty
> brokerage = 2 × brokeragePerOrder
> stt       = sellTurn × sttPctSell  / 100
> exch      = (sellTurn + buyTurn) × exchTxnPct / 100
> sebi      = (sellTurn + buyTurn) × sebiPct    / 100
> stamp     = buyTurn  × stampPctBuy / 100
> gst       = (brokerage + exch + sebi) × gstPct / 100
> total     = brokerage + stt + exch + sebi + stamp + gst
> netPnl    = (entry - exit) × qty - total
> ```
>
> Track per-leg: `realizedPnl`, `tradesPlaced`, `tradesCompleted`,
> `tradesWon`, `tradesLost`, `grossProfit`, `grossLoss`, `maxTradeProfit`,
> `maxTradeLoss`, `totalCharges`, `lastTradeCharges`. Mirror in points:
> `realizedPoints`, `grossProfitPoints`, `grossLossPoints`,
> `maxTradeProfitPoints`, `maxTradeLossPoints`. Session-level: `peakRealized`,
> `maxDrawdown`, `dayLossLocked` (set true when total realised
> ≤ -maxDailyLoss; cleared on next save).
>
> ## Public API
>
> ```js
> load() / save(patch) / get() / status()
> start() / stop() / toggle(on)
> exitAll(reason)              // close every open SHORT at latest LTP
> restrikeLeg(side)            // OTM-only manual roll; refuses while position!=NONE
> activeLegTokens()            // tokens the ticker must keep subscribed
> onBrokerSwitch()             // stop, clear cfg.expiry, reset legState
> ```
>
> Plus a `_testing` namespace with `setCfg`, `setLeg`, `attach`, `detach`,
> `resetState` so a backtest harness can drive the strategy off historical
> ticks without touching the live ticker or DB.
>
> ## Multi-broker support
>
> Wrap the whole module in a `createPremiumTrigger({ broker, settingsKey,
> kite, instruments, market })` factory. `forBroker(name)` memoises one
> instance per broker; the module's default exports route to whichever broker
> is currently active. Each instance subscribes to its own broker's tick
> channel only.
>
> ## HTTP routes
>
> ```
> GET  /status                           → status()
> GET  /underlyings                      → popular ∪ broker-loaded
> GET  /expiries?underlying=NIFTY        → future expiries for that underlying
> POST /reload-instruments               → broker.refreshScripMaster()
> POST /settings                         → save(patch); accept both nested and flat form-encoded shapes
> POST /toggle  (?on=true|false)         → toggle(on)
> POST /exit-all                         → exitAll('manual')
> POST /restrike?side=ce|pe              → restrikeLeg(side)
> ```
>
> ## UI (server-rendered template + small JS poller)
>
> A live dashboard polling `/status` every ~1s and rendering:
>
> - Per-leg block: symbol/strike/expiry, position pill, LTP, entry price,
>   target price, SL price, unrealised ₹+pts, realised ₹+pts, won/lost counts,
>   max trade profit/loss.
> - Totals strip: realised, unrealised, peak, drawdown, accuracy %, profit
>   factor, total trades.
> - Signal history (last 20).
> - Buttons: Toggle ON/OFF, Square off all, Restrike CE / PE.
> - A separate settings page with the full config form, an Underlying dropdown,
>   Expiry dropdown driven by `/expiries`, a Strike Mode picker (ATM / Fixed /
>   OTM), per-leg field grid, cost model panel, and Daily-Loss + Auto-Restrike
>   controls.
>
> ## Acceptance criteria
>
> - First entry uses `legCfg.target`; re-entries use `legCfg.reentryTarget`.
> - SL closure followed by no re-arm tick must NOT immediately re-enter
>   (`slLocked=true` until LTP re-enters the re-arm window).
> - Captured `triggerPrice` is discarded by either fade
>   (LTP ≤ trigger − fade) or window exit; not by both independently.
> - Saving settings while running does: stop → clear session counters →
>   restart → re-arm both legs → resolve expiry/strike again.
> - Daily-loss breach: both legs refuse new entries; existing positions can
>   still exit; `lastError` carries an explanatory message; saving settings
>   clears the lock.
> - OTM auto-restrike: never fires while `position='SHORT'`; respects per-leg
>   cooldown; on success the old token is unsubscribed and the new one is
>   subscribed before counters move.
> - Failed broker call during entry must NOT leave `position='SHORT'` in memory
>   (rollback).
> - `activeLegTokens()` returns CE token, PE token, and the underlying.
> - Broker switch: stops the strategy, clears `cfg.expiry`, resets per-leg
>   `token/symbol/strike/expiry/ltp/position`.
>
> ## Reference
>
> `algo-trading-no-redis/src/services/premiumTrigger.service.js` on branch
> `uat-main` (~1374 lines, factory pattern, MySQL `settings` table, Zerodha
> Kite event bus). Use as reference but adapt to the target platform's broker
> SDK, event bus, and persistence layer.
