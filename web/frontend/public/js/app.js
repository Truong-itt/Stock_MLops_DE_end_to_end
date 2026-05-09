/* ╔══════════════════════════════════════════════════════════════╗
   ║  Stock Dashboard — Real-Time Client                         ║
   ╚══════════════════════════════════════════════════════════════╝ */

const API   = window.location.origin;
const WS_PROTO = window.location.protocol === 'https:' ? 'wss' : 'ws';
const WS_URL= `${WS_PROTO}://${window.location.host}/ws`;
const MAX_LINE_POINTS = 5000;

// ─── State ──────────────────────────────────────────────────
let stocks      = {};          // keyed by symbol
let prevStocks  = {};          // full previous state for cell-level flash
let ws          = null;
let retryTimer  = null;
let pingTimer   = null;        // ping interval id
let selected    = null;        // currently viewed symbol
let chart       = null;
let sortMode    = "symbol";
let dataSource  = "daily";     // "latest" | "daily" | "merged"
let sectorFilter = "";         // current sector filter
let marketFilter = "";         // "" = all, "vn" = Vietnam, "world" = International
let symbolConfig = { vn: [], world: [] };
let cpSignals   = {};          // keyed by symbol, latest BOCPD state
let cpAlerts    = [];          // enriched abnormal/whale alerts from backend
let cpAlertMap  = {};          // keyed by symbol, best abnormal alert per symbol
let cpAlertSummary = null;     // summary payload from /changepoint/abnormal or /market/overview
let drawerCandles = null;      // last OHLC candle payload rendered in drawer
let drawerCandleSymbol = null;
let drawerResizeTimer = null;
let drawerCpHistory = [];
let drawerCpHistorySymbol = null;
let drawerRunlengthSegments = [];
let selectedRunlengthSegmentId = "__all";
let activePriceChartMode = "line";
let drawerLineSeries = [];
let drawerLineSymbol = null;
let drawerOhlcvMeta = null;
let drawerCandleCycleMeta = null;
let selectedCandles = [];
let selectedCandlesSymbol = null;
let activeCandleHitState = null;
let candleMinuteDetailLoading = false;
let candleCycleAuto = true;
let candleViewport = { key: null, start: 0, end: 0 };
let lineViewport = { key: null, start: 0, end: 0, stickToRight: true };
let lineAutoExpandInFlight = false;
let lineAutoExpandLastKey = "";
let lineAutoExpandLastAt = 0;
let candleDragSelection = {
  active: false,
  canvasId: null,
  symbol: null,
  anchorKey: null,
  anchorIndex: -1,
  endIndex: -1,
  moved: false,
  suppressClickUntil: 0,
};
let candlePanDrag = {
  active: false,
  canvasId: null,
  lastClientX: 0,
  moved: false,
  suppressClickUntil: 0,
};
let linePanDrag = {
  active: false,
  canvasId: null,
  lastClientX: 0,
};

// ─── Stock lists for market filter ─────────────────────────
let VN_STOCKS = new Set();
let WORLD_STOCKS = new Set();

// ─── Company name mapping ───────────────────────────────────
const COMPANY_NAME = {
  // Vietnam
  VCB: 'Vietcombank', BID: 'BIDV', FPT: 'FPT Corporation',
  HPG: 'Hòa Phát Group', CTG: 'VietinBank', VHM: 'Vinhomes',
  TCB: 'Techcombank', VPB: 'VPBank', VNM: 'Vinamilk',
  MBB: 'MB Bank', GAS: 'PV Gas', ACB: 'ACB',
  MSN: 'Masan Group', GVR: 'Tập đoàn Cao su VN', LPB: 'LienVietPostBank',
  SSB: 'SeABank', STB: 'Sacombank', VIB: 'VIB',
  MWG: 'Mobile World', HDB: 'HDBank',
  PLX: 'Petrolimex', POW: 'PetroVietnam Power', SAB: 'Sabeco',
  BCM: 'Becamex IDC', PDR: 'Phát Đạt', KDH: 'Khang Điền',
  NVL: 'Novaland', DGC: 'Hóa chất Đức Giang', SHB: 'SHB',
  EIB: 'Eximbank', VIC: 'Vingroup', REE: 'REE Corporation',
  VJC: 'VietJet Air', GMD: 'Gemadept', TPB: 'TPBank',
  VRE: 'Vincom Retail', VCI: 'CTCK Vietcap', SSI: 'CTCK SSI',
  HCM: 'CTCK HSC', VGC: 'Viglacera', DPM: 'PetroVietnam Fertilizer',
  KBC: 'Kinh Bắc', DCM: 'Phân đạm Cà Mau', VND: 'Vedan VN',
  PNJ: 'PNJ', HNG: 'HANEL', PVD: 'PVDrilling',
  DHG: 'Dược Hậu Giang', NT2: 'Nhà Khang Điền', DIG: 'DIC Corp',
  // International
  AAPL: 'Apple Inc.', MSFT: 'Microsoft Corp.', NVDA: 'NVIDIA Corp.',
  AMZN: 'Amazon.com Inc.', GOOGL: 'Alphabet Inc.', META: 'Meta Platforms Inc.',
  TSLA: 'Tesla Inc.', 'BRK-B': 'Berkshire Hathaway', LLY: 'Eli Lilly',
  AVGO: 'Broadcom Inc.', JPM: 'JPMorgan Chase', V: 'Visa Inc.',
  UNH: 'UnitedHealth Group', WMT: 'Walmart Inc.', MA: 'Mastercard Inc.',
  XOM: 'Exxon Mobil', JNJ: 'Johnson & Johnson', PG: 'Procter & Gamble',
  HD: 'Home Depot', COST: 'Costco',
  NFLX: 'Netflix Inc.', AMD: 'AMD', INTC: 'Intel Corp.',
  DIS: 'Walt Disney', PYPL: 'PayPal', BA: 'Boeing',
  CRM: 'Salesforce', ORCL: 'Oracle', CSCO: 'Cisco', ABT: 'Abbott Labs',
};
function getCompanyName(sym){ return COMPANY_NAME[sym] || sym; }

// ─── Company descriptions ───────────────────────────────────
const COMPANY_DESC = {
  // Vietnam
  VCB: 'Ngân hàng TMCP Ngoại thương Việt Nam - một trong những ngân hàng lớn nhất Việt Nam',
  BID: 'Ngân hàng TMCP Đầu tư và Phát triển Việt Nam - ngân hàng thương mại nhà nước',
  FPT: 'Tập đoàn FPT - công ty công nghệ thông tin hàng đầu Việt Nam',
  HPG: 'Tập đoàn Hòa Phát - doanh nghiệp sản xuất thép lớn nhất Việt Nam',
  VHM: 'Vinhomes - công ty bất động sản thuộc Vingroup',
  TCB: 'Techcombank - ngân hàng TMCP tư nhân hàng đầu Việt Nam',
  VNM: 'Vinamilk - công ty sữa lớn nhất Việt Nam và Đông Nam Á',
  MBB: 'Ngân hàng TMCP Quân đội - ngân hàng tăng trưởng nhanh tại Việt Nam',
  ACB: 'Ngân hàng TMCP Á Châu - một trong những ngân hàng TMCP hàng đầu',
  MSN: 'Tập đoàn Masan - tập đoàn đa ngành hàng đầu Việt Nam',
  HDB: 'HDBank - ngân hàng TMCP Phát triển TP.HCM',
  SSB: 'SeABank - ngân hàng TMCP Đông Nam Á',
  PLX: 'Petrolimex - Tập đoàn Xăng dầu Việt Nam',
  POW: 'PetroVietnam Power - công ty điện lực thuộc PVN',
  VIC: 'Vingroup - tập đoàn đa ngành lớn nhất Việt Nam',
  // International
  AAPL: 'Apple Inc. - Công ty công nghệ đa quốc gia Mỹ, sản xuất iPhone, Mac, iPad',
  MSFT: 'Microsoft Corporation - Tập đoàn công nghệ đa quốc gia, sở hữu Windows, Azure, Office',
  NVDA: 'NVIDIA Corporation - Công ty thiết kế chip đồ họa và AI hàng đầu thế giới',
  AMZN: 'Amazon.com Inc. - Công ty thương mại điện tử và điện toán đám mây lớn nhất',
  GOOGL: 'Alphabet Inc. - Công ty mẹ của Google, YouTube, Android',
  META: 'Meta Platforms Inc. - Công ty mẹ của Facebook, Instagram, WhatsApp',
  TSLA: 'Tesla Inc. - Công ty xe điện và năng lượng sạch của Elon Musk',
  JPM: 'JPMorgan Chase - Ngân hàng đầu tư lớn nhất nước Mỹ',
  NFLX: 'Netflix Inc. - Dịch vụ phát trực tuyến video hàng đầu thế giới',
  AMD: 'Advanced Micro Devices - Công ty bán dẫn, sản xuất CPU và GPU',
};
function getCompanyDesc(sym){ return COMPANY_DESC[sym] || ''; }

// ─── Watchlist (localStorage) ───────────────────────────────
let watchlist = new Set(JSON.parse(localStorage.getItem('watchlist')||'[]'));
function saveWatchlist(){ localStorage.setItem('watchlist',JSON.stringify([...watchlist])); }
function isInWatchlist(sym){ return watchlist.has(sym); }
function toggleWatchlist(sym){
  if(watchlist.has(sym)){ watchlist.delete(sym); }
  else { watchlist.add(sym); }
  saveWatchlist();
  renderTable();
  updateDrawerFavBtn();
  return watchlist.has(sym);
}

// ─── Sector mapping ────────────────────────────────────────
const SECTOR_MAP = {
  // Vietnam
  VCB:'Financial Services', BID:'Financial Services', FPT:'Technology',
  HPG:'Basic Materials', CTG:'Financial Services', VHM:'Real Estate',
  TCB:'Financial Services', VPB:'Financial Services', VNM:'Consumer Defensive',
  MBB:'Financial Services', GAS:'Energy', ACB:'Financial Services',
  MSN:'Consumer Defensive', GVR:'Basic Materials', LPB:'Financial Services',
  SSB:'Financial Services', STB:'Financial Services', VIB:'Financial Services',
  MWG:'Consumer Cyclical', HDB:'Financial Services',
  PLX:'Energy', POW:'Utilities', SAB:'Consumer Defensive',
  BCM:'Industrials', PDR:'Real Estate', KDH:'Real Estate',
  NVL:'Real Estate', DGC:'Basic Materials', SHB:'Financial Services',
  EIB:'Financial Services', VIC:'Real Estate', REE:'Industrials',
  VJC:'Transportation', GMD:'Industrials', TPB:'Financial Services',
  VRE:'Real Estate', VCI:'Financial Services', SSI:'Financial Services',
  HCM:'Financial Services', VGC:'Basic Materials', DPM:'Basic Materials',
  KBC:'Real Estate', DCM:'Basic Materials', VND:'Consumer Defensive',
  PNJ:'Consumer Cyclical', HNG:'Industrials', PVD:'Energy',
  DHG:'Healthcare', NT2:'Real Estate', DIG:'Real Estate',
  // International
  AAPL:'Technology', MSFT:'Technology', NVDA:'Technology', AMZN:'Consumer Cyclical',
  GOOGL:'Communication Services', META:'Communication Services',
  TSLA:'Consumer Cyclical', 'BRK-B':'Financial Services', LLY:'Healthcare',
  AVGO:'Technology', JPM:'Financial Services', V:'Financial Services',
  UNH:'Healthcare', WMT:'Consumer Defensive', MA:'Financial Services',
  XOM:'Energy', JNJ:'Healthcare', PG:'Consumer Defensive',
  HD:'Consumer Cyclical', COST:'Consumer Defensive',
  NFLX:'Communication Services', AMD:'Technology', INTC:'Technology',
  DIS:'Communication Services', PYPL:'Financial Services', BA:'Industrials',
  CRM:'Technology', ORCL:'Technology', CSCO:'Technology', ABT:'Healthcare',
};
function getSector(sym){ return SECTOR_MAP[sym] || 'Other'; }
function matchesMarket(sym){
  if(!marketFilter) return true;
  if(marketFilter==='vn') return VN_STOCKS.has(sym);
  if(marketFilter==='world') return WORLD_STOCKS.has(sym);
  return true;
}

// ─── DOM refs ───────────────────────────────────────────────
const $ = id => document.getElementById(id);
const el = {
  connBadge : $("connBadge"),
  connLabel : $("connBadge")?.querySelector(".label"),
  syncBtn   : $("syncBtn"),
  clock     : $("clock"),
  sTotal    : $("sTotal"),   sUp: $("sUp"),   sDown: $("sDown"),
  sFlat     : $("sFlat"),    sVol: $("sVol"), sTime: $("sTime"),
  body      : $("stockBody"),
  rowCount  : $("rowCount"),
  search    : $("searchInput"),
  sort      : $("sortSelect"),
  configuredCount: $("configuredCount"),
  tabs      : $("tabs"),
  newsGrid  : $("newsGrid"),
  chartDock : $("chartDock"),
  dockInterval: $("dockInterval"),
  dockCandleInterval: $("dockCandleInterval"),
  dockChartMode: $("dockChartMode"),
  dockChartTitle: $("dockChartTitle"),
  dockChart: $("dockChart"),
  dockRunlengthStrip: $("dockRunlengthStrip"),
  dockCandleInfo: $("dockCandleInfo"),
  drawer    : $("drawer"),
  overlay   : $("drawerOverlay"),
  drSymbol  : $("drSymbol"),
  drPrice   : $("drPrice"),
  drChange  : $("drChange"),
  drInfo    : $("drInfo"),
  drInterval: $("drInterval"),
  drCandleInterval: $("drCandleInterval"),
  drChartMode: $("drChartMode"),
  drChartTitle: $("drChartTitle"),
  drChart   : $("drChart"),
  drCandleInfo: $("drCandleInfo"),
  drCpInfo  : $("drCpInfo"),
  drCpChart : $("drCpChart"),
  drNews    : $("drNews"),
  drawerClose: $("drawerClose"),
  toasts    : $("toasts"),
  moMeta    : $("moMeta"),
  moBody    : $("moBody"),
  moBuyVal  : $("moBuyVal"),
  moTotalVal: $("moTotalVal"),
  moSellVal : $("moSellVal"),
  moBuyPct  : $("moBuyPct"),
  moSellPct : $("moSellPct"),
  moBuyBar  : $("moBuyBar"),
  moSellBar : $("moSellBar"),
  mobDepthBody: $("mobDepthBody"),
  mobRoundBtn: $("mobRoundBtn"),
  mobOddBtn: $("mobOddBtn"),
  mobBuyForceLbl: $("mobBuyForceLbl"),
  mobSellForceLbl: $("mobSellForceLbl"),
  mobBuyForceBar: $("mobBuyForceBar"),
  mobSellForceBar: $("mobSellForceBar"),
  symbolFormStatus: $("symbolFormStatus"),
};
let moTimer = null;  // matched orders auto-refresh timer
let moLotMode = "round";
let moLastRows = [];
let moLastTotalCount = 0;
let breadthChart = null;  // market breadth chart instance
let volumeTop10Chart = null; // top 10 volume chart instance
let currentTopTab = 'gainers'; // current top tab
let cpChart = null;
let cpTimer = null;
let cpSummaryTimer = null;
let overviewSignalChart = null;
let wlSignalChart = null;

/* ═══════════════════════════════════════════════════════════
   UTILITIES
   ═══════════════════════════════════════════════════════════ */
const fmt  = (n,d=2) => n==null||isNaN(n)?"--":Number(n).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const fmtV = v => {if(v==null)return"--";if(v>=1e9)return(v/1e9).toFixed(2)+"B";if(v>=1e6)return(v/1e6).toFixed(2)+"M";if(v>=1e3)return(v/1e3).toFixed(1)+"K";return v.toString()};
const fmtProb = n => n==null||isNaN(n)?"--":`${(Number(n)*100).toFixed(2)}%`;
const cls = v => v>0?"up":v<0?"down":"flat";

function isDarkTheme(){
  return document.documentElement?.getAttribute("data-theme") === "dark";
}

function cssVar(name, fallback){
  try{
    const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return value || fallback;
  }catch(_err){
    return fallback;
  }
}

function getChartPalette(){
  const dark = isDarkTheme();
  return {
    line: cssVar("--chart-line", dark ? "#4ea2ff" : "#1f7bff"),
    fillTop: cssVar("--chart-fill-top", dark ? "rgba(78,162,255,.30)" : "rgba(31,123,255,.24)"),
    fillBottom: cssVar("--chart-fill-bottom", dark ? "rgba(78,162,255,.04)" : "rgba(31,123,255,.03)"),
    grid: cssVar("--chart-grid", dark ? "rgba(167,189,225,.22)" : "rgba(86,111,153,.19)"),
    text: cssVar("--chart-text", dark ? "#d3def3" : "#5a6b86"),
    tooltipBg: cssVar("--chart-tooltip-bg", dark ? "#243a5a" : "#ffffff"),
    tooltipTitle: cssVar("--chart-tooltip-title", dark ? "#f4f8ff" : "#18253b"),
    tooltipBody: cssVar("--chart-tooltip-body", dark ? "#d3def3" : "#475a78"),
    tooltipBorder: cssVar("--chart-tooltip-border", dark ? "#5c78a8" : "#d5dfed"),
    up: cssVar("--green", "#10b981"),
    down: cssVar("--red", "#ef4444"),
    wick: cssVar("--chart-wick", dark ? "rgba(198,214,240,.92)" : "rgba(126,146,180,.9)"),
    volumeUp: cssVar("--chart-volume-up", dark ? "rgba(16,185,129,.36)" : "rgba(16,185,129,.34)"),
    volumeDown: cssVar("--chart-volume-down", dark ? "rgba(239,68,68,.34)" : "rgba(239,68,68,.32)"),
    selectionFill: cssVar("--chart-selection-fill", dark ? "rgba(96,165,250,.14)" : "rgba(56,139,253,.10)"),
    selectionStroke: cssVar("--chart-selection-stroke", dark ? "rgba(167,208,255,.86)" : "rgba(43,117,234,.58)"),
    selectionDot: cssVar("--chart-selection-dot", dark ? "rgba(96,165,250,.95)" : "rgba(31,123,255,.86)"),
  };
}

function _normSym(value){
  return String(value || "").trim().toUpperCase();
}

function _mlDirectionMeta(direction){
  const d = String(direction || "").toLowerCase();
  if(d === "up") return { key: "up", label: "Tang", klass: "up" };
  if(d === "down") return { key: "down", label: "Giam", klass: "down" };
  return { key: "flat", label: "Trung tinh", klass: "flat" };
}

function _bestAlertPerSymbol(alerts){
  const map = {};
  (alerts || []).forEach(row => {
    const sym = _normSym(row?.symbol);
    if(!sym) return;
    const prev = map[sym];
    if(!prev){
      map[sym] = row;
      return;
    }
    const rowScore = Number(row?.suspicion_score || 0);
    const prevScore = Number(prev?.suspicion_score || 0);
    if(rowScore > prevScore){
      map[sym] = row;
      return;
    }
    if(rowScore === prevScore && Number(row?.cp_prob || 0) > Number(prev?.cp_prob || 0)){
      map[sym] = row;
    }
  });
  return map;
}

function _hasMlForecast(row){
  if(!row) return false;
  return (
    row.ml_direction != null ||
    row.ml_prob_up != null ||
    row.ml_prob_down != null ||
    row.ml_expected_sessions != null
  );
}

function _mlForecastView(row){
  if(!_hasMlForecast(row)) return null;
  const meta = _mlDirectionMeta(row.ml_direction);
  const probUp = Number(row.ml_prob_up || 0);
  const probDown = Number(row.ml_prob_down || 0);
  const expectedRaw = Number(row.ml_expected_sessions);
  const expectedSessions = Number.isFinite(expectedRaw) && expectedRaw > 0 ? expectedRaw : 0;
  const expectedText = expectedSessions > 0 ? `${fmt(expectedSessions, 1)} phien` : "--";
  const text = row.ml_text || `Du kien ${expectedText} ${meta.label.toLowerCase()}`;
  return {
    ...meta,
    directionLabel: meta.label,
    probUp,
    probDown,
    expectedSessions,
    expectedText,
    text,
  };
}

function getSymbolMlForecast(symbol){
  const sym = _normSym(symbol);
  if(!sym) return null;
  return _mlForecastView(cpAlertMap[sym]);
}

function parseChartTime(value){
  if(value == null || value === "") return null;
  if(value instanceof Date) return isNaN(value) ? null : value;

  if(typeof value === "number"){
    const iv = value > 0 && value < 10_000_000_000 ? value * 1000 : value;
    const d = new Date(iv);
    return isNaN(d) ? null : d;
  }

  if(typeof value === "string"){
    const raw = value.trim();
    if(!raw) return null;

    if(/^\d+$/.test(raw)){
      let iv = Number(raw);
      if(!Number.isFinite(iv)) return null;
      if(iv < 10_000_000_000) iv *= 1000;
      const d = new Date(iv);
      return isNaN(d) ? null : d;
    }

    let normalized = raw;

    // Backend often serializes Scylla timestamps without timezone suffix.
    if(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(raw)){
      normalized = `${raw}Z`;
    }else if(/^\d{4}-\d{2}-\d{2}$/.test(raw)){
      normalized = `${raw}T00:00:00Z`;
    }

    const d = new Date(normalized);
    return isNaN(d) ? null : d;
  }

  const d = new Date(value);
  return isNaN(d) ? null : d;
}

function getRowTime(row){
  return parseChartTime(row?.ts || row?.bucket_ts || row?.trade_date || row?.bucket || row?.timestamp);
}

function normalizePriceSeries(rows){
  return (rows || [])
    .map(row => {
      const time = getRowTime(row);
      const close = row?.close ?? row?.price;
      const closeNum = Number(close);
      if(!time || !Number.isFinite(closeNum) || closeNum <= 0) return null;
      return {
        x: time,
        y: closeNum,
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.x - b.x);
}

function sanitizeRealtimeSeries(symbol, series){
  const cleaned = (series || [])
    .map(point => {
      const x = parseChartTime(point?.x);
      const y = Number(point?.y);
      if(!x || !Number.isFinite(y) || y <= 0) return null;
      return { x, y };
    })
    .filter(Boolean)
    .sort((a, b) => a.x - b.x);

  if(cleaned.length < 2) return cleaned;

  const ys = cleaned.map(item => item.y);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  if(!Number.isFinite(minY) || !Number.isFinite(maxY) || minY <= 0) return cleaned;

  const anchor = Number(stocks[symbol]?.price);
  const ref = Number.isFinite(anchor) && anchor > 0 ? anchor : cleaned[cleaned.length - 1].y;
  if(!Number.isFinite(ref) || ref <= 0) return cleaned;

  // If scale explodes (e.g. mixed markets with same ticker), keep points near realtime scale.
  if((maxY / minY) <= 20) return cleaned;

  const nearRef = cleaned.filter(item => item.y >= ref * 0.2 && item.y <= ref * 5);
  if(nearRef.length >= 2) return nearRef;

  if(nearRef.length === 1){
    const p = nearRef[0];
    return [{ x: new Date(p.x.getTime() - 60000), y: p.y }, p];
  }

  return cleaned;
}

function normalizeCandlestickSeries(rows){
  return (rows || [])
    .map(row => {
      const time = getRowTime(row);
      const open = Number(row?.open);
      const high = Number(row?.high);
      const low = Number(row?.low);
      const closeRaw = row?.close ?? row?.price;
      const close = Number(closeRaw);
      const volumeRaw = row?.volume ?? row?.day_volume;
      const volume = Number(volumeRaw);
      const exchange = row?.exchange != null ? String(row.exchange) : null;
      if(
        !time ||
        !Number.isFinite(open) ||
        !Number.isFinite(high) ||
        !Number.isFinite(low) ||
        !Number.isFinite(close) ||
        open <= 0 || high <= 0 || low <= 0 || close <= 0
      ){
        return null;
      }
      return {
        x: time,
        o: open,
        h: Math.max(high, open, close),
        l: Math.min(low, open, close),
        c: close,
        v: Number.isFinite(volume) ? volume : null,
        ex: exchange,
        key: String(time.getTime()),
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.x - b.x);
}

const INTERVAL_LABELS = {
  "15m": "15 phút",
  "1h": "1 giờ",
  "4h": "4 giờ",
  "1d": "1 ngày",
  "1w": "1 tuần",
  "1mo": "1 tháng",
  "3mo": "3 tháng",
  "1y": "1 năm",
  "5y": "5 năm",
};

const CANDLE_CYCLE_MINUTES = {
  "15m": 15,
  "1h": 60,
  "4h": 240,
  "1d": 1440,
  "1w": 10080,
  "1mo": 43200,
};

const RANGE_DEFAULT_CANDLE_CYCLE = {
  "1d": "15m",
  "1w": "1h",
  "1mo": "4h",
  "3mo": "1d",
  "1y": "1w",
  "5y": "1mo",
};

function intervalLabel(iv){
  return INTERVAL_LABELS[String(iv || "").toLowerCase()] || String(iv || "--");
}

function getBucketStartByCycle(dateLike, cycle){
  const d = new Date(dateLike);
  if(isNaN(d)) return null;

  switch(String(cycle || "").toLowerCase()){
    case "15m": {
      d.setUTCSeconds(0, 0);
      d.setUTCMinutes(Math.floor(d.getUTCMinutes() / 15) * 15);
      return d;
    }
    case "1h": {
      d.setUTCMinutes(0, 0, 0);
      return d;
    }
    case "4h": {
      d.setUTCMinutes(0, 0, 0);
      d.setUTCHours(Math.floor(d.getUTCHours() / 4) * 4);
      return d;
    }
    case "1d": {
      d.setUTCHours(0, 0, 0, 0);
      return d;
    }
    case "1w": {
      const weekday = (d.getUTCDay() + 6) % 7; // Monday = 0
      d.setUTCDate(d.getUTCDate() - weekday);
      d.setUTCHours(0, 0, 0, 0);
      return d;
    }
    case "1mo": {
      d.setUTCDate(1);
      d.setUTCHours(0, 0, 0, 0);
      return d;
    }
    default:
      return d;
  }
}

function inferSourceCandleMinutes(candles){
  if(!Array.isArray(candles) || candles.length < 2) return null;

  const deltas = [];
  for(let i = 1; i < candles.length; i += 1){
    const prev = candles[i - 1]?.x?.getTime?.();
    const curr = candles[i]?.x?.getTime?.();
    if(!Number.isFinite(prev) || !Number.isFinite(curr)) continue;
    const diffMinutes = (curr - prev) / 60000;
    if(Number.isFinite(diffMinutes) && diffMinutes > 0){
      deltas.push(diffMinutes);
    }
  }

  if(!deltas.length) return null;
  deltas.sort((a, b) => a - b);
  return deltas[Math.floor(deltas.length / 2)];
}

function getSuggestedCandleCycle(rangeInterval){
  const key = String(rangeInterval || "").toLowerCase();
  return RANGE_DEFAULT_CANDLE_CYCLE[key] || "1h";
}

function syncRecommendedCandleCycle(rangeInterval, options = {}){
  const suggested = getSuggestedCandleCycle(rangeInterval);
  if(!suggested) return;

  const force = !!options.force;
  if(!force && !candleCycleAuto) return;

  if(el.drCandleInterval) el.drCandleInterval.value = suggested;
  if(el.dockCandleInterval) el.dockCandleInterval.value = suggested;
}

function isVnSymbolForCandles(symbol){
  const sym = _normSym(symbol);
  if(!sym) return false;
  if(VN_STOCKS.has(sym)) return true;
  const exch = String(stocks?.[sym]?.exchange || "").toUpperCase();
  return exch === "VSE";
}

function applyCandlestickCycle(candles, requestedCycle, options = {}){
  const requested = String(requestedCycle || "1d").toLowerCase();
  if(!Array.isArray(candles) || !candles.length){
    return {
      candles: [],
      requested,
      resolved: requested,
      limitedBySource: false,
      sourceMinutes: null,
    };
  }

  const targetMinutes = CANDLE_CYCLE_MINUTES[requested] || CANDLE_CYCLE_MINUTES["1d"];
  const sourceMinutes = inferSourceCandleMinutes(candles);
  const keepDetailWhenCollapsed = !!options.keepDetailWhenCollapsed;

  // If requested cycle is finer than source data, keep source candles.
  if(sourceMinutes && targetMinutes < (sourceMinutes * 0.9)){
    return {
      candles,
      requested,
      resolved: requested,
      limitedBySource: true,
      sourceMinutes,
    };
  }

  if(sourceMinutes && Math.abs(targetMinutes - sourceMinutes) < Math.max(1, sourceMinutes * 0.12)){
    return {
      candles,
      requested,
      resolved: requested,
      limitedBySource: false,
      sourceMinutes,
    };
  }

  const grouped = new Map();
  candles.forEach(candle => {
    const bucket = getBucketStartByCycle(candle.x, requested);
    if(!bucket) return;
    const key = String(bucket.getTime());
    const volume = Number(candle.v);

    if(!grouped.has(key)){
      grouped.set(key, {
        x: bucket,
        o: candle.o,
        h: candle.h,
        l: candle.l,
        c: candle.c,
        v: Number.isFinite(volume) ? volume : null,
        ex: candle.ex,
        key,
      });
      return;
    }

    const agg = grouped.get(key);
    agg.h = Math.max(agg.h, candle.h);
    agg.l = Math.min(agg.l, candle.l);
    agg.c = candle.c;
    if(!agg.ex && candle.ex) agg.ex = candle.ex;
    if(Number.isFinite(volume)){
      agg.v = Number.isFinite(agg.v) ? (agg.v + volume) : volume;
    }
  });

  const groupedCandles = [...grouped.values()].sort((a, b) => a.x - b.x);
  if(keepDetailWhenCollapsed && groupedCandles.length <= 1 && candles.length >= 8){
    return {
      candles,
      requested,
      resolved: requested,
      limitedBySource: true,
      sourceMinutes,
    };
  }

  return {
    candles: groupedCandles,
    requested,
    resolved: requested,
    limitedBySource: false,
    sourceMinutes,
  };
}

function candleKey(candle){
  if(!candle) return "";
  if(candle.key) return String(candle.key);
  const t = candle.x?.getTime?.();
  return Number.isFinite(t) ? String(t) : "";
}

function resetSelectedCandles(symbol){
  selectedCandlesSymbol = symbol || null;
  selectedCandles = [];
}

function selectedCandleRows(candles){
  const map = new Map((candles || []).map(item => [candleKey(item), item]));
  selectedCandles = selectedCandles.filter(key => map.has(key));
  return selectedCandles.map(key => map.get(key)).filter(Boolean);
}

function toggleSelectedCandle(candle){
  if(!candle || !selected) return;
  if(selectedCandlesSymbol !== selected){
    resetSelectedCandles(selected);
  }
  const key = candleKey(candle);
  if(!key) return;
  const idx = selectedCandles.indexOf(key);
  if(idx >= 0){
    selectedCandles.splice(idx, 1);
  }else{
    selectedCandles.push(key);
  }
}

function setSelectedCandleRange(candles, startIdx, endIdx){
  if(!Array.isArray(candles) || !candles.length || !selected) return;
  if(selectedCandlesSymbol !== selected){
    resetSelectedCandles(selected);
  }

  const start = Math.max(0, Math.min(candles.length - 1, Math.floor(startIdx)));
  const end = Math.max(0, Math.min(candles.length - 1, Math.floor(endIdx)));
  const from = Math.min(start, end);
  const to = Math.max(start, end);

  selectedCandles = candles
    .slice(from, to + 1)
    .map(item => candleKey(item))
    .filter(Boolean);
}

function formatSelectionDuration(ms){
  if(!Number.isFinite(ms) || ms <= 0) return "0 phut";
  if(ms < 60_000) return `${Math.max(1, Math.round(ms / 1000))} giay`;
  if(ms < 3_600_000) return `${(ms / 60_000).toFixed(ms < 600_000 ? 1 : 0)} phut`;
  if(ms < 86_400_000) return `${(ms / 3_600_000).toFixed(ms < 21_600_000 ? 1 : 0)} gio`;
  return `${(ms / 86_400_000).toFixed(ms < 604_800_000 ? 1 : 0)} ngay`;
}

function buildSelectedCandleSummary(picked){
  const ordered = [...(picked || [])].sort((a, b) => a.x - b.x);
  if(!ordered.length) return null;

  const first = ordered[0];
  const last = ordered[ordered.length - 1];
  const open = Number(first.o);
  const close = Number(last.c);

  let high = -Infinity;
  let low = Infinity;
  let totalVolume = 0;
  let closeSum = 0;
  let weightedCloseSum = 0;
  let weightedVol = 0;

  ordered.forEach(item => {
    const h = Number(item.h);
    const l = Number(item.l);
    const c = Number(item.c);
    const v = Number.isFinite(item.v) ? Number(item.v) : 0;

    if(Number.isFinite(h) && h > high) high = h;
    if(Number.isFinite(l) && l < low) low = l;
    if(Number.isFinite(c)) closeSum += c;

    totalVolume += v;
    if(v > 0 && Number.isFinite(c)){
      weightedCloseSum += c * v;
      weightedVol += v;
    }
  });

  const change = Number.isFinite(open) && Number.isFinite(close) ? close - open : null;
  const changePct = Number.isFinite(change) && Number.isFinite(open) && open !== 0
    ? (change / open) * 100
    : null;

  return {
    ordered,
    first,
    last,
    open: Number.isFinite(open) ? open : null,
    close: Number.isFinite(close) ? close : null,
    high: Number.isFinite(high) ? high : null,
    low: Number.isFinite(low) ? low : null,
    totalVolume,
    avgVolume: totalVolume / Math.max(1, ordered.length),
    avgClose: closeSum / Math.max(1, ordered.length),
    weightedClose: weightedVol > 0 ? (weightedCloseSum / weightedVol) : null,
    change,
    changePct,
    durationMs: Math.max(0, last.x - first.x),
  };
}

function buildSelectionPreviewRows(ordered){
  if(!ordered.length) return { rows: [], truncated: false };
  if(ordered.length <= 24){
    return { rows: ordered, truncated: false };
  }

  const sample = [...ordered.slice(0, 2), ...ordered.slice(-2)];
  const seen = new Set();
  const rows = sample.filter(item => {
    const key = candleKey(item);
    if(!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  return { rows, truncated: true };
}

function abortCandleDragSelection(){
  candleDragSelection.active = false;
  candleDragSelection.canvasId = null;
  candleDragSelection.symbol = null;
  candleDragSelection.anchorKey = null;
  candleDragSelection.anchorIndex = -1;
  candleDragSelection.endIndex = -1;
  candleDragSelection.moved = false;
}

function beginCandlePanDrag(canvas, clientX){
  if(!canvas || !Number.isFinite(clientX)) return;
  candlePanDrag.active = true;
  candlePanDrag.canvasId = canvas.id;
  candlePanDrag.lastClientX = clientX;
  candlePanDrag.moved = false;
}

function abortCandlePanDrag(){
  candlePanDrag.active = false;
  candlePanDrag.canvasId = null;
  candlePanDrag.moved = false;
}

function finishCandlePanDrag(){
  if(!candlePanDrag.active) return;
  const moved = candlePanDrag.moved;
  abortCandlePanDrag();
  if(moved){
    candlePanDrag.suppressClickUntil = Date.now() + 260;
  }
  const activeCanvas = getActiveChartCanvas();
  if(activeCanvas) activeCanvas.style.cursor = "crosshair";
}

function updateCandlePanDrag(clientX){
  if(!candlePanDrag.active || !selected || !isCandlestickMode()) return false;

  const canvas = candlePanDrag.canvasId === "dockChart" ? el.dockChart : el.drChart;
  if(!canvas || canvas !== getActiveChartCanvas()) return false;

  const hitState = getCandlestickStateForCanvas(canvas);
  if(!hitState || !Number.isFinite(hitState.step) || hitState.step <= 0) return false;

  const dx = clientX - candlePanDrag.lastClientX;
  if(Math.abs(dx) < hitState.step) return false;

  const rawSteps = Math.trunc(dx / hitState.step);
  if(rawSteps === 0) return false;

  const direction = rawSteps < 0 ? -1 : 1;
  let changed = false;
  for(let i = 0; i < Math.abs(rawSteps); i += 1){
    if(!panCandlestickViewport(direction)) break;
    changed = true;
  }

  candlePanDrag.lastClientX += rawSteps * hitState.step;

  if(changed){
    candlePanDrag.moved = true;
    redrawActiveCandlestickChart();
    canvas.style.cursor = "grabbing";
  }
  return changed;
}

function getCandlestickStateForCanvas(canvas){
  const state = activeCandleHitState;
  if(!state || !canvas) return null;
  if(state.canvasId !== canvas.id) return null;
  if(state.symbol !== selected) return null;
  if(!state.candles?.length) return null;
  return state;
}

function getCandleIndexFromClientX(hitState, canvas, clientX, clampToPlot = false){
  if(!hitState || !canvas || !Number.isFinite(hitState.step) || hitState.step <= 0) return -1;
  const rect = canvas.getBoundingClientRect();
  const minX = hitState.padLeft;
  const maxX = hitState.padLeft + hitState.plotWidth;

  let localX = clientX - rect.left;
  if(clampToPlot){
    localX = Math.max(minX, Math.min(maxX - 0.001, localX));
  }else if(localX < minX || localX > maxX){
    return -1;
  }

  const idx = Math.floor((localX - hitState.padLeft) / hitState.step);
  return Math.max(0, Math.min(hitState.candles.length - 1, idx));
}

function resolveDragAnchorIndex(hitState){
  if(!hitState?.candles?.length) return -1;

  if(candleDragSelection.anchorKey){
    const found = hitState.candles.findIndex(item => candleKey(item) === candleDragSelection.anchorKey);
    if(found >= 0) return found;
  }

  const idx = Number.isFinite(candleDragSelection.anchorIndex)
    ? Math.floor(candleDragSelection.anchorIndex)
    : 0;
  return Math.max(0, Math.min(hitState.candles.length - 1, idx));
}

function beginCandleDragSelection(canvas, hitState, anchorIdx){
  if(!canvas || !hitState || anchorIdx < 0) return;

  candleDragSelection.active = true;
  candleDragSelection.canvasId = canvas.id;
  candleDragSelection.symbol = selected;
  candleDragSelection.anchorIndex = anchorIdx;
  candleDragSelection.endIndex = anchorIdx;
  candleDragSelection.anchorKey = candleKey(hitState.candles[anchorIdx]);
  candleDragSelection.moved = false;
}

function updateCandleDragSelection(clientX){
  if(!candleDragSelection.active || !selected || !isCandlestickMode()) return false;

  const canvas = candleDragSelection.canvasId === "dockChart" ? el.dockChart : el.drChart;
  if(!canvas || canvas !== getActiveChartCanvas()) return false;

  const hitState = getCandlestickStateForCanvas(canvas);
  if(!hitState) return false;

  const anchorIdx = resolveDragAnchorIndex(hitState);
  if(anchorIdx < 0) return false;

  const nextIdx = getCandleIndexFromClientX(hitState, canvas, clientX, true);
  if(nextIdx < 0) return false;
  if(nextIdx === candleDragSelection.endIndex && candleDragSelection.moved) return false;

  candleDragSelection.endIndex = nextIdx;
  candleDragSelection.moved = candleDragSelection.moved || nextIdx !== anchorIdx;
  if(!candleDragSelection.moved) return false;

  setSelectedCandleRange(hitState.candles, anchorIdx, nextIdx);
  renderCandleSelectionInfo();
  redrawActiveCandlestickChart();
  canvas.style.cursor = "ew-resize";
  return true;
}

function finishCandleDragSelection(clientX = null){
  if(!candleDragSelection.active) return;

  if(Number.isFinite(clientX)){
    updateCandleDragSelection(clientX);
  }

  const moved = candleDragSelection.moved;
  abortCandleDragSelection();
  if(moved){
    candleDragSelection.suppressClickUntil = Date.now() + 280;
    renderCandleSelectionInfo();
    redrawActiveCandlestickChart();
  }

  const activeCanvas = getActiveChartCanvas();
  if(activeCanvas) activeCanvas.style.cursor = "crosshair";
}

function setCandleInfoPanelsVisible(visible){
  [el.dockCandleInfo, el.drCandleInfo].forEach(panel => {
    if(!panel) return;
    panel.classList.toggle("hidden", !visible);
  });
}

function renderCandleSelectionInfo(){
  const panels = [el.dockCandleInfo, el.drCandleInfo].filter(Boolean);
  if(!panels.length) return;

  const visible = !!selected && !!el.drawer?.classList.contains("open") && isCandlestickMode();
  setCandleInfoPanelsVisible(visible);
  if(!visible){
    panels.forEach(panel => { panel.innerHTML = ""; });
    return;
  }

  const candles = Array.isArray(drawerCandles) ? drawerCandles : [];
  if(selectedCandlesSymbol !== selected){
    resetSelectedCandles(selected);
  }

  if(!candles.length){
    const emptyHtml = '<div class="candle-info-empty">Chua co du lieu nen. Thu doi interval khac.</div>';
    panels.forEach(panel => { panel.innerHTML = emptyHtml; });
    return;
  }

  const picked = selectedCandleRows(candles);
  const currentExchange = stocks[selected]?.exchange || "--";

  if(!picked.length){
    const html = `
      <div class="candle-info-head">
        <div>
          <strong>Chi tiet nen</strong>
           <span>Bam de chon tung nen, keo chuot trai de chon vung nhieu nen, giu Alt + keo de luot du lieu, giu Shift + lan de qua trai/phai, giu Ctrl + lan de thu/phong.</span>
        </div>
      </div>
      <div class="candle-info-empty">San hien tai: <strong>${currentExchange}</strong></div>
    `;
    panels.forEach(panel => { panel.innerHTML = html; });
    return;
  }

  const summary = buildSelectedCandleSummary(picked);
  if(!summary){
    panels.forEach(panel => { panel.innerHTML = '<div class="candle-info-empty">Khong tong hop duoc du lieu nen da chon.</div>'; });
    return;
  }

  const moveClass = summary.change > 0 ? "positive" : (summary.change < 0 ? "negative" : "neutral");
  const changeText = Number.isFinite(summary.change)
    ? `${summary.change >= 0 ? "+" : ""}${fmt(summary.change, 2)}`
    : "--";
  const changePctText = Number.isFinite(summary.changePct)
    ? `${summary.changePct >= 0 ? "+" : ""}${fmt(summary.changePct, 2)}%`
    : "--";
  const weightedCloseText = Number.isFinite(summary.weightedClose) ? fmt(summary.weightedClose, 2) : "--";
  const preview = buildSelectionPreviewRows(summary.ordered);

  const infoRows = preview.rows.map(item => {
    const exchange = item.ex || currentExchange;
    return `
      <div class="candle-info-item">
        <div class="stamp">${fmtDT(item.x)}</div>
        <div class="meta">O/H/L/C: ${fmt(item.o, 2)} / ${fmt(item.h, 2)} / ${fmt(item.l, 2)} / ${fmt(item.c, 2)}</div>
        <div class="meta">Khoi luong: ${Number.isFinite(item.v) ? fmtV(item.v) : "--"} | San: ${exchange || "--"}</div>
      </div>
    `;
  }).join("");

  const truncateNote = preview.truncated
    ? `<div class="candle-info-note">Da chon ${summary.ordered.length} nen. Dang hien 4 moc dai dien, thu phong chart de xem chi tiet hon.</div>`
    : "";

  const html = `
    <div class="candle-info-head">
      <div>
        <strong>Tong hop ${summary.ordered.length} nen</strong>
        <span>${fmtDT(summary.first.x)} → ${fmtDT(summary.last.x)} (${formatSelectionDuration(summary.durationMs)})</span>
      </div>
      <button type="button" class="candle-info-clear" data-action="clear-candle-select">Xoa chon</button>
    </div>
    <div class="candle-summary-grid">
      <div class="candle-summary-item">
        <span>Open dau ky</span>
        <strong>${fmt(summary.open, 2)}</strong>
      </div>
      <div class="candle-summary-item">
        <span>Close cuoi ky</span>
        <strong>${fmt(summary.close, 2)}</strong>
      </div>
      <div class="candle-summary-item">
        <span>High / Low</span>
        <strong>${fmt(summary.high, 2)} / ${fmt(summary.low, 2)}</strong>
      </div>
      <div class="candle-summary-item ${moveClass}">
        <span>Bien dong</span>
        <strong>${changeText} (${changePctText})</strong>
      </div>
      <div class="candle-summary-item">
        <span>Tong KL</span>
        <strong>${fmtV(summary.totalVolume)}</strong>
      </div>
      <div class="candle-summary-item">
        <span>KL TB / nen</span>
        <strong>${fmtV(summary.avgVolume)}</strong>
      </div>
      <div class="candle-summary-item">
        <span>Close TB</span>
        <strong>${fmt(summary.avgClose, 2)}</strong>
      </div>
      <div class="candle-summary-item">
        <span>Close weighted theo KL</span>
        <strong>${weightedCloseText}</strong>
      </div>
    </div>
    ${truncateNote}
    <div class="candle-info-list">${infoRows}</div>
  `;
  panels.forEach(panel => { panel.innerHTML = html; });
}

function clearCandleHitState(){
  abortCandleDragSelection();
  abortCandlePanDrag();
  activeCandleHitState = null;
  if(el.dockChart) el.dockChart.style.cursor = "default";
  if(el.drChart) el.drChart.style.cursor = "default";
}

function candleViewportKey(symbol){
  return `${_normSym(symbol)}|${getActiveChartInterval()}`;
}

function resetCandleViewport(){
  candleViewport = { key: null, start: 0, end: 0 };
}

function lineViewportKey(symbol){
  return `${_normSym(symbol)}|${getActiveChartInterval()}`;
}

function resetLineViewport(){
  lineViewport = { key: null, start: 0, end: 0, stickToRight: true };
}

function getDefaultLineViewportSize(interval, total){
  const iv = String(interval || "1d").toLowerCase();
  const map = {
    "1d": 16,
    "1w": 32,
    "1mo": 48,
    "3mo": 72,
    "1y": 96,
    "5y": 140,
  };
  const fallback = Math.min(total, 80);
  return Math.min(total, map[iv] || fallback);
}

function getNextLineHistoryInterval(interval){
  const iv = String(interval || "1d").toLowerCase();
  const expandMap = {
    "1d": "1w",
    "1w": "1mo",
    "1mo": "3mo",
    "3mo": "1y",
    "1y": "5y",
  };
  return expandMap[iv] || null;
}

function tryAutoExpandLineHistory(trigger = ""){
  if(!selected || isCandlestickMode()) return false;
  if(lineAutoExpandInFlight) return false;

  const currentIv = String(getActiveChartInterval() || "1d").toLowerCase();
  const nextIv = getNextLineHistoryInterval(currentIv);
  if(!nextIv) return false;

  const now = Date.now();
  const key = `${selected}|${currentIv}->${nextIv}`;
  if(lineAutoExpandLastKey === key && (now - lineAutoExpandLastAt) < 1400){
    return false;
  }
  lineAutoExpandLastKey = key;
  lineAutoExpandLastAt = now;
  lineAutoExpandInFlight = true;

  syncChartIntervals({ value: nextIv });
  candleCycleAuto = true;
  syncRecommendedCandleCycle(nextIv, { force: true });
  selectedRunlengthSegmentId = "__all";
  candleMinuteDetailLoading = false;
  resetCandleViewport();
  resetLineViewport();
  if(selected) resetSelectedCandles(selected);
  renderCandleSelectionInfo();
  updatePriceChartTitle();

  const triggerText = trigger ? ` (${trigger})` : "";
  toast(`Mo rong du lieu line sang ${intervalLabel(nextIv)}${triggerText}`, "ok");

  loadOHLCV(selected, nextIv, {
    onComplete: () => {
      lineAutoExpandInFlight = false;
    },
  });
  return true;
}

function getLineSeriesForViewport(symbol, series){
  if(!symbol || !Array.isArray(series) || !series.length){
    resetLineViewport();
    return [];
  }

  const key = lineViewportKey(symbol);
  const max = series.length;
  if(lineViewport.key !== key){
    const windowSize = getDefaultLineViewportSize(getActiveChartInterval(), max);
    const start = Math.max(0, max - windowSize);
    lineViewport = { key, start, end: max, stickToRight: true };
  }

  let start = Number.isFinite(lineViewport.start) ? Math.floor(lineViewport.start) : 0;
  let end = Number.isFinite(lineViewport.end) ? Math.floor(lineViewport.end) : max;
  let count = end - start;
  let stickToRight = lineViewport.stickToRight !== false;

  if(count < 2){
    const windowSize = Math.max(2, getDefaultLineViewportSize(getActiveChartInterval(), max));
    start = Math.max(0, max - windowSize);
    end = max;
    count = end - start;
    stickToRight = true;
  }

  if(stickToRight){
    end = max;
    start = Math.max(0, end - count);
  }

  if(start < 0) start = 0;
  if(end > max) end = max;
  if(start >= end){
    const windowSize = Math.max(2, getDefaultLineViewportSize(getActiveChartInterval(), max));
    start = Math.max(0, max - windowSize);
    end = max;
  }

  const atRightEdge = end >= max;
  lineViewport = { key, start, end, stickToRight: atRightEdge ? true : stickToRight };
  return series.slice(start, end);
}

function zoomLineViewport(zoomIn, focusRatio = 0.5){
  if(!selected || !drawerLineSeries?.length || drawerLineSymbol !== selected) return false;

  const fullSeries = drawerLineSeries;
  const key = lineViewportKey(selected);
  if(lineViewport.key !== key){
    lineViewport = { key, start: 0, end: fullSeries.length, stickToRight: true };
  }

  let start = Number.isFinite(lineViewport.start) ? Math.floor(lineViewport.start) : 0;
  let end = Number.isFinite(lineViewport.end) ? Math.floor(lineViewport.end) : fullSeries.length;
  let count = end - start;
  if(count <= 0){
    start = 0;
    end = fullSeries.length;
    count = end - start;
  }

  const minCount = Math.min(12, fullSeries.length);
  const maxCount = fullSeries.length;

  let nextCount = count;
  if(zoomIn){
    nextCount = Math.max(minCount, Math.floor(count * 0.86));
  }else{
    nextCount = Math.min(maxCount, Math.ceil(count * 1.16));
  }
  if(nextCount === count) return false;

  const clampedFocus = Math.max(0.05, Math.min(0.95, focusRatio));
  const anchorIndex = start + Math.round(clampedFocus * Math.max(count - 1, 0));
  let nextStart = Math.round(anchorIndex - clampedFocus * Math.max(nextCount - 1, 0));
  let nextEnd = nextStart + nextCount;

  if(nextStart < 0){
    nextStart = 0;
    nextEnd = nextCount;
  }
  if(nextEnd > maxCount){
    nextEnd = maxCount;
    nextStart = Math.max(0, nextEnd - nextCount);
  }

  lineViewport = {
    key,
    start: nextStart,
    end: nextEnd,
    stickToRight: nextEnd >= maxCount,
  };
  return true;
}

function panLineViewport(direction){
  if(!selected || !Array.isArray(drawerLineSeries) || !drawerLineSeries.length || drawerLineSymbol !== selected){
    return false;
  }

  const fullSeries = drawerLineSeries;
  const key = lineViewportKey(selected);
  if(lineViewport.key !== key){
    getLineSeriesForViewport(selected, fullSeries);
  }

  let start = Number.isFinite(lineViewport.start) ? Math.floor(lineViewport.start) : 0;
  let end = Number.isFinite(lineViewport.end) ? Math.floor(lineViewport.end) : fullSeries.length;
  let count = end - start;

  if(count <= 0){
    start = 0;
    end = fullSeries.length;
    count = end - start;
  }
  if(count >= fullSeries.length) return false;

  const dir = direction > 0 ? 1 : -1;
  const step = Math.max(1, Math.floor(count * 0.12));
  let nextStart = start + dir * step;
  nextStart = Math.max(0, Math.min(fullSeries.length - count, nextStart));
  const nextEnd = nextStart + count;

  if(nextStart === start && nextEnd === end) return false;

  lineViewport = {
    key,
    start: nextStart,
    end: nextEnd,
    stickToRight: nextEnd >= fullSeries.length,
  };
  return true;
}

function beginLinePanDrag(canvas, clientX){
  if(!canvas || !Number.isFinite(clientX)) return false;
  if(isCandlestickMode() || !selected || drawerLineSymbol !== selected) return false;

  const visible = getLineSeriesForViewport(selected, drawerLineSeries);
  if(!visible.length || visible.length >= drawerLineSeries.length) return false;

  linePanDrag.active = true;
  linePanDrag.canvasId = canvas.id;
  linePanDrag.lastClientX = clientX;
  canvas.style.cursor = "grabbing";
  return true;
}

function abortLinePanDrag(){
  linePanDrag.active = false;
  linePanDrag.canvasId = null;
}

function finishLinePanDrag(){
  if(!linePanDrag.active) return;
  const canvas = linePanDrag.canvasId === "dockChart" ? el.dockChart : el.drChart;
  abortLinePanDrag();
  if(canvas && !isCandlestickMode()){
    const total = Array.isArray(drawerLineSeries) ? drawerLineSeries.length : 0;
    const visible = total > 0 ? getLineSeriesForViewport(selected, drawerLineSeries) : [];
    canvas.style.cursor = total > visible.length ? "grab" : "default";
  }
}

function updateLinePanDrag(clientX){
  if(!linePanDrag.active || isCandlestickMode() || !selected || drawerLineSymbol !== selected) return false;

  const canvas = linePanDrag.canvasId === "dockChart" ? el.dockChart : el.drChart;
  if(!canvas || canvas !== getActiveChartCanvas()) return false;

  if(!Array.isArray(drawerLineSeries) || drawerLineSeries.length < 3) return false;
  const visible = getLineSeriesForViewport(selected, drawerLineSeries);
  const visibleCount = Math.max(2, visible.length);
  const width = Math.max(1, canvas.clientWidth || canvas.getBoundingClientRect().width || 1);
  const stepPx = Math.max(6, width / Math.max(visibleCount - 1, 1));

  const dx = clientX - linePanDrag.lastClientX;
  if(Math.abs(dx) < stepPx) return false;

  const rawSteps = Math.trunc(dx / stepPx);
  if(rawSteps === 0) return false;

  const direction = rawSteps < 0 ? -1 : 1;
  let changed = false;
  for(let i = 0; i < Math.abs(rawSteps); i += 1){
    if(!panLineViewport(direction)) break;
    changed = true;
  }

  linePanDrag.lastClientX += rawSteps * stepPx;

  if(changed){
    refreshRealtimeLineViewport();
    canvas.style.cursor = "grabbing";
  }else if(direction < 0){
    tryAutoExpandLineHistory("drag-left");
  }
  return changed;
}

function getDefaultCandleViewportSize(interval, total){
  const iv = String(interval || "1d").toLowerCase();
  const map = {
    "1d": 20,
    "1w": 48,
    "1mo": 72,
    "3mo": 96,
    "1y": 120,
    "5y": 180,
  };
  const fallback = Math.min(total, 120);
  return Math.min(total, map[iv] || fallback);
}

function getCandlesForViewport(symbol, candles){
  if(!symbol || !Array.isArray(candles) || !candles.length){
    resetCandleViewport();
    return [];
  }

  const key = candleViewportKey(symbol);
  if(candleViewport.key !== key){
    const max = candles.length;
    const windowSize = getDefaultCandleViewportSize(getActiveChartInterval(), max);
    const start = Math.max(0, max - windowSize);
    candleViewport = { key, start, end: max };
  }

  const max = candles.length;
  let start = Number.isFinite(candleViewport.start) ? Math.floor(candleViewport.start) : 0;
  let end = Number.isFinite(candleViewport.end) ? Math.floor(candleViewport.end) : max;

  if(start < 0) start = 0;
  if(end > max) end = max;
  if(end - start < 2){
    start = Math.max(0, end - Math.min(max, 60));
    end = max;
  }
  if(start >= end){
    start = 0;
    end = max;
  }

  candleViewport.start = start;
  candleViewport.end = end;
  return candles.slice(start, end);
}

function zoomCandlestickViewport(zoomIn, focusRatio = 0.5){
  if(!selected || !drawerCandles?.length || drawerCandleSymbol !== selected) return false;

  const fullCandles = drawerCandles;
  const key = candleViewportKey(selected);
  if(candleViewport.key !== key){
    candleViewport = { key, start: 0, end: fullCandles.length };
  }

  let start = candleViewport.start;
  let end = candleViewport.end;
  let count = end - start;
  if(count <= 0){
    start = 0;
    end = fullCandles.length;
    count = end - start;
  }

  const minCount = Math.min(16, fullCandles.length);
  const maxCount = fullCandles.length;

  let nextCount = count;
  if(zoomIn){
    nextCount = Math.max(minCount, Math.floor(count * 0.85));
  }else{
    nextCount = Math.min(maxCount, Math.ceil(count * 1.18));
  }
  if(nextCount === count) return false;

  const clampedFocus = Math.max(0.05, Math.min(0.95, focusRatio));
  const anchorIndex = start + Math.round(clampedFocus * Math.max(count - 1, 0));
  let nextStart = Math.round(anchorIndex - clampedFocus * Math.max(nextCount - 1, 0));
  let nextEnd = nextStart + nextCount;

  if(nextStart < 0){
    nextStart = 0;
    nextEnd = nextCount;
  }
  if(nextEnd > maxCount){
    nextEnd = maxCount;
    nextStart = Math.max(0, nextEnd - nextCount);
  }

  candleViewport = { key, start: nextStart, end: nextEnd };
  return true;
}

function panCandlestickViewport(direction){
  if(!selected || !drawerCandles?.length || drawerCandleSymbol !== selected) return false;

  const fullCandles = drawerCandles;
  const key = candleViewportKey(selected);
  if(candleViewport.key !== key){
    candleViewport = { key, start: 0, end: fullCandles.length };
  }

  let start = Number.isFinite(candleViewport.start) ? Math.floor(candleViewport.start) : 0;
  let end = Number.isFinite(candleViewport.end) ? Math.floor(candleViewport.end) : fullCandles.length;
  let count = end - start;

  if(count <= 0){
    start = 0;
    end = fullCandles.length;
    count = end - start;
  }
  if(count >= fullCandles.length) return false;

  const dir = direction > 0 ? 1 : -1;
  const step = Math.max(1, Math.floor(count * 0.12));
  let nextStart = start + dir * step;
  nextStart = Math.max(0, Math.min(fullCandles.length - count, nextStart));
  const nextEnd = nextStart + count;

  if(nextStart === start && nextEnd === end) return false;

  candleViewport = { key, start: nextStart, end: nextEnd };
  return true;
}

function normalizeChangepointSeries(rows){
  return (rows || [])
    .map(row => {
      const time = parseChartTime(row?.event_time || row?.timestamp || row?.bucket_ts || row?.ts);
      if(!time) return null;
      const cpProb = Number(row?.cp_prob || 0);
      const mapRunLength = Number(row?.map_run_length || 0);
      const expectedRunLength = Number(row?.expected_run_length || 0);
      const whaleScore = Number(row?.whale_score || 0);
      const price = Number(row?.price);
      return {
        x: time,
        cpProb: Number.isFinite(cpProb) ? cpProb : 0,
        mapRunLength: Number.isFinite(mapRunLength) ? mapRunLength : 0,
        expectedRunLength: Number.isFinite(expectedRunLength) ? expectedRunLength : 0,
        whaleScore: Number.isFinite(whaleScore) ? whaleScore : 0,
        regimeLabel: String(row?.regime_label || "stable"),
        price: Number.isFinite(price) ? price : null,
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.x - b.x);
}

function _runSegmentTone(regime){
  const key = String(regime || "").toLowerCase();
  if(key === "whale-watch") return "whale";
  if(key === "transition") return "transition";
  return "stable";
}

function _formatRunSegmentTime(date, withDate){
  if(!(date instanceof Date) || isNaN(date)) return "--";
  if(withDate){
    return date.toLocaleString("vi-VN", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
  }
  return date.toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" });
}

function _filterChangepointInCandleWindow(cpRows, candles){
  if(!cpRows?.length || !candles?.length) return [];
  const startMs = candles[0].x.getTime();
  const endMs = candles[candles.length - 1].x.getTime();
  return cpRows.filter(item => {
    const t = item.x?.getTime?.();
    return Number.isFinite(t) && t >= startMs && t <= endMs;
  });
}

function buildRunlengthSegments(cpRows){
  if(!cpRows?.length) return [];

  const segments = [];
  let current = null;

  for(let i = 0; i < cpRows.length; i++){
    const point = cpRows[i];
    const prev = i > 0 ? cpRows[i - 1] : null;

    const cpSpike = point.cpProb >= 0.2 && (!prev || prev.cpProb < 0.2);
    const runReset = !!prev && prev.mapRunLength >= 3 && point.mapRunLength + 1 < prev.mapRunLength;
    const regimeSwitch = !!prev && point.regimeLabel !== prev.regimeLabel && point.cpProb >= 0.12;
    const shouldBreak = !current || cpSpike || runReset || regimeSwitch;

    if(shouldBreak){
      if(current && prev?.x) current.end = prev.x;
      current = {
        id: `seg-${segments.length + 1}`,
        start: point.x,
        end: point.x,
        maxRunLength: Math.max(0, point.mapRunLength || 0),
        maxCpProb: Math.max(0, point.cpProb || 0),
        maxWhale: Math.max(0, point.whaleScore || 0),
        samples: 0,
        regimeVotes: {},
        regimeLabel: "stable",
      };
      segments.push(current);
    }

    current.end = point.x;
    current.samples += 1;
    current.maxRunLength = Math.max(current.maxRunLength, point.mapRunLength || 0);
    current.maxCpProb = Math.max(current.maxCpProb, point.cpProb || 0);
    current.maxWhale = Math.max(current.maxWhale, point.whaleScore || 0);
    const regimeKey = String(point.regimeLabel || "stable");
    current.regimeVotes[regimeKey] = (current.regimeVotes[regimeKey] || 0) + 1;
  }

  segments.forEach(seg => {
    const entries = Object.entries(seg.regimeVotes || {});
    if(entries.length){
      entries.sort((a,b) => b[1] - a[1]);
      seg.regimeLabel = entries[0][0];
    }
  });

  return segments;
}

function renderRunlengthStrip(segments, symbol){
  const strip = el.dockRunlengthStrip;
  if(!strip) return;

  const canShow = isCandlestickMode() && !!selected && selected === symbol && isDockChartActive();
  if(!canShow){
    strip.innerHTML = "";
    return;
  }

  const list = Array.isArray(segments) ? segments : [];
  if(selectedRunlengthSegmentId !== "__all" && !list.some(seg => seg.id === selectedRunlengthSegmentId)){
    selectedRunlengthSegmentId = "__all";
  }

  const spanMs = list.length
    ? Math.max(0, list[list.length - 1].end?.getTime?.() - list[0].start?.getTime?.())
    : 0;
  const withDate = spanMs > 1000 * 60 * 60 * 24;

  const chips = [
    `<button class="rl-chip overview ${selectedRunlengthSegmentId === "__all" ? "active" : ""}" data-seg="__all">Tong quat</button>`,
    ...list.map((seg, idx) => {
      const tone = _runSegmentTone(seg.regimeLabel);
      const from = _formatRunSegmentTime(seg.start, withDate);
      const to = _formatRunSegmentTime(seg.end, withDate);
      const cpPct = `${(Number(seg.maxCpProb || 0) * 100).toFixed(0)}%`;
      const text = `R${idx + 1} ${from}→${to} | r max ${fmt(seg.maxRunLength, 0)} | CP ${cpPct}`;
      const active = selectedRunlengthSegmentId === seg.id ? "active" : "";
      return `<button class="rl-chip ${tone} ${active}" data-seg="${seg.id}">${text}</button>`;
    }),
  ];

  strip.innerHTML = chips.join("");
}

function getCandlestickOverlay(symbol, candles){
  if(!symbol || !candles?.length) return null;
  if(drawerCpHistorySymbol !== symbol || !drawerCpHistory?.length){
    drawerRunlengthSegments = [];
    renderRunlengthStrip([], symbol);
    return null;
  }

  const inWindow = _filterChangepointInCandleWindow(drawerCpHistory, candles);
  if(!inWindow.length){
    drawerRunlengthSegments = [];
    renderRunlengthStrip([], symbol);
    return null;
  }

  drawerRunlengthSegments = buildRunlengthSegments(inWindow);
  renderRunlengthStrip(drawerRunlengthSegments, symbol);

  return {
    points: inWindow,
    segments: drawerRunlengthSegments,
    activeSegmentId: selectedRunlengthSegmentId,
  };
}

function buildRealtimeSeriesFromCandles(candles){
  return (candles || []).map(item => ({ x: item.x, y: item.c }));
}

function upsertRealtimeLinePoint(symbol, price, rawTime){
  if(!symbol || !Number.isFinite(Number(price))) return;
  if(drawerLineSymbol !== symbol){
    drawerLineSymbol = symbol;
    drawerLineSeries = [];
  }

  const pointTime = parseChartTime(rawTime) || new Date();
  const numericPrice = Number(price);
  const last = drawerLineSeries[drawerLineSeries.length - 1];

  if(last && Math.abs(pointTime.getTime() - last.x.getTime()) < 800){
    last.x = pointTime;
    last.y = numericPrice;
  }else{
    drawerLineSeries.push({ x: pointTime, y: numericPrice });
  }

  if(drawerLineSeries.length > MAX_LINE_POINTS){
    drawerLineSeries = drawerLineSeries.slice(-MAX_LINE_POINTS);
  }

  drawerLineSeries = sanitizeRealtimeSeries(symbol, drawerLineSeries);
}

function buildRealtimeLineGradient(canvas, palette){
  const ctx = canvas?.getContext?.("2d");
  if(!ctx) return palette.fillTop;
  const h = Math.max(220, canvas.clientHeight || canvas.height || 320);
  const gradient = ctx.createLinearGradient(0, 0, 0, h);
  gradient.addColorStop(0, palette.fillTop);
  gradient.addColorStop(1, palette.fillBottom);
  return gradient;
}

function resolveRealtimeXAxisMeta(series, canvas){
  const firstMs = series?.[0]?.x?.getTime?.();
  const lastMs = series?.[series.length - 1]?.x?.getTime?.();
  const spanMs = (Number.isFinite(firstMs) && Number.isFinite(lastMs))
    ? Math.max(0, lastMs - firstMs)
    : 0;

  const minute = 60 * 1000;
  const hour = 60 * minute;
  const day = 24 * hour;
  const monthApprox = 30 * day;

  let unit = "minute";
  if(spanMs > (45 * day)){
    unit = "month";
  }else if(spanMs > (2 * day)){
    unit = "day";
  }else if(spanMs > (3 * hour)){
    unit = "hour";
  }

  const width = Math.max(320, canvas?.clientWidth || canvas?.width || 820);
  const roughLabelWidth = unit === "month" ? 96 : unit === "day" ? 82 : 108;
  const maxTicksLimit = Math.max(4, Math.min(14, Math.floor(width / roughLabelWidth)));

  return {
    unit,
    maxTicksLimit,
    tooltipFormat: spanMs >= monthApprox ? "dd/MM/yyyy" : "dd/MM/yyyy HH:mm",
    displayFormats: {
      minute: "HH:mm",
      hour: "dd/MM HH:mm",
      day: "dd/MM",
      week: "dd/MM",
      month: "MM/yyyy",
      quarter: "MM/yyyy",
      year: "yyyy",
    },
  };
}

function buildRealtimePriceGuidePlugin(palette){
  return {
    id: "realtimePriceGuide",
    afterDatasetsDraw(chartInstance){
      const datasets = chartInstance?.data?.datasets || [];
      const lineDs = datasets.find(ds => ds?.yAxisID === "y" && ds?.type !== "bar");
      const points = lineDs?.data || [];
      if(!points.length) return;

      const latest = points[points.length - 1];
      const latestPrice = Number(latest?.y);
      if(!Number.isFinite(latestPrice)) return;

      const yScale = chartInstance?.scales?.y;
      const area = chartInstance?.chartArea;
      const ctx = chartInstance?.ctx;
      if(!yScale || !area || !ctx) return;

      const y = yScale.getPixelForValue(latestPrice);
      if(!Number.isFinite(y) || y < area.top || y > area.bottom) return;

      ctx.save();

      // Draw horizontal dashed price guide similar to the reference style.
      ctx.beginPath();
      ctx.setLineDash([4, 3]);
      ctx.lineWidth = 1;
      ctx.strokeStyle = palette.line;
      ctx.moveTo(area.left, y);
      ctx.lineTo(area.right, y);
      ctx.stroke();
      ctx.setLineDash([]);

      const text = fmt(latestPrice, 2);
      ctx.font = "600 11px JetBrains Mono, monospace";
      ctx.textBaseline = "middle";

      const padX = 7;
      const boxH = 18;
      const boxW = Math.ceil(ctx.measureText(text).width + (padX * 2));
      const boxX = Math.max(area.left + 2, area.right - boxW - 2);
      const boxY = Math.max(area.top + 2, Math.min(y - (boxH / 2), area.bottom - boxH - 2));
      const radius = 4;

      ctx.fillStyle = palette.line;
      ctx.beginPath();
      ctx.moveTo(boxX + radius, boxY);
      ctx.lineTo(boxX + boxW - radius, boxY);
      ctx.quadraticCurveTo(boxX + boxW, boxY, boxX + boxW, boxY + radius);
      ctx.lineTo(boxX + boxW, boxY + boxH - radius);
      ctx.quadraticCurveTo(boxX + boxW, boxY + boxH, boxX + boxW - radius, boxY + boxH);
      ctx.lineTo(boxX + radius, boxY + boxH);
      ctx.quadraticCurveTo(boxX, boxY + boxH, boxX, boxY + boxH - radius);
      ctx.lineTo(boxX, boxY + radius);
      ctx.quadraticCurveTo(boxX, boxY, boxX + radius, boxY);
      ctx.closePath();
      ctx.fill();

      ctx.fillStyle = "#ffffff";
      ctx.fillText(text, boxX + padX, boxY + (boxH / 2));
      ctx.restore();
    },
  };
}

function buildRealtimeVolumeSeries(symbol, lineSeries){
  if(drawerCandleSymbol !== symbol || !Array.isArray(drawerCandles) || !drawerCandles.length){
    return [];
  }
  if(!Array.isArray(lineSeries) || !lineSeries.length){
    return [];
  }

  const volumeByTs = new Map();
  drawerCandles.forEach(candle => {
    const ts = candle?.x?.getTime?.();
    const vol = Number(candle?.v);
    if(Number.isFinite(ts) && Number.isFinite(vol) && vol > 0){
      volumeByTs.set(ts, vol);
    }
  });
  if(!volumeByTs.size){
    return [];
  }

  let prevPrice = null;
  const rawSeries = lineSeries
    .map(point => {
      const ts = point?.x?.getTime?.();
      const price = Number(point?.y);
      if(!Number.isFinite(ts) || !Number.isFinite(price)){
        return null;
      }
      const volume = Number(volumeByTs.get(ts));
      if(!Number.isFinite(volume) || volume <= 0){
        prevPrice = price;
        return null;
      }
      const dir = prevPrice == null || price >= prevPrice ? "up" : "down";
      prevPrice = price;
      return { x: point.x, volume, dir };
    })
    .filter(Boolean);

  if(!rawSeries.length){
    return [];
  }

  // Keep bars inside the bottom strip of the realtime chart (TradingView-like).
  const maxVolume = rawSeries.reduce((acc, item) => Math.max(acc, item.volume), 0);
  if(!Number.isFinite(maxVolume) || maxVolume <= 0){
    return [];
  }
  // Keep volume bars in a thin strip at the bottom (similar to TradingView).
  const volumeBand = 0.08;
  const minVisible = 0.003;

  return rawSeries.map(item => {
    const ratio = Math.max(0, Math.min(1, item.volume / maxVolume));
    const scaled = Math.max(minVisible, ratio * volumeBand);
    return {
      x: item.x,
      y: scaled,
      raw_volume: item.volume,
      dir: item.dir,
    };
  });
}

function buildRealtimeDatasets(canvas, symbol, series, palette){
  const datasets = [];
  const volumeSeries = buildRealtimeVolumeSeries(symbol, series);
  if(volumeSeries.length){
    datasets.push({
      type: "bar",
      label: `${symbol} volume`,
      data: volumeSeries,
      yAxisID: "yVol",
      backgroundColor: ctx => ctx.raw?.dir === "down" ? palette.volumeDown : palette.volumeUp,
      borderWidth: 0,
      maxBarThickness: 5,
      barPercentage: 0.76,
      categoryPercentage: 0.9,
      order: 0,
    });
  }

  datasets.push({
    type: "line",
    label: `${symbol} realtime`,
    data: series,
    borderColor: palette.line,
    backgroundColor: buildRealtimeLineGradient(canvas, palette),
    fill: true,
    tension: 0.22,
    pointRadius: 0,
    pointHoverRadius: 2.5,
    pointHitRadius: 14,
    borderWidth: 2.35,
    yAxisID: "y",
    order: 1,
  });

  return datasets;
}

function drawRealtimeLineChart(canvas, series, sym){
  if(chart){chart.destroy();chart=null}
  if(!series?.length){
    if(canvas) canvas.style.cursor = "default";
    drawCanvasEmpty(canvas, "Chua co du lieu realtime");
    return;
  }

  const palette = getChartPalette();
  const xMeta = resolveRealtimeXAxisMeta(series, canvas);

  chart = new Chart(canvas, {
    type: "line",
    plugins: [buildRealtimePriceGuidePlugin(palette)],
    data: {
      datasets: buildRealtimeDatasets(canvas, sym, series, palette),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      normalized: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          mode: "index",
          intersect: false,
          backgroundColor: palette.tooltipBg,
          titleColor: palette.tooltipTitle,
          bodyColor: palette.tooltipBody,
          borderColor: palette.tooltipBorder,
          borderWidth: 1,
          callbacks: {
            label: ctx => {
              const y = Number(ctx.parsed?.y);
              if(ctx.dataset?.yAxisID === "yVol"){
                const raw = Number(ctx.raw?.raw_volume);
                return `KL: ${fmtV(Number.isFinite(raw) ? raw : y)}`;
              }
              return `Gia: ${fmt(y, 2)}`;
            },
          },
        },
      },
      scales: {
        x: {
          type: "time",
          time: {
            tooltipFormat: xMeta.tooltipFormat,
            unit: xMeta.unit,
            displayFormats: xMeta.displayFormats,
          },
          ticks: { color: palette.text, maxTicksLimit: xMeta.maxTicksLimit },
          grid: { color: palette.grid },
        },
        y: {
          position: "right",
          ticks: {
            color: palette.text,
            callback: value => fmt(Number(value), 2),
          },
          grid: { color: palette.grid },
        },
        yVol: {
          display: false,
          min: 0,
          max: 1,
          beginAtZero: true,
          ticks: { display: false },
          grid: { display: false },
        },
      },
    },
  });

  const total = Array.isArray(drawerLineSeries) ? drawerLineSeries.length : series.length;
  if(canvas) canvas.style.cursor = total > series.length ? "grab" : "default";
}

function updateRealtimeLineChart(sym){
  if(!chart || chart.config?.type !== "line") return;
  const fullSeries = drawerLineSymbol === sym ? drawerLineSeries : [];
  if(!fullSeries.length) return;

  const series = getLineSeriesForViewport(sym, fullSeries);
  if(!series.length) return;

  const palette = getChartPalette();
  const activeCanvas = chart.canvas || getActiveChartCanvas();
  const xMeta = resolveRealtimeXAxisMeta(series, activeCanvas);
  chart.data.datasets = buildRealtimeDatasets(activeCanvas, sym, series, palette);
  if(chart.options?.plugins?.tooltip){
    chart.options.plugins.tooltip.backgroundColor = palette.tooltipBg;
    chart.options.plugins.tooltip.titleColor = palette.tooltipTitle;
    chart.options.plugins.tooltip.bodyColor = palette.tooltipBody;
    chart.options.plugins.tooltip.borderColor = palette.tooltipBorder;
  }
  if(chart.options?.scales?.x?.ticks) chart.options.scales.x.ticks.color = palette.text;
  if(chart.options?.scales?.x?.ticks) chart.options.scales.x.ticks.maxTicksLimit = xMeta.maxTicksLimit;
  if(chart.options?.scales?.x?.time){
    chart.options.scales.x.time.tooltipFormat = xMeta.tooltipFormat;
    chart.options.scales.x.time.unit = xMeta.unit;
    chart.options.scales.x.time.displayFormats = xMeta.displayFormats;
  }
  if(chart.options?.scales?.x?.grid) chart.options.scales.x.grid.color = palette.grid;
  if(chart.options?.scales?.y?.ticks) chart.options.scales.y.ticks.color = palette.text;
  if(chart.options?.scales?.y?.grid) chart.options.scales.y.grid.color = palette.grid;
  chart.update("none");

  const canvas = getActiveChartCanvas();
  if(canvas && !isCandlestickMode()){
    canvas.style.cursor = fullSeries.length > series.length ? "grab" : "default";
  }
}

function refreshRealtimeLineViewport(){
  if(isCandlestickMode() || !selected) return;

  const canvas = getActiveChartCanvas();
  if(!canvas) return;

  updatePriceChartTitle();
  if(chart && chart.config?.type === "line"){
    updateRealtimeLineChart(selected);
  }else{
    redrawActivePriceChart();
  }
}

function redrawActivePriceChart(){
  if(!selected) return;
  const canvas = getActiveChartCanvas();
  if(!canvas) return;

  updatePriceChartTitle();

  if(isCandlestickMode()){
    if(!drawerCandles?.length || drawerCandleSymbol !== selected){
      clearCandleHitState();
      renderRunlengthStrip([], selected);
      renderCandleSelectionInfo();
      drawCanvasEmpty(canvas, "Chua co du lieu nen OHLCV");
      return;
    }
    const visibleCandles = getCandlesForViewport(selected, drawerCandles);
    if(!visibleCandles.length){
      clearCandleHitState();
      renderRunlengthStrip([], selected);
      renderCandleSelectionInfo();
      drawCanvasEmpty(canvas, "Khung thu/phong dang trong. Nhan Ctrl+0 de reset.");
      return;
    }
    const overlay = getCandlestickOverlay(selected, visibleCandles);
    if(chart){chart.destroy();chart=null}
    drawCandlestickChart(canvas, visibleCandles, overlay);
    renderCandleSelectionInfo();
    return;
  }

  clearCandleHitState();
  renderRunlengthStrip([], selected);
  renderCandleSelectionInfo();
  if(drawerLineSymbol !== selected){
    drawerLineSymbol = selected;
    drawerLineSeries = [];
    resetLineViewport();
  }

  if(!drawerLineSeries.length && drawerCandles?.length && drawerCandleSymbol === selected){
    drawerLineSeries = buildRealtimeSeriesFromCandles(drawerCandles).slice(-MAX_LINE_POINTS);
  }

  if(!drawerLineSeries.length){
    const row = stocks[selected];
    if(row && Number.isFinite(Number(row.price))){
      upsertRealtimeLinePoint(selected, Number(row.price), row.date || new Date());
    }
  }

  const visibleLineSeries = getLineSeriesForViewport(selected, drawerLineSeries);
  drawRealtimeLineChart(canvas, visibleLineSeries, selected);
}

function redrawActiveCandlestickChart(){
  if(!isCandlestickMode()) return;
  redrawActivePriceChart();
}

function drawCandlestickChart(canvas, candles, overlay = null){
  const ctx = canvas?.getContext("2d");
  if(!ctx) return;

  const dpr = window.devicePixelRatio || 1;
  const cssWidth = Math.max(1, Math.floor(canvas.clientWidth || canvas.parentElement?.clientWidth || 640));
  const cssHeight = Math.max(1, Math.floor(canvas.clientHeight || canvas.parentElement?.clientHeight || 320));

  canvas.width = Math.floor(cssWidth * dpr);
  canvas.height = Math.floor(cssHeight * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssWidth, cssHeight);

  const padLeft = 56;
  const padRight = 12;
  const padTop = 12;
  const padBottom = 24;
  const plotWidth = cssWidth - padLeft - padRight;
  const plotHeight = cssHeight - padTop - padBottom;
  if(plotWidth <= 12 || plotHeight <= 12) return;

  let minLow = Infinity;
  let maxHigh = -Infinity;
  candles.forEach(c => {
    if(c.l < minLow) minLow = c.l;
    if(c.h > maxHigh) maxHigh = c.h;
  });
  if(!Number.isFinite(minLow) || !Number.isFinite(maxHigh)) return;

  const rawSpan = Math.max(maxHigh - minLow, 1e-12);
  const yPad = rawSpan * 0.06;
  const yMin = minLow - yPad;
  const yMax = maxHigh + yPad;
  const ySpan = Math.max(yMax - yMin, 1e-12);
  const yToPx = value => padTop + ((yMax - value) / ySpan) * plotHeight;

  const palette = getChartPalette();
  const darkTheme = isDarkTheme();
  const gridColor = palette.grid;
  const textColor = palette.text;
  const upColor = palette.up;
  const downColor = palette.down;
  const wickColor = palette.wick;
  const cpLineRGB = darkTheme ? "255,132,150" : "224,76,90";
  const whaleRGB = darkTheme ? "244,114,182" : "218,76,152";
  const timeMin = candles[0].x.getTime();
  const timeMax = candles[candles.length - 1].x.getTime();
  const timeSpan = Math.max(timeMax - timeMin, 1);
  const toX = dateLike => {
    const t = dateLike?.getTime?.();
    if(!Number.isFinite(t)) return padLeft;
    const ratio = Math.max(0, Math.min(1, (t - timeMin) / timeSpan));
    return padLeft + ratio * plotWidth;
  };

  const overlaySegments = Array.isArray(overlay?.segments) ? overlay.segments : [];
  const overlayPoints = Array.isArray(overlay?.points) ? overlay.points : [];
  const activeSegmentId = overlay?.activeSegmentId || "__all";

  if(overlaySegments.length){
    overlaySegments.forEach(seg => {
      const x1 = Math.max(padLeft, Math.min(padLeft + plotWidth, toX(seg.start)));
      const x2 = Math.max(padLeft, Math.min(padLeft + plotWidth, toX(seg.end)));
      const width = Math.max(1, x2 - x1);
      const tone = _runSegmentTone(seg.regimeLabel);
      const isFocus = activeSegmentId === "__all" || activeSegmentId === seg.id;
      const alpha = activeSegmentId === "__all" ? 0.08 : (isFocus ? 0.2 : 0.03);
      let fill = `rgba(16,185,129,${alpha})`;
      if(tone === "transition") fill = `rgba(245,158,11,${alpha})`;
      if(tone === "whale") fill = `rgba(244,114,182,${alpha})`;
      ctx.fillStyle = fill;
      ctx.fillRect(x1, padTop, width, plotHeight);
      if(activeSegmentId !== "__all" && activeSegmentId === seg.id){
        ctx.strokeStyle = palette.selectionStroke;
        ctx.lineWidth = 1;
        ctx.strokeRect(x1 + 0.5, padTop + 0.5, Math.max(1, width - 1), Math.max(1, plotHeight - 1));
      }
    });
  }

  ctx.lineWidth = 1;
  ctx.strokeStyle = gridColor;
  ctx.fillStyle = textColor;
  ctx.font = "11px Inter, sans-serif";
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";

  const yTicks = 5;
  for(let i = 0; i < yTicks; i++){
    const ratio = i / (yTicks - 1);
    const y = padTop + ratio * plotHeight;
    const value = yMax - ratio * ySpan;
    ctx.beginPath();
    ctx.moveTo(padLeft, y);
    ctx.lineTo(padLeft + plotWidth, y);
    ctx.stroke();
    ctx.fillText(fmt(value, 2), padLeft - 6, y);
  }

  const step = plotWidth / candles.length;
  const bodyWidth = Math.max(3, Math.min(14, step * 0.68));
  const selectedSet = new Set(selectedCandles);
  const selectedIndexes = [];
  candles.forEach((item, idx) => {
    if(selectedSet.has(candleKey(item))) selectedIndexes.push(idx);
  });
  const hasRangeSelection = selectedIndexes.length > 1;
  const firstSelectedIdx = hasRangeSelection ? selectedIndexes[0] : -1;
  const lastSelectedIdx = hasRangeSelection ? selectedIndexes[selectedIndexes.length - 1] : -1;
  const denseRangeSelection = selectedIndexes.length > 16;

  if(hasRangeSelection){
    const x1 = padLeft + firstSelectedIdx * step;
    const x2 = padLeft + (lastSelectedIdx + 1) * step;
    const w = Math.max(1, x2 - x1);
    ctx.fillStyle = palette.selectionFill;
    ctx.fillRect(x1, padTop, w, plotHeight);
    ctx.strokeStyle = palette.selectionStroke;
    ctx.lineWidth = 1;
    ctx.strokeRect(x1 + 0.5, padTop + 0.5, Math.max(1, w - 1), Math.max(1, plotHeight - 1));
  }

  candles.forEach((candle, idx) => {
    const x = padLeft + (idx + 0.5) * step;
    const yOpen = yToPx(candle.o);
    const yClose = yToPx(candle.c);
    const yHigh = yToPx(candle.h);
    const yLow = yToPx(candle.l);
    const isUp = candle.c >= candle.o;
    const bodyColor = isUp ? upColor : downColor;

    ctx.strokeStyle = wickColor;
    ctx.beginPath();
    ctx.moveTo(x, yHigh);
    ctx.lineTo(x, yLow);
    ctx.stroke();

    const top = Math.min(yOpen, yClose);
    const bodyHeight = Math.max(1, Math.abs(yClose - yOpen));
    ctx.fillStyle = bodyColor;
    ctx.strokeStyle = bodyColor;
    if(bodyHeight <= 1.5){
      ctx.beginPath();
      ctx.moveTo(x - bodyWidth / 2, yClose);
      ctx.lineTo(x + bodyWidth / 2, yClose);
      ctx.stroke();
    }else{
      ctx.fillRect(x - bodyWidth / 2, top, bodyWidth, bodyHeight);
    }

    if(selectedSet.has(candleKey(candle))){
      const isRangeEdge = !hasRangeSelection || idx === firstSelectedIdx || idx === lastSelectedIdx;
      if(denseRangeSelection && !isRangeEdge){
        return;
      }

      ctx.strokeStyle = palette.selectionStroke;
      ctx.lineWidth = 1.5;
      ctx.strokeRect(x - bodyWidth / 2 - 1, top - 1, bodyWidth + 2, bodyHeight + 2);
      ctx.fillStyle = palette.selectionDot;
      ctx.beginPath();
      ctx.arc(x, padTop + plotHeight + 11, 3, 0, Math.PI * 2);
      ctx.fill();
    }
  });

  if(overlayPoints.length){
    overlayPoints.forEach(point => {
      const x = toX(point.x);
      const cpStrength = Math.max(0, Math.min(1, Number(point.cpProb || 0)));
      const whaleStrength = Math.max(0, Math.min(1, Number(point.whaleScore || 0)));

      if(cpStrength >= 0.14){
        const cpAlpha = 0.08 + cpStrength * (darkTheme ? 0.5 : 0.42);
        ctx.strokeStyle = `rgba(${cpLineRGB},${cpAlpha})`;
        ctx.lineWidth = cpStrength >= 0.28 ? 1.4 : 1;
        ctx.beginPath();
        ctx.moveTo(x, padTop);
        ctx.lineTo(x, padTop + plotHeight);
        ctx.stroke();
      }

      if(whaleStrength >= 0.2){
        const priceGuess = Number.isFinite(point.price)
          ? point.price
          : candles[Math.max(0, Math.min(candles.length - 1, Math.round(((point.x.getTime() - timeMin) / timeSpan) * (candles.length - 1))))]?.c;
        const y = yToPx(Number.isFinite(priceGuess) ? priceGuess : candles[candles.length - 1].c);
        const radius = 2 + Math.min(4, whaleStrength * 6);
        ctx.fillStyle = `rgba(${whaleRGB},${0.35 + whaleStrength * 0.45})`;
        ctx.beginPath();
        ctx.arc(x, y, radius, 0, Math.PI * 2);
        ctx.fill();
      }
    });

    ctx.textAlign = "left";
    ctx.textBaseline = "top";
    ctx.font = "11px Inter, sans-serif";
    const legendY = padTop + 6;
    ctx.fillStyle = `rgba(${cpLineRGB},.85)`;
    ctx.fillRect(padLeft + 6, legendY, 10, 2);
    ctx.fillStyle = textColor;
    ctx.fillText("CP line", padLeft + 20, legendY - 4);

    ctx.fillStyle = `rgba(${whaleRGB},.9)`;
    ctx.beginPath();
    ctx.arc(padLeft + 96, legendY + 1, 3, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = textColor;
    ctx.fillText("Whale", padLeft + 104, legendY - 4);
  }

  const timeSpanMs = candles.length > 1 ? (candles[candles.length - 1].x - candles[0].x) : 0;
  const xTicks = Math.min(6, candles.length);
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  ctx.fillStyle = textColor;

  for(let i = 0; i < xTicks; i++){
    const idx = xTicks === 1 ? 0 : Math.round((i * (candles.length - 1)) / (xTicks - 1));
    const candle = candles[idx];
    const x = padLeft + (idx + 0.5) * step;
    const label = timeSpanMs >= (1000 * 60 * 60 * 24 * 2)
      ? candle.x.toLocaleDateString("vi-VN", { day: "2-digit", month: "2-digit" })
      : candle.x.toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" });

    ctx.beginPath();
    ctx.strokeStyle = gridColor;
    ctx.moveTo(x, padTop + plotHeight);
    ctx.lineTo(x, padTop + plotHeight + 4);
    ctx.stroke();

    ctx.fillText(label, x, padTop + plotHeight + 6);
  }

  const otherCanvas = canvas.id === "dockChart" ? el.drChart : el.dockChart;
  if(otherCanvas) otherCanvas.style.cursor = "default";
  canvas.style.cursor = "crosshair";
  activeCandleHitState = {
    canvasId: canvas.id,
    symbol: selected,
    candles,
    padLeft,
    padTop,
    plotWidth,
    plotHeight,
    step,
  };
}

const fmtTime = t => {const d=parseChartTime(t);return d?d.toLocaleTimeString("vi-VN",{hour:"2-digit",minute:"2-digit",second:"2-digit"}):"--"};
const fmtDate = t => {const d=parseChartTime(t);return d?d.toLocaleDateString("vi-VN"):String(t||"--")};
const fmtDT   = t => {const d=parseChartTime(t);return d?d.toLocaleString("vi-VN"):String(t||"--")};

function toast(msg, type="ok"){
  if(!el.toasts) return;
  const t=document.createElement("div");
  t.className=`toast ${type}`;t.textContent=msg;
  el.toasts.appendChild(t);
  setTimeout(()=>{t.style.opacity="0";t.style.transform="translateX(100%)";setTimeout(()=>t.remove(),300)},3500);
}

function setSymbolFormStatus(message="", type=""){
  if(!el.symbolFormStatus) return;
  el.symbolFormStatus.textContent = message;
  el.symbolFormStatus.className = `symbol-form-status${type ? ` ${type}` : ""}`;
}

function drawCanvasEmpty(canvas, message){
  if(!canvas) return;
  const palette = getChartPalette();
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0,0,canvas.width,canvas.height);
  ctx.fillStyle = palette.text;
  ctx.font = "600 13px Inter";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(message, canvas.width / 2, canvas.height / 2);
}

function ingestChangepoint(rows){
  if(!Array.isArray(rows)) return;
  cpSignals = {};
  rows.forEach(row => {
    const sym = String(row?.symbol || '').toUpperCase();
    if(!sym) return;
    cpSignals[sym] = row;
  });
  if(Object.keys(stocks).length) renderTable();
  if(_ovData) renderOverviewSignalChart();
  const isWatchlistTab = document.querySelector(".tab.active")?.dataset.tab === "watchlist";
  if(isWatchlistTab){
    const wlStocks = [...watchlist].filter(sym=>stocks[sym]);
    if(wlStocks.length){
      scheduleWatchlistCardRefresh(wlStocks);
      renderWatchlistSignalChart(wlStocks);
    }
  }
}

function ingestChangepointAlerts(payload){
  const envelope = Array.isArray(payload) ? { alerts: payload } : (payload || {});
  const alerts = Array.isArray(envelope.alerts) ? envelope.alerts : [];
  cpAlerts = alerts;
  cpAlertMap = _bestAlertPerSymbol(cpAlerts);
  cpAlertSummary = envelope.summary || envelope.alert_summary || null;
  if(Object.keys(stocks).length) renderTable();
  if(selected && cpSignals[selected]){
    renderChangepointInfo(cpSignals[selected], selected);
  }
  if(_ovData) renderOverviewAbnormalBoard();
}

async function loadChangepointSummary(silent=true){
  try{
    const [respLatest, respAbnormal] = await Promise.all([
      fetch(`${API}/api/changepoint/latest`),
      fetch(`${API}/api/changepoint/abnormal?limit=50`),
    ]);
    const jsonLatest = await respLatest.json();
    const jsonAbnormal = await respAbnormal.json();
    if(respLatest.ok && jsonLatest.status === "ok"){
      ingestChangepoint(jsonLatest.data || []);
      if(respAbnormal.ok && jsonAbnormal.status === "ok"){
        ingestChangepointAlerts(jsonAbnormal.data || {});
      }
    }else if(!silent){
      throw new Error(jsonLatest.detail || "Khong tai duoc BOCPD summary");
    }
  }catch(err){
    console.error("loadChangepointSummary error:", err);
    if(!silent) toast(`BOCPD loi: ${err.message || err}`, "err");
  }
}

async function loadSymbolRegistry(){
  try{
    const r = await fetch(`${API}/api/system/symbols`);
    const j = await r.json();
    if(j.status !== "ok") throw new Error("bad registry response");
    const markets = j.data?.markets || {};
    symbolConfig = {
      vn: markets.vn?.symbols || [],
      world: markets.world?.symbols || [],
    };
    VN_STOCKS = new Set(symbolConfig.vn);
    WORLD_STOCKS = new Set(symbolConfig.world);
    const allowedSymbols = new Set([...symbolConfig.vn, ...symbolConfig.world]);
    const cleanedWatchlist = [...watchlist].filter(sym => allowedSymbols.has(sym));
    if(cleanedWatchlist.length !== watchlist.size){
      watchlist = new Set(cleanedWatchlist);
      saveWatchlist();
    }
    if(el.configuredCount){
      el.configuredCount.textContent = `Cau hinh: ${j.data?.total_symbols || 0} ma`;
    }
    if(Object.keys(stocks).length){
      renderTable();
      updateStats();
    }
    return j.data;
  }catch(err){
    console.error("loadSymbolRegistry error:", err);
    if(el.configuredCount){
      el.configuredCount.textContent = "Cau hinh: loi tai";
    }
    return null;
  }
}

async function resyncData(showToast=true){
  try{
    if(el.syncBtn){
      el.syncBtn.disabled = true;
      el.syncBtn.textContent = "Dang dong bo...";
    }
    const [registryResp, latestResp, cpResp, cpAlertResp] = await Promise.all([
      loadSymbolRegistry(),
      fetch(`${API}/api/stocks/latest`).then(r=>r.json()),
      fetch(`${API}/api/changepoint/latest`).then(r=>r.json()).catch(()=>({status:"err"})),
      fetch(`${API}/api/changepoint/abnormal?limit=50`).then(r=>r.json()).catch(()=>({status:"err"})),
    ]);
    if(latestResp.status === "ok"){
      ingest(latestResp.data || [], true);
    } else {
      throw new Error(latestResp.detail || "Khong tai duoc du lieu bang gia");
    }
    if(cpResp.status === "ok"){
      ingestChangepoint(cpResp.data || []);
    }
    if(cpAlertResp.status === "ok"){
      ingestChangepointAlerts(cpAlertResp.data || {});
    }
    if(document.querySelector(".tab.active")?.dataset.tab === "news"){
      loadAllNews();
    }
    if(showToast){
      const total = registryResp?.total_symbols ?? Object.keys(stocks).length;
      toast(`Dong bo xong ${total} ma`, "ok");
    }
  }catch(err){
    console.error("resyncData error:", err);
    if(showToast) toast(`Dong bo that bai: ${err.message || err}`, "err");
  }finally{
    if(el.syncBtn){
      el.syncBtn.disabled = false;
      el.syncBtn.textContent = "Reconnect";
    }
  }
}

// Normalise row: auto-detect schema by checking which fields exist
function norm(row){
  // stock_latest_prices has "price" + "timestamp"
  // stock_daily_summary has "close" + "trade_date"
  const isLatest = ("price" in row && "timestamp" in row);

  if(isLatest){
    return {
      symbol: row.symbol,
      price:  row.price,
      change: row.change||0,
      pct:    row.change_percent||0,
      open:   row.open!=null?row.open:null,
      high:   row.high!=null?row.high:null,
      low:    row.low!=null?row.low:null,
      volume: row.day_volume||row.volume||null,
      vwap:   row.vwap!=null?row.vwap:null,
      exchange: row.exchange,
      date:   row.timestamp,
      isRealtime: true,
      market_hours: row.market_hours,
      last_size: row.last_size,
    };
  }
  // daily summary
  return {
    symbol: row.symbol,
    price:  row.close,
    change: row.change||0,
    pct:    row.change_percent||0,
    open:   row.open,
    high:   row.high,
    low:    row.low,
    volume: row.volume,
    vwap:   row.vwap,
    exchange: row.exchange,
    date:   row.trade_date,
    isRealtime: false,
    market_hours: row.market_hours,
    last_size: null,
  };
}

/* ═══════════════════════════════════════════════════════════
   CLOCK
   ═══════════════════════════════════════════════════════════ */
setInterval(()=>{
  el.clock.textContent=new Date().toLocaleTimeString("vi-VN",{hour:"2-digit",minute:"2-digit",second:"2-digit"});
},1000);

/* ═══════════════════════════════════════════════════════════
   WEBSOCKET
   ═══════════════════════════════════════════════════════════ */
function connect(){
  if(ws&&(ws.readyState===0||ws.readyState===1))return;
  ws=new WebSocket(WS_URL);

  ws.onopen=()=>{
    el.connBadge.className="conn-badge ok";
    el.connLabel.textContent="Đã kết nối";
    toast("WebSocket kết nối thành công","ok");
    if(retryTimer){clearTimeout(retryTimer);retryTimer=null}
    if(pingTimer) clearInterval(pingTimer);
    pingTimer=setInterval(()=>{if(ws&&ws.readyState===1)ws.send(JSON.stringify({type:"ping"}))},20000);
  };
  ws.onmessage=e=>{try{handle(JSON.parse(e.data))}catch(err){console.error(err)}};
  ws.onclose=()=>{el.connBadge.className="conn-badge err";el.connLabel.textContent="Mất kết nối";retry()};
  ws.onerror=()=>{el.connBadge.className="conn-badge err";el.connLabel.textContent="Lỗi"};
}
function retry(){if(!retryTimer)retryTimer=setTimeout(()=>{retryTimer=null;connect()},3000)}

/* ═══════════════════════════════════════════════════════════
   MESSAGE HANDLER
   ═══════════════════════════════════════════════════════════ */
function handle(msg){
  switch(msg.type){
    case "snapshot":
    case "price_update":
      dataSource = msg.source || "daily";
      ingest(msg.data, msg.type==="snapshot");
      break;
    case "ohlcv_data":  renderChart(msg.data, msg.symbol); break;
    case "news_data":   renderDrawerNews(msg.data); break;
    case "daily_data":  renderDrawerDaily(msg.data, msg.symbol); break;
    case "matched_orders": renderMatchedOrders(msg.data, msg.total_count); break;
    case "heartbeat": case "pong": break;
  }
}

function ingest(rows, full){
  if(!rows)return;
  if(full){ prevStocks={}; stocks={}; }
  const changed=new Set();
  rows.forEach(r=>{
    const n = norm(r);
    const sym = n.symbol;
    if(!sym) return;
    // Skip rows where price is still null (no data at all)
    if(n.price==null && stocks[sym]) return;
    if(stocks[sym]) prevStocks[sym] = {...stocks[sym]};
    stocks[sym] = n;
    changed.add(sym);
  });
  renderTable(changed);
  updateStats();
  el.sTime.textContent=fmtTime(new Date());

  if(selected && changed.has(selected)){
    updateDrawerPrice();
    ingestSelectedRealtimePoint(selected);
  }
  if(document.querySelector(".tab.active")?.dataset.tab === "watchlist"){
    const touchedWatchlist = [...watchlist].filter(sym => changed.has(sym));
    if(touchedWatchlist.length) scheduleWatchlistCardRefresh(touchedWatchlist);
  }
}

/* ═══════════════════════════════════════════════════════════
   STATS
   ═══════════════════════════════════════════════════════════ */
function updateStats(){
  let arr=Object.values(stocks);
  if(marketFilter) arr=arr.filter(s=>matchesMarket(s.symbol));
  if(sectorFilter) arr=arr.filter(s=>getSector(s.symbol)===sectorFilter);
  let up=0,dn=0,fl=0,vol=0;
  arr.forEach(s=>{
    if(s.pct>0)up++;else if(s.pct<0)dn++;else fl++;
    vol += s.volume||0;
  });
  el.sTotal.textContent=arr.length;
  el.sUp.textContent=up;
  el.sDown.textContent=dn;
  el.sFlat.textContent=fl;
  el.sVol.textContent=fmtV(vol);
}

/* ═══════════════════════════════════════════════════════════
   TABLE
   ═══════════════════════════════════════════════════════════ */
function sorted(){
  let arr=Object.values(stocks);
  const q=el.search.value.trim().toUpperCase();
  if(q) arr=arr.filter(s=>s.symbol.includes(q));
  if(marketFilter) arr=arr.filter(s=>matchesMarket(s.symbol));
  if(sectorFilter) arr=arr.filter(s=>getSector(s.symbol)===sectorFilter);
  switch(sortMode){
    case "symbol":   arr.sort((a,b)=>a.symbol.localeCompare(b.symbol));break;
    case "pct_desc": arr.sort((a,b)=>(b.pct||0)-(a.pct||0));break;
    case "pct_asc":  arr.sort((a,b)=>(a.pct||0)-(b.pct||0));break;
    case "vol_desc": arr.sort((a,b)=>(b.volume||0)-(a.volume||0));break;
  }
  return arr;
}

function renderTable(changed){
  const rows=sorted();
  el.rowCount.textContent=`${rows.length} mã`;

  if(!rows.length){
    el.body.innerHTML=`<tr><td colspan="13" class="empty-row">${Object.keys(stocks).length?"Không tìm thấy":'<div class="spinner"></div>Đang tải …'}</td></tr>`;
    return;
  }

  el.body.innerHTML=rows.map(s=>{
    const c=cls(s.pct);
    const isFav = isInWatchlist(s.symbol);
    const cp = cpSignals[s.symbol] || null;
    const cpClass =
      !cp ? "unknown"
      : cp.regime_label === "whale-watch" ? "up"
      : cp.regime_label === "transition" ? "flat"
      : "down";
    const cpLabel =
      !cp ? "Chua co"
      : cp.regime_label === "whale-watch" ? "Whale"
      : cp.regime_label === "transition" ? "Transition"
      : "Stable";
    const ml = getSymbolMlForecast(s.symbol);
    const mlMeta = ml
      ? ` | ML: ${ml.directionLabel} ${ml.expectedText} | up ${fmtProb(ml.probUp)} down ${fmtProb(ml.probDown)}`
      : "";
    const cpTitle = cp
      ? `CP prob: ${fmtProb(cp.cp_prob)} | E[r_t]: ${fmt(cp.expected_run_length, 2)} | MAP r_t: ${fmt(cp.map_run_length, 0)}${mlMeta}`
      : "Chua co du lieu BOCPD";
    // Cell-level flash: only highlight specific cells that changed
    let priceFlash="",changeFlash="",pctFlash="",openFlash="",highFlash="",lowFlash="",volFlash="",vwapFlash="";
    if(changed&&changed.has(s.symbol)){
      const prev=prevStocks[s.symbol];
      if(prev){
        if(prev.price!=null&&s.price!=null&&s.price!==prev.price)
          priceFlash=s.price>prev.price?"flash-up":"flash-down";
        if(prev.change!=null&&s.change!=null&&s.change!==prev.change)
          changeFlash=s.change>prev.change?"flash-up":"flash-down";
        if(prev.pct!=null&&s.pct!=null&&s.pct!==prev.pct)
          pctFlash=s.pct>prev.pct?"flash-up":"flash-down";
        if(prev.open!=null&&s.open!=null&&s.open!==prev.open)
          openFlash=s.open>prev.open?"flash-up":"flash-down";
        if(prev.high!=null&&s.high!=null&&s.high!==prev.high)
          highFlash=s.high>prev.high?"flash-up":"flash-down";
        if(prev.low!=null&&s.low!=null&&s.low!==prev.low)
          lowFlash=s.low>prev.low?"flash-up":"flash-down";
        if(prev.volume!=null&&s.volume!=null&&s.volume!==prev.volume)
          volFlash=s.volume>prev.volume?"flash-up":"flash-down";
        if(prev.vwap!=null&&s.vwap!=null&&s.vwap!==prev.vwap)
          vwapFlash=s.vwap>prev.vwap?"flash-up":"flash-down";
      }
    }
    return `<tr>
      <td class="sticky-col sym-col" onclick="openDrawer('${s.symbol}')"><span class="sym">${s.symbol}</span></td>
      <td class="company-col" onclick="openDrawer('${s.symbol}')">${getCompanyName(s.symbol)}</td>
      <td class="num ${c} ${priceFlash}" onclick="openDrawer('${s.symbol}')">${fmt(s.price)}</td>
      <td class="num ${c} ${changeFlash}" onclick="openDrawer('${s.symbol}')">${(s.change>=0?"+":"")+fmt(s.change)}</td>
      <td class="num ${c} ${pctFlash}" onclick="openDrawer('${s.symbol}')">${(s.pct>=0?"+":"")+fmt(s.pct)}%</td>
      <td onclick="openDrawer('${s.symbol}')">
        <div class="cp-table" title="${cpTitle}">
          <span class="cp-pill ${cpClass}">${cpLabel}</span>
          <span class="cp-pill-meta">${cp ? fmtProb(cp.cp_prob) : '--'}</span>
          <span class="cp-pill-ml ${ml ? ml.klass : ''}">
            ${ml ? `ML ${ml.directionLabel} ${ml.expectedText}` : 'ML --'}
          </span>
        </div>
      </td>
      <td class="num ${openFlash}" onclick="openDrawer('${s.symbol}')">${fmt(s.open)}</td>
      <td class="num ${highFlash}" onclick="openDrawer('${s.symbol}')">${fmt(s.high)}</td>
      <td class="num ${lowFlash}" onclick="openDrawer('${s.symbol}')">${fmt(s.low)}</td>
      <td class="num ${volFlash}" onclick="openDrawer('${s.symbol}')">${fmtV(s.volume)}</td>
      <td class="num ${vwapFlash}" onclick="openDrawer('${s.symbol}')">${s.vwap!=null?fmt(s.vwap,2):"--"}</td>
      <td class="date-col" onclick="openDrawer('${s.symbol}')">${s.isRealtime ? fmtDT(s.date) : fmtDate(s.date)}</td>
      <td class="fav-col"><button class="fav-btn ${isFav?'active':''}" onclick="event.stopPropagation();toggleWatchlist('${s.symbol}')" title="${isFav?'Bỏ quan tâm':'Thêm quan tâm'}">${isFav?'★':'☆'}</button></td>
    </tr>`;
  }).join("");
}

/* ═══════════════════════════════════════════════════════════
   DRAWER
   ═══════════════════════════════════════════════════════════ */
function openDrawer(sym){
  selected=sym;
  el.drawer.classList.add("open");
  el.overlay.classList.add("open");
  resetSelectedCandles(sym);
  clearCandleHitState();
  candleMinuteDetailLoading = false;
  abortLinePanDrag();
  resetCandleViewport();
  resetLineViewport();
  drawerCandles = [];
  drawerCandleSymbol = sym;
  drawerLineSeries = [];
  drawerLineSymbol = sym;
  drawerOhlcvMeta = null;
  drawerCandleCycleMeta = null;
  drawerCpHistory = [];
  drawerCpHistorySymbol = null;
  drawerRunlengthSegments = [];
  selectedRunlengthSegmentId = "__all";
  setPriceChartMode("line", { rerender: false, force: true });
  candleCycleAuto = true;
  syncRecommendedCandleCycle(getActiveChartInterval(), { force: true });
  renderRunlengthStrip([], sym);
  updateDrawerPrice();
  updateChartDockMode();
  loadOHLCV(sym);
  loadChangepoint(sym);
  loadMatchedOrders(sym);
  loadDrawerNews(sym);
  if(cpTimer){clearInterval(cpTimer);cpTimer=null;}
  cpTimer=setInterval(()=>{
    if(selected===sym) loadChangepoint(sym);
    else { clearInterval(cpTimer); cpTimer=null; }
  }, 4000);
}

function closeDrawer(){
  el.drawer.classList.remove("open");
  el.drawer.classList.remove("use-external-chart");
  el.overlay.classList.remove("open");
  if(el.chartDock) el.chartDock.classList.remove("open");
  selected=null;
  resetSelectedCandles(null);
  clearCandleHitState();
  candleMinuteDetailLoading = false;
  abortLinePanDrag();
  resetCandleViewport();
  resetLineViewport();
  drawerCandles = [];
  drawerCandleSymbol = null;
  drawerLineSeries = [];
  drawerLineSymbol = null;
  drawerOhlcvMeta = null;
  drawerCandleCycleMeta = null;
  drawerCpHistory = [];
  drawerCpHistorySymbol = null;
  drawerRunlengthSegments = [];
  selectedRunlengthSegmentId = "__all";
  renderRunlengthStrip([], null);
  if(moTimer){clearInterval(moTimer);moTimer=null;}
  if(cpTimer){clearInterval(cpTimer);cpTimer=null;}
  if(cpChart){cpChart.destroy();cpChart=null;}
  if(chart){chart.destroy();chart=null;}
  renderCandleSelectionInfo();
}

function updateDrawerPrice(){
  const s=stocks[selected];
  if(!s)return;
  const c=cls(s.pct);
  el.drSymbol.textContent=s.symbol;
  
  // Company name & description
  const drCompany = document.getElementById('drCompany');
  const drDescription = document.getElementById('drDescription');
  if(drCompany) drCompany.textContent = getCompanyName(s.symbol);
  if(drDescription){
    const desc = getCompanyDesc(s.symbol);
    if(desc){
      drDescription.textContent = desc;
      drDescription.classList.add('show');
    } else {
      drDescription.classList.remove('show');
    }
  }
  
  // Update favorite button
  updateDrawerFavBtn();
  
  el.drPrice.textContent=fmt(s.price);
  el.drPrice.className=`hero-price ${c}`;
  el.drChange.textContent=`${s.change>=0?"+":""}${fmt(s.change)}  (${s.pct>=0?"+":""}${fmt(s.pct)}%)`;
  el.drChange.className=`hero-change ${c}`;

  el.drInfo.innerHTML=[
    {l:"Mở cửa",v:fmt(s.open)},{l:"Cao nhất",v:fmt(s.high)},
    {l:"Thấp nhất",v:fmt(s.low)},{l:"Khối lượng",v:fmtV(s.volume)},
    {l:"VWAP",v:s.vwap!=null?fmt(s.vwap):"--"},{l:"Sàn",v:s.exchange||"--"},
  ].map(i=>`<div class="info-cell"><div class="lbl">${i.l}</div><div class="val">${i.v}</div></div>`).join("");
}

function updateDrawerFavBtn(){
  const btn = document.getElementById('drFavBtn');
  if(!btn||!selected) return;
  const isFav = isInWatchlist(selected);
  btn.textContent = isFav ? '★' : '☆';
  btn.className = `dr-fav-btn ${isFav?'active':''}`;
  btn.title = isFav ? 'Bỏ quan tâm' : 'Thêm vào quan tâm';
}

function ingestSelectedRealtimePoint(sym){
  const row = stocks[sym];
  if(!row || !Number.isFinite(Number(row.price))) return;
  upsertRealtimeLinePoint(sym, Number(row.price), row.date || new Date());

  if(!selected || sym !== selected) return;
  if(!el.drawer?.classList.contains("open")) return;
  if(isCandlestickMode()) return;

  const activeCanvas = getActiveChartCanvas();
  if(!activeCanvas) return;

  updatePriceChartTitle();
  if(chart && chart.config?.type === "line"){
    updateRealtimeLineChart(sym);
  }else{
    redrawActivePriceChart();
  }
}

function useExternalChartDock(){
  return window.innerWidth >= 1100;
}

function isDockChartActive(){
  return !!(el.chartDock && el.chartDock.classList.contains("open"));
}

function setPriceChartTitle(text){
  if(el.drChartTitle) el.drChartTitle.textContent = text;
  if(el.dockChartTitle) el.dockChartTitle.textContent = text;
}

function isCandlestickMode(){
  return activePriceChartMode === "candlestick";
}

function syncChartModeButtons(){
  [el.drChartMode, el.dockChartMode].forEach(group => {
    if(!group) return;
    group.querySelectorAll(".chart-mode-btn").forEach(btn => {
      const mode = btn.dataset.mode === "candlestick" ? "candlestick" : "line";
      btn.classList.toggle("active", mode === activePriceChartMode);
    });
  });
  syncCandleCycleControls();
}

function syncCandleCycleControls(){
  const visible = isCandlestickMode();
  [el.drCandleInterval, el.dockCandleInterval].forEach(control => {
    if(!control) return;
    control.classList.toggle("hidden", !visible);
    control.disabled = !visible;
  });
}

function updatePriceChartTitle(){
  if(!isCandlestickMode()){
    setPriceChartTitle("Biểu đồ realtime | Keo chuot trai de luot du lieu cu/moi, Ctrl + lan de thu/phong, Shift + lan de qua trai/phai");
    return;
  }
  const rangeLabel = intervalLabel(getActiveChartInterval());
  const cycleLabel = intervalLabel(getActiveCandleInterval());
  const zoomHint = " | Bam de chon tung nen, keo chuot trai de chon vung nhieu nen, giu Alt + keo de luot, Ctrl + lan de thu/phong";
  const meta = drawerOhlcvMeta || {};
  const cycleMeta = drawerCandleCycleMeta || {};
  const limitedNote = cycleMeta.limitedBySource
    ? ` • du lieu nguon ~${Math.round(cycleMeta.sourceMinutes || 0)}p`
    : "";

  if(meta.fallback_used && meta.resolved_interval){
    setPriceChartTitle(`Biểu đồ nến (Range ${rangeLabel} • Chu kỳ ${cycleLabel}${limitedNote} | fallback ${meta.resolved_interval} từ ${meta.requested_interval})${zoomHint}`);
    return;
  }
  if(meta.resolved_interval){
    setPriceChartTitle(`Biểu đồ nến (Range ${rangeLabel} • Chu kỳ ${cycleLabel}${limitedNote})${zoomHint}`);
    return;
  }
  setPriceChartTitle(`Biểu đồ nến (Range ${rangeLabel} • Chu kỳ ${cycleLabel}${limitedNote})${zoomHint}`);
}

function setPriceChartMode(mode, options = {}){
  const nextMode = mode === "candlestick" ? "candlestick" : "line";
  const force = !!options.force;
  const rerender = options.rerender !== false;

  if(!force && activePriceChartMode === nextMode){
    syncChartModeButtons();
    return;
  }

  abortLinePanDrag();
  candleMinuteDetailLoading = false;
  activePriceChartMode = nextMode;
  selectedRunlengthSegmentId = "__all";
  if(nextMode === "candlestick"){
    syncRecommendedCandleCycle(getActiveChartInterval());
  }
  syncChartModeButtons();
  updatePriceChartTitle();

  if(!isCandlestickMode()){
    clearCandleHitState();
    renderRunlengthStrip([], selected);
  }
  renderCandleSelectionInfo();

  if(!rerender || !selected || !el.drawer?.classList.contains("open")) return;

  if(isCandlestickMode() && (!drawerCandles?.length || drawerCandleSymbol !== selected)){
    loadOHLCV(selected);
    return;
  }

  redrawActivePriceChart();
}

function syncChartIntervals(sourceEl){
  const value = sourceEl?.value;
  if(!value) return;
  if(el.drInterval) el.drInterval.value = value;
  if(el.dockInterval) el.dockInterval.value = value;
}

function syncCandleIntervals(sourceEl){
  const value = sourceEl?.value;
  if(!value) return;
  if(el.drCandleInterval) el.drCandleInterval.value = value;
  if(el.dockCandleInterval) el.dockCandleInterval.value = value;
}

function updateChartDockMode(){
  const shouldDock = !!selected && !!el.drawer?.classList.contains("open") && useExternalChartDock();
  if(el.chartDock) el.chartDock.classList.toggle("open", shouldDock);
  if(el.drawer) el.drawer.classList.toggle("use-external-chart", shouldDock);
  if(shouldDock && el.dockInterval && el.drInterval){
    el.dockInterval.value = el.drInterval.value;
    if(el.dockCandleInterval && el.drCandleInterval){
      el.dockCandleInterval.value = el.drCandleInterval.value;
    }
  }else if(!shouldDock){
    renderRunlengthStrip([], selected);
  }
  return shouldDock;
}

function getActiveChartCanvas(){
  if(isDockChartActive() && el.dockChart) return el.dockChart;
  return el.drChart;
}

function getActiveChartInterval(){
  if(isDockChartActive() && el.dockInterval) return el.dockInterval.value;
  return el.drInterval?.value || "1d";
}

function getActiveCandleInterval(){
  if(isDockChartActive() && el.dockCandleInterval) return el.dockCandleInterval.value;
  return el.drCandleInterval?.value || "15m";
}

function isAutoMinuteDetailActive(){
  return false;
}

function maybeLoadMinuteDetailForCandlestick(){
  return false;
}

/* ── Chart ───────────────────────────────────────────────── */
function loadOHLCV(sym, intervalOverride = null, options = {}){
  const onComplete = typeof options?.onComplete === "function" ? options.onComplete : null;
  const iv = String(intervalOverride || getActiveChartInterval() || "1d").toLowerCase();
  const isMinuteRequest = iv === "1m";
  if(isMinuteRequest) candleMinuteDetailLoading = true;
  drawerOhlcvMeta = null;
  drawerCandleCycleMeta = null;
  updatePriceChartTitle();
  fetch(`${API}/api/stocks/ohlcv/${encodeURIComponent(sym)}?interval=${encodeURIComponent(iv)}`)
    .then(r=>r.json())
    .then(j=>{
      if(isMinuteRequest) candleMinuteDetailLoading = false;
      if(j.status!=="ok"){
        const selectedIv = String(getActiveChartInterval() || "").toLowerCase();
        if(isMinuteRequest && selectedIv !== "1m" && isCandlestickMode() && selected === sym){
          loadOHLCV(sym);
          return;
        }
        renderChart([], sym);
        return;
      }
      drawerOhlcvMeta = j.meta || null;
      renderChart(j.data,sym);
    })
    .catch(err=>{
      if(isMinuteRequest) candleMinuteDetailLoading = false;
      console.error("loadOHLCV error:", err);
      const selectedIv = String(getActiveChartInterval() || "").toLowerCase();
      if(isMinuteRequest && selectedIv !== "1m" && isCandlestickMode() && selected === sym){
        loadOHLCV(sym);
        return;
      }
      drawerOhlcvMeta = null;
      drawerCandleCycleMeta = null;
      renderChart([], sym);
    })
    .finally(() => {
      if(!onComplete) return;
      try{
        onComplete();
      }catch(callbackErr){
        console.error("loadOHLCV onComplete error:", callbackErr);
      }
    });
}

function renderChart(data,sym){
  if(!sym || !selected || _normSym(sym) !== _normSym(selected)) return;
  const rawCandles = normalizeCandlestickSeries(data);
  const cycleResult = applyCandlestickCycle(
    rawCandles,
    getActiveCandleInterval(),
    { keepDetailWhenCollapsed: isVnSymbolForCandles(sym) }
  );
  const candles = cycleResult.candles;
  const priceSeries = normalizePriceSeries(data);

  if(selectedCandlesSymbol !== sym){
    resetSelectedCandles(sym);
  }

  drawerCandleSymbol = sym;
  drawerCandles = candles;
  drawerCandleCycleMeta = cycleResult;

  resetLineViewport();
  drawerLineSymbol = sym;
  if(priceSeries.length){
    drawerLineSeries = priceSeries.slice(-MAX_LINE_POINTS);
  }else if(candles.length){
    drawerLineSeries = buildRealtimeSeriesFromCandles(candles).slice(-MAX_LINE_POINTS);
  }else{
    drawerLineSeries = [];
  }

  const rt = stocks[sym];
  if(rt && Number.isFinite(Number(rt.price))){
    upsertRealtimeLinePoint(sym, Number(rt.price), rt.date || new Date());
  }

  drawerLineSeries = sanitizeRealtimeSeries(sym, drawerLineSeries);

  redrawActivePriceChart();
}

/* ── Changepoint / BOCPD ───────────────────────────────── */
async function loadChangepoint(sym){
  if(!el.drCpInfo || !el.drCpChart) return;
  try{
    const [latestResp, historyResp] = await Promise.all([
      fetch(`${API}/api/changepoint/${sym}`),
      fetch(`${API}/api/changepoint/${sym}/history?limit=120&days=5`),
    ]);

    const latestJson = await latestResp.json().catch(()=>({}));
    const historyJson = await historyResp.json().catch(()=>({}));

    if(latestResp.ok && latestJson.status === "ok"){
      cpSignals[sym] = latestJson.data;
      renderChangepointInfo(latestJson.data, sym);
    } else {
      renderChangepointInfo(null, sym);
    }

    if(historyResp.ok && historyJson.status === "ok"){
      const cpHistoryRows = historyJson.data || [];
      drawerCpHistory = normalizeChangepointSeries(cpHistoryRows);
      drawerCpHistorySymbol = sym;
      renderChangepointChart(cpHistoryRows, sym);
      if(selected === sym && drawerCandleSymbol === sym && drawerCandles?.length){
        redrawActiveCandlestickChart();
      }
    } else {
      drawerCpHistory = [];
      drawerCpHistorySymbol = sym;
      drawerRunlengthSegments = [];
      if(selected === sym) renderRunlengthStrip([], sym);
      renderChangepointChart([], sym);
    }
  }catch(err){
    console.error("loadChangepoint error:", err);
    drawerCpHistory = [];
    drawerCpHistorySymbol = sym;
    drawerRunlengthSegments = [];
    if(selected === sym) renderRunlengthStrip([], sym);
    renderChangepointInfo(null, sym);
    renderChangepointChart([], sym);
  }
}

function renderChangepointInfo(data, symbol=selected){
  if(!el.drCpInfo) return;
  if(!data){
    el.drCpInfo.innerHTML = '<div class="cp-empty">Chua co du lieu BOCPD cho ma nay.</div>';
    return;
  }

  const cpClass = (data.cp_prob || 0) >= 0.25 ? 'up' : (data.cp_prob || 0) >= 0.1 ? 'flat' : 'down';
  const whaleClass = (data.whale_score || 0) >= 0.35 ? 'up' : (data.whale_score || 0) >= 0.15 ? 'flat' : 'down';
  const zClass = (data.innovation_zscore || 0) >= 2 ? 'up' : (data.innovation_zscore || 0) >= 1 ? 'flat' : 'down';

  const cards = [
    {label:'CP Prob', value: fmtProb(data.cp_prob), klass: cpClass},
    {label:'E[r_t]', value: fmt(data.expected_run_length, 2), klass: 'flat'},
    {label:'MAP r_t', value: fmt(data.map_run_length, 0), klass: 'flat'},
    {label:'Return', value: `${(data.return_value||0) >= 0 ? '+' : ''}${fmt((data.return_value||0)*100, 3)}%`, klass: cls(data.return_value||0)},
    {label:'Pred Vol', value: fmt((data.predictive_volatility||0)*100, 3) + '%', klass: 'flat'},
    {label:'Z Score', value: fmt(data.innovation_zscore, 2), klass: zClass},
    {label:'Whale Score', value: fmtProb(data.whale_score), klass: whaleClass},
    {label:'Regime', value: data.regime_label || '--', klass: data.regime_label === 'whale-watch' ? 'up' : data.regime_label === 'transition' ? 'flat' : 'down'},
  ];

  const ml = getSymbolMlForecast(symbol);
  if(ml){
    cards.push({label:'ML Dir', value: ml.directionLabel, klass: ml.klass});
    cards.push({label:'ML Session', value: ml.expectedText, klass: 'flat'});
    cards.push({label:'ML P(up)', value: fmtProb(ml.probUp), klass: 'up'});
    cards.push({label:'ML P(down)', value: fmtProb(ml.probDown), klass: 'down'});
  }

  el.drCpInfo.innerHTML = cards.map(card => `
    <div class="cp-card">
      <div class="cp-label">${card.label}</div>
      <div class="cp-value ${card.klass}">${card.value}</div>
    </div>
  `).join('');
}

function renderChangepointChart(data, sym){
  const canvas = el.drCpChart;
  if(!canvas) return;
  if(cpChart){ cpChart.destroy(); cpChart = null; }

  if(!data || !data.length){
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#4e556b';
    ctx.font = '13px Inter';
    ctx.textAlign = 'center';
    ctx.fillText('Chua co lich su r_t / changepoint', canvas.width / 2, canvas.height / 2);
    return;
  }

  const labels = data.map(item => new Date(item.event_time));
  const expectedRunLength = data.map(item => item.expected_run_length || 0);
  const mapRunLength = data.map(item => item.map_run_length || 0);
  const cpProb = data.map(item => item.cp_prob || 0);

  cpChart = new Chart(canvas, {
    data: {
      labels,
      datasets: [
        {
          type: 'line',
          label: 'E[r_t]',
          data: expectedRunLength,
          borderColor: '#f59e0b',
          backgroundColor: 'rgba(245,158,11,.12)',
          tension: .28,
          pointRadius: 0,
          borderWidth: 2,
          yAxisID: 'y',
        },
        {
          type: 'line',
          label: 'MAP r_t',
          data: mapRunLength,
          borderColor: '#60a5fa',
          backgroundColor: 'transparent',
          tension: .18,
          pointRadius: 0,
          borderDash: [6, 4],
          borderWidth: 1.5,
          yAxisID: 'y',
        },
        {
          type: 'bar',
          label: 'CP prob',
          data: cpProb,
          backgroundColor: 'rgba(239,68,68,.22)',
          borderColor: '#ef4444',
          borderWidth: 1,
          yAxisID: 'y1',
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, labels: { color: '#7d849b', boxWidth: 10 } },
        title: {
          display: true,
          text: `${sym} - Run length r_t theo thoi gian`,
          color: '#c3c8d8',
          font: { size: 12, weight: '600' },
        },
        tooltip: {
          callbacks: {
            label: ctx => {
              const v = ctx.parsed.y;
              if(ctx.dataset.label === 'CP prob') return `CP prob: ${(v*100).toFixed(2)}%`;
              return `${ctx.dataset.label}: ${Number(v).toFixed(2)}`;
            }
          }
        }
      },
      scales: {
        x: {
          type: 'time',
          time: { tooltipFormat: 'dd/MM/yyyy HH:mm:ss' },
          ticks: { color: '#4e556b', maxTicksLimit: 6 },
          grid: { color: 'rgba(31,39,57,.35)' },
        },
        y: {
          position: 'left',
          title: { display: true, text: 'Run length r_t', color: '#f59e0b' },
          ticks: { color: '#f59e0b' },
          grid: { color: 'rgba(31,39,57,.35)' },
          beginAtZero: true,
        },
        y1: {
          position: 'right',
          title: { display: true, text: 'CP prob', color: '#ef4444' },
          ticks: {
            color: '#ef4444',
            callback: value => `${(Number(value) * 100).toFixed(0)}%`,
          },
          min: 0,
          max: 1,
          grid: { drawOnChartArea: false },
        },
      },
    },
  });
}

/* ── Matched Orders ──────────────────────────────────────── */
function setMarketOrderbookLoading(){
  if(el.mobDepthBody){
    el.mobDepthBody.innerHTML = [1, 2, 3].map(() => (
      '<div class="mob-depth-item"><span class="mob-col-empty">--</span><span class="mob-col-empty">--</span><span class="mob-col-empty">--</span><span class="mob-col-empty">--</span></div>'
    )).join("");
  }
  if(el.mobBuyForceLbl) el.mobBuyForceLbl.textContent = "Lực mua --%";
  if(el.mobSellForceLbl) el.mobSellForceLbl.textContent = "Lực bán --%";
  if(el.mobBuyForceBar) el.mobBuyForceBar.style.width = "50%";
  if(el.mobSellForceBar) el.mobSellForceBar.style.width = "50%";
}

function setMoLotMode(mode, rerender = true){
  moLotMode = mode === "odd" ? "odd" : "round";
  if(el.mobRoundBtn) el.mobRoundBtn.classList.toggle("active", moLotMode === "round");
  if(el.mobOddBtn) el.mobOddBtn.classList.toggle("active", moLotMode === "odd");
  if(rerender && moLastRows.length){
    renderMatchedOrders(moLastRows, moLastTotalCount, false);
  }
}

function loadMatchedOrders(sym){
  el.moBody.innerHTML='<tr><td colspan="4" class="muted" style="text-align:center;padding:20px">Đang tải …</td></tr>';
  if(el.moMeta) el.moMeta.textContent = 'Đang cập nhật...';
  if(el.moBuyVal) el.moBuyVal.textContent = '--';
  if(el.moTotalVal) el.moTotalVal.textContent = '--';
  if(el.moSellVal) el.moSellVal.textContent = '--';
  if(el.moBuyPct) el.moBuyPct.textContent = '--%';
  if(el.moSellPct) el.moSellPct.textContent = '--%';
  if(el.moBuyBar) el.moBuyBar.style.width = '50%';
  if(el.moSellBar) el.moSellBar.style.width = '50%';
  setMarketOrderbookLoading();
  moLastRows = [];
  moLastTotalCount = 0;
  // Clear previous timer
  if(moTimer){clearInterval(moTimer);moTimer=null;}

  const doLoad = () => {
    // Always use REST API for reliability (avoids WS race with price_update broadcasts)
    fetch(`${API}/api/stocks/matched-orders/${sym}?limit=50`)
      .then(r=>r.json())
      .then(j=>{
        if(j.status==="ok") renderMatchedOrders(j.data, j.total_count, true);
        else console.warn("matched-orders bad response:", j);
      })
      .catch(err=>console.error("matched-orders fetch error:", err));
  };
  doLoad();
  // Auto-refresh every 3s while drawer is open
  moTimer=setInterval(()=>{if(selected===sym)doLoad();else{clearInterval(moTimer);moTimer=null;}},3000);
}

function inferMatchedOrderSide(rows, idx, fallbackSide = "buy"){
  const row = rows[idx] || {};
  const prev = idx > 0 ? rows[idx - 1] : null;
  const next = idx + 1 < rows.length ? rows[idx + 1] : null;

  const currPrice = Number(row.price);
  const prevPrice = Number(prev?.price);
  const nextPrice = Number(next?.price);

  if(Number.isFinite(currPrice) && Number.isFinite(prevPrice) && currPrice !== prevPrice){
    return currPrice > prevPrice ? "buy" : "sell";
  }
  if(Number.isFinite(currPrice) && Number.isFinite(nextPrice) && currPrice !== nextPrice){
    return currPrice > nextPrice ? "buy" : "sell";
  }

  const change = Number(row.change);
  if(Number.isFinite(change) && change !== 0){
    return change > 0 ? "buy" : "sell";
  }

  const changePct = Number(row.change_percent);
  if(Number.isFinite(changePct) && changePct !== 0){
    return changePct > 0 ? "buy" : "sell";
  }

  return fallbackSide;
}

function fmtVolVn(v){
  const n = Number(v);
  if(!Number.isFinite(n)) return "0";
  return Math.max(0, Math.round(n)).toLocaleString("vi-VN");
}

function resolveMatchedOrderSize(rows, idx){
  const row = rows[idx] || {};

  const matchedSize = Number(row.matched_size);
  if(Number.isFinite(matchedSize) && matchedSize > 0){
    return matchedSize;
  }

  const lastSize = Number(row.last_size);
  if(Number.isFinite(lastSize) && lastSize > 0){
    return lastSize;
  }

  const curDayVolume = Number(row.day_volume);
  const nextDayVolume = Number(rows[idx + 1]?.day_volume);
  if(Number.isFinite(curDayVolume) && Number.isFinite(nextDayVolume)){
    const delta = curDayVolume - nextDayVolume;
    if(delta > 0) return delta;
  }

  return 0;
}

function getMatchedEpochMs(row){
  const producer = Number(row?.producer_timestamp);
  if(Number.isFinite(producer) && producer > 0){
    return producer < 10_000_000_000 ? producer * 1000 : producer;
  }

  const ts = row?.timestamp;
  if(typeof ts === "number" && Number.isFinite(ts)){
    return ts < 10_000_000_000 ? ts * 1000 : ts;
  }
  if(typeof ts === "string"){
    const raw = ts.trim();
    if(/^\d+$/.test(raw)){
      let iv = Number(raw);
      if(Number.isFinite(iv)){
        if(iv < 10_000_000_000) iv *= 1000;
        return iv;
      }
    }
  }

  const d = parseChartTime(ts);
  return d ? d.getTime() : -1;
}

function isMatchedRowInLotMode(size){
  if(!Number.isFinite(size) || size <= 0) return false;
  if(moLotMode === "odd"){
    return size < 100 || (size % 100 !== 0);
  }
  return size >= 100 && (size % 100 === 0);
}

function fmtOrderbookPrice(price){
  const p = Number(price);
  if(!Number.isFinite(p)) return "--";
  const decimals = Math.abs(p - Math.round(p)) < 1e-8 ? 0 : 2;
  return fmt(p, decimals);
}

function renderMarketOrderbook(rows, buyVolume, sellVolume){
  if(!el.mobDepthBody) return;

  const hasPositiveVolume = (rows || []).some(r => Number.isFinite(Number(r?.size)) && Number(r.size) > 0);

  const buyMap = new Map();
  const sellMap = new Map();
  const buyPriceOnly = new Map();
  const sellPriceOnly = new Map();
  rows.forEach(row => {
    const price = Number(row.price);
    if(!Number.isFinite(price)) return;

    if(row.side === "buy"){
      buyPriceOnly.set(price, (buyPriceOnly.get(price) || 0) + 1);
    }else if(row.side === "sell"){
      sellPriceOnly.set(price, (sellPriceOnly.get(price) || 0) + 1);
    }

    if(!hasPositiveVolume) return;

    const rowSize = Number(row.size);
    if(!Number.isFinite(rowSize) || rowSize <= 0) return;

    if(row.side === "buy"){
      buyMap.set(price, (buyMap.get(price) || 0) + rowSize);
    }else if(row.side === "sell"){
      sellMap.set(price, (sellMap.get(price) || 0) + rowSize);
    }
  });

  const buyLevels = hasPositiveVolume
    ? [...buyMap.entries()]
      .sort((a, b) => b[0] - a[0])
      .slice(0, 3)
      .map(([price, size]) => ({ price, size }))
    : [...buyPriceOnly.entries()]
      .sort((a, b) => b[0] - a[0])
      .slice(0, 3)
      .map(([price]) => ({ price, size: null }));
  const sellLevels = hasPositiveVolume
    ? [...sellMap.entries()]
      .sort((a, b) => a[0] - b[0])
      .slice(0, 3)
      .map(([price, size]) => ({ price, size }))
    : [...sellPriceOnly.entries()]
      .sort((a, b) => a[0] - b[0])
      .slice(0, 3)
      .map(([price]) => ({ price, size: null }));

  const rowCount = Math.max(3, buyLevels.length, sellLevels.length);
  const hasDepth = buyLevels.length || sellLevels.length;

  el.mobDepthBody.innerHTML = Array.from({ length: rowCount }, (_, idx) => {
    const b = buyLevels[idx];
    const s = sellLevels[idx];
    const buyVol = b && hasPositiveVolume ? fmtVolVn(b.size) : "--";
    const buyPrice = b ? fmtOrderbookPrice(b.price) : "--";
    const sellPrice = s ? fmtOrderbookPrice(s.price) : "--";
    const sellVol = s && hasPositiveVolume ? fmtVolVn(s.size) : "--";
    const buyCls = b ? "mob-col-buy-vol" : "mob-col-empty";
    const buyPriceCls = b ? "mob-col-buy-price" : "mob-col-empty";
    const sellPriceCls = s ? "mob-col-sell-price" : "mob-col-empty";
    const sellCls = s ? "mob-col-sell-vol" : "mob-col-empty";
    return `<div class="mob-depth-item">
      <span class="${buyCls}">${buyVol}</span>
      <span class="${buyPriceCls}">${buyPrice}</span>
      <span class="${sellPriceCls}">${sellPrice}</span>
      <span class="${sellCls}">${sellVol}</span>
    </div>`;
  }).join("");

  const total = buyVolume + sellVolume;
  const hasVolumeRatio = total > 0;
  const buyPct = hasVolumeRatio ? (buyVolume / total) * 100 : 0;
  const sellPct = hasVolumeRatio ? (sellVolume / total) * 100 : 0;
  const buyText = hasVolumeRatio ? buyPct.toFixed(0) : "--";
  const sellText = hasVolumeRatio ? sellPct.toFixed(0) : "--";

  if(el.mobBuyForceLbl) el.mobBuyForceLbl.textContent = `Lực mua ${buyText}%`;
  if(el.mobSellForceLbl) el.mobSellForceLbl.textContent = `Lực bán ${sellText}%`;
  if(el.mobBuyForceBar) el.mobBuyForceBar.style.width = hasVolumeRatio ? `${buyPct}%` : "50%";
  if(el.mobSellForceBar) el.mobSellForceBar.style.width = hasVolumeRatio ? `${sellPct}%` : "50%";
}

function renderMatchedOrders(data, totalCount, remember = true){
  if(remember){
    moLastRows = Array.isArray(data) ? data.slice() : [];
    moLastTotalCount = totalCount != null ? Number(totalCount) : moLastRows.length;
  }

  if(!data||!data.length){
    el.moBody.innerHTML='<tr><td colspan="4" class="muted" style="text-align:center;padding:20px">Chưa có dữ liệu</td></tr>';
    if(el.moMeta) el.moMeta.textContent = '0 lệnh';
    if(el.moBuyVal) el.moBuyVal.textContent = '0';
    if(el.moTotalVal) el.moTotalVal.textContent = '0';
    if(el.moSellVal) el.moSellVal.textContent = '0';
    if(el.moBuyPct) el.moBuyPct.textContent = '0%';
    if(el.moSellPct) el.moSellPct.textContent = '0%';
    if(el.moBuyBar) el.moBuyBar.style.width = '0%';
    if(el.moSellBar) el.moSellBar.style.width = '0%';
    renderMarketOrderbook([], 0, 0);
    return;
  }

  const sortedData = data
    .slice()
    .sort((a, b) => getMatchedEpochMs(b) - getMatchedEpochMs(a));

  let carrySide = "buy";

  const rows = sortedData.map((d, idx) => {
    const ts = parseChartTime(d.timestamp) || parseChartTime(d.producer_timestamp);
    const timeStr = ts
      ? ts.toLocaleTimeString("vi-VN",{hour:"2-digit",minute:"2-digit",second:"2-digit"})
      : "--";
    const side = inferMatchedOrderSide(sortedData, idx, carrySide);
    carrySide = side;

    const size = Math.max(0, Math.round(resolveMatchedOrderSize(sortedData, idx)));

    return {
      ...d,
      timeStr,
      side,
      size,
    };
  });

  const roundRows = rows.filter(r => Number.isFinite(r.size) && r.size > 0 && r.size >= 100 && (r.size % 100 === 0));
  const oddRows = rows.filter(r => Number.isFinite(r.size) && r.size > 0 && (r.size < 100 || (r.size % 100 !== 0)));
  let effectiveLotMode = moLotMode;
  let lotLabel = effectiveLotMode === "odd" ? "lô lẻ" : "lô chẵn";
  let fallbackNote = "";
  let lotClassified = true;
  let filteredRows = (effectiveLotMode === "odd" ? oddRows : roundRows).slice(0, 50);

  if(!filteredRows.length){
    const altMode = effectiveLotMode === "odd" ? "round" : "odd";
    const altRows = altMode === "odd" ? oddRows : roundRows;
    if(altRows.length){
      effectiveLotMode = altMode;
      filteredRows = altRows.slice(0, 50);
      setMoLotMode(effectiveLotMode, false);
      lotLabel = effectiveLotMode === "odd" ? "lô lẻ" : "lô chẵn";
      fallbackNote = " • tự chuyển sang lô có dữ liệu";
    }else{
      filteredRows = rows.slice(0, 50);
      lotClassified = false;
      lotLabel = "không xác định lô";
      fallbackNote = " • dữ liệu khối lượng chưa đủ";
    }
  }

  if(!filteredRows.length){
    el.moBody.innerHTML='<tr><td colspan="4" class="muted" style="text-align:center;padding:20px">Chưa có dữ liệu khớp lệnh</td></tr>';
    if(el.moMeta) el.moMeta.textContent = '0 lệnh';
    if(el.moBuyVal) el.moBuyVal.textContent = '0';
    if(el.moTotalVal) el.moTotalVal.textContent = '0';
    if(el.moSellVal) el.moSellVal.textContent = '0';
    if(el.moBuyPct) el.moBuyPct.textContent = '0%';
    if(el.moSellPct) el.moSellPct.textContent = '0%';
    if(el.moBuyBar) el.moBuyBar.style.width = '0%';
    if(el.moSellBar) el.moSellBar.style.width = '0%';
    renderMarketOrderbook([], 0, 0);
    return;
  }

  let buyVolume = 0;
  let sellVolume = 0;
  filteredRows.forEach(d => {
    if(d.side === "buy") buyVolume += d.size;
    else sellVolume += d.size;
  });
  const hasEstimatedSize = filteredRows.some(d => (
    d.matched_size_source === "minute_volume_est" || d.matched_size_source === "daily_volume_est"
  ));

  el.moBody.innerHTML=filteredRows.map(d=>{
    const sideLabel = d.side === "buy" ? "Mua" : "Bán";
    const sideCls = d.side === "buy" ? "buy" : "sell";
    const priceCls = d.side === "buy" ? "mo-price-buy" : "mo-price-sell";
    return `<tr>
      <td class="mo-time">${d.timeStr}</td>
      <td><span class="mo-side-badge ${sideCls}">${sideLabel}</span></td>
      <td class="mo-size">${d.size > 0 ? fmtVolVn(d.size) : "--"}</td>
      <td class="${priceCls}">${d.price!=null?fmt(d.price,2):"--"}</td>
    </tr>`;
  }).join("");

  const totalVolume = buyVolume + sellVolume;
  const buyPct = totalVolume > 0 ? (buyVolume / totalVolume) * 100 : 0;
  const sellPct = totalVolume > 0 ? (sellVolume / totalVolume) * 100 : 0;
  const hasVolume = totalVolume > 0;

  if(el.moMeta){
    const totalRows = totalCount!=null ? Number(totalCount) : filteredRows.length;
    const lotText = lotClassified ? lotLabel : "gần nhất";
    const estimateNote = hasEstimatedSize ? " • khối lượng ước tính" : "";
    el.moMeta.textContent = `${filteredRows.length} lệnh ${lotText} • Tổng bản ghi: ${totalRows.toLocaleString("vi-VN")}${fallbackNote}${estimateNote}`;
  }
  if(el.moBuyVal) el.moBuyVal.textContent = hasVolume ? fmtVolVn(buyVolume) : '--';
  if(el.moTotalVal) el.moTotalVal.textContent = hasVolume ? fmtVolVn(totalVolume) : '--';
  if(el.moSellVal) el.moSellVal.textContent = hasVolume ? fmtVolVn(sellVolume) : '--';
  if(el.moBuyPct) el.moBuyPct.textContent = hasVolume ? `${buyPct.toFixed(0)}%` : '--%';
  if(el.moSellPct) el.moSellPct.textContent = hasVolume ? `${sellPct.toFixed(0)}%` : '--%';
  if(el.moBuyBar) el.moBuyBar.style.width = hasVolume ? `${buyPct}%` : '50%';
  if(el.moSellBar) el.moSellBar.style.width = hasVolume ? `${sellPct}%` : '50%';

  renderMarketOrderbook(filteredRows, buyVolume, sellVolume);
}

/* ── Drawer News ─────────────────────────────────────────── */
function loadDrawerNews(sym){
  el.drNews.innerHTML='<p class="muted">Đang tải …</p>';
  if(ws&&ws.readyState===1){
    ws.send(JSON.stringify({type:"get_news",stock_code:sym}));
  } else {
    fetch(`${API}/api/news/${sym}`).then(r=>r.json()).then(j=>{if(j.status==="ok")renderDrawerNews(j.data)}).catch(()=>{el.drNews.innerHTML='<p class="muted">Lỗi tải tin</p>'});
  }
}

function renderDrawerNews(data){
  if(!data||!data.length){el.drNews.innerHTML='<p class="muted">Không có tin tức</p>';return}
  el.drNews.innerHTML=data.slice(0,10).map(n=>{
    const sc=n.sentiment_score||0;
    const sCls=sc>0?"sent-pos":sc<0?"sent-neg":"sent-neu";
    const sLabel=sc>0?"Tích cực":sc<0?"Tiêu cực":"Trung lập";
    const newsJson = JSON.stringify(n).replace(/"/g,'&quot;');
    return `<div class="dn-item" onclick="openNewsModal(${newsJson})" style="cursor:pointer">
      <div class="dn-title">${n.title||"Untitled"}</div>
      <div class="dn-meta"><span>${fmtDT(n.date)}</span><span class="${sCls}">${sLabel} (${sc.toFixed(2)})</span></div>
    </div>`;
  }).join("");
}

function renderDrawerDaily(data, sym){ /* optional future use */ }

function setDefaultNewsDateRange(){
  const fromInput = document.getElementById('newsDateFrom');
  const toInput = document.getElementById('newsDateTo');
  if(!fromInput || !toInput) return;
  const to = new Date();
  const from = new Date();
  from.setDate(to.getDate() - 7);
  const fmtDateInput = d => d.toISOString().slice(0,10);
  if(!toInput.value) toInput.value = fmtDateInput(to);
  if(!fromInput.value) fromInput.value = fmtDateInput(from);
}

/* ═══════════════════════════════════════════════════════════
   NEWS TAB
   ═══════════════════════════════════════════════════════════ */
async function loadAllNews(){
  el.newsGrid.innerHTML='<p class="muted">Đang tải tin tức …</p>';
  try{
    setDefaultNewsDateRange();
    const q = (document.getElementById('newsSearchInput')?.value||'').trim();
    const df = document.getElementById('newsDateFrom')?.value||'';
    const dt = document.getElementById('newsDateTo')?.value||'';
    let url = `${API}/api/news/search?limit=60`;
    if(q) url += `&q=${encodeURIComponent(q)}`;
    if(df) url += `&date_from=${df}`;
    if(dt) url += `&date_to=${dt}`;
    const r=await fetch(url);
    const j=await r.json();
    if(j.status==="ok"&&j.data.length){
      el.newsGrid.innerHTML=j.data.map(n=>{
        const sc=n.sentiment_score||0;
        const sCls=sc>0?"sent-pos":sc<0?"sent-neg":"sent-neu";
        const sLabel=sc>0?"Tích cực":sc<0?"Tiêu cực":"Trung lập";
        const snippet=(n.content||"").slice(0,180);
        const newsJson = JSON.stringify(n).replace(/"/g,'&quot;');
        return `<div class="news-card" onclick="openNewsModal(${newsJson})" style="cursor:pointer">
          <span class="nc-code">${n.stock_code}</span>
          <div class="nc-title">${n.title||"Untitled"}</div>
          ${snippet?`<div class="nc-snippet">${snippet}…</div>`:""}
          <div class="nc-meta"><span>${fmtDT(n.date)}</span><span class="${sCls}">${sLabel} (${sc.toFixed(2)})</span></div>
        </div>`;
      }).join("");
    } else {
      el.newsGrid.innerHTML='<p class="muted">Không có tin tức</p>';
    }
  }catch(e){
    el.newsGrid.innerHTML='<p class="muted">Lỗi tải tin tức</p>';
  }
}

/* ═══════════════════════════════════════════════════════════
   TABS
   ═══════════════════════════════════════════════════════════ */
el.tabs.addEventListener("click",e=>{
  const btn=e.target.closest(".tab");if(!btn)return;
  const tab=btn.dataset.tab;
  document.querySelectorAll(".tab").forEach(t=>t.classList.remove("active"));
  document.querySelectorAll(".tab-content").forEach(t=>t.classList.remove("active"));
  btn.classList.add("active");
  document.getElementById("tab-"+tab)?.classList.add("active");
  // Show/hide market filter & sector filter per tab
  const mfg = document.getElementById('marketFilterGroup');
  const sf = document.getElementById('sectorFilter');
  if(tab==='market'){
    if(mfg) mfg.style.display='';
    if(sf) sf.style.display='';
  } else {
    if(mfg) mfg.style.display='none';
    if(sf) sf.style.display='none';
  }
  if(tab==="news") loadAllNews();
  if(tab==="overview") loadMarketOverview();
  if(tab==="watchlist") loadWatchlist();
});

// Top tabs inside overview
document.addEventListener('click',e=>{
  const btn=e.target.closest('.ov-top-tab');
  if(!btn)return;
  document.querySelectorAll('.ov-top-tab').forEach(t=>t.classList.remove('active'));
  btn.classList.add('active');
  currentTopTab=btn.dataset.top;
  renderTopTable();
});

/* ═══════════════════════════════════════════════════════════
   EVENT LISTENERS
   ═══════════════════════════════════════════════════════════ */
el.search.addEventListener("input",()=>renderTable());
el.sort.addEventListener("change",e=>{sortMode=e.target.value;renderTable()});
document.getElementById('sectorFilter')?.addEventListener('change',e=>{sectorFilter=e.target.value;renderTable();updateStats()});
// Market filter buttons
document.getElementById('marketFilterGroup')?.addEventListener('click',e=>{
  const btn=e.target.closest('.mf-btn');if(!btn)return;
  document.querySelectorAll('.mf-btn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  marketFilter=btn.dataset.market||'';
  renderTable();updateStats();
});
el.mobRoundBtn?.addEventListener("click",()=>setMoLotMode("round"));
el.mobOddBtn?.addEventListener("click",()=>setMoLotMode("odd"));
setMoLotMode(moLotMode, false);
el.drawerClose.addEventListener("click",closeDrawer);
el.overlay.addEventListener("click",closeDrawer);
const onChartModeClick = event => {
  const btn = event.target.closest(".chart-mode-btn");
  if(!btn) return;
  setPriceChartMode(btn.dataset.mode || "line");
};
el.drChartMode?.addEventListener("click", onChartModeClick);
el.dockChartMode?.addEventListener("click", onChartModeClick);
const onCandleCanvasClick = event => {
  if(!selected || !isCandlestickMode()) return;
  if(candleDragSelection.active) return;
  if(candlePanDrag.active) return;
  if(candleDragSelection.suppressClickUntil > Date.now()) return;
  if(candlePanDrag.suppressClickUntil > Date.now()) return;

  const canvas = event.currentTarget;
  if(canvas !== getActiveChartCanvas()) return;

  const state = getCandlestickStateForCanvas(canvas);
  if(!state) return;

  const rect = canvas.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  if(x < state.padLeft || x > state.padLeft + state.plotWidth) return;
  if(y < state.padTop || y > state.padTop + state.plotHeight) return;

  const idx = getCandleIndexFromClientX(state, canvas, event.clientX, false);
  if(idx < 0) return;
  const candle = state.candles[idx];
  if(!candle) return;

  toggleSelectedCandle(candle);
  renderCandleSelectionInfo();
  redrawActiveCandlestickChart();
};
const onCandleCanvasMouseDown = event => {
  if(event.button !== 0 || !selected) return;

  const canvas = event.currentTarget;
  if(canvas !== getActiveChartCanvas()) return;

  if(!isCandlestickMode()){
    if(beginLinePanDrag(canvas, event.clientX)){
      event.preventDefault();
    }
    return;
  }

  const state = getCandlestickStateForCanvas(canvas);
  if(!state) return;

  const rect = canvas.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  if(x < state.padLeft || x > state.padLeft + state.plotWidth) return;
  if(y < state.padTop || y > state.padTop + state.plotHeight) return;

  const idx = getCandleIndexFromClientX(state, canvas, event.clientX, false);
  if(idx < 0) return;

  if(event.altKey || event.shiftKey){
    beginCandlePanDrag(canvas, event.clientX);
  }else{
    beginCandleDragSelection(canvas, state, idx);
  }
  event.preventDefault();
};
const onWindowCandleMouseMove = event => {
  if(linePanDrag.active){
    if(event.buttons === 0){
      finishLinePanDrag();
      return;
    }
    if(updateLinePanDrag(event.clientX)){
      event.preventDefault();
    }
    return;
  }

  if(candlePanDrag.active){
    if(event.buttons === 0){
      finishCandlePanDrag();
      return;
    }
    if(updateCandlePanDrag(event.clientX)){
      event.preventDefault();
    }
    return;
  }

  if(!candleDragSelection.active) return;
  if(event.buttons === 0){
    finishCandleDragSelection();
    return;
  }
  if(updateCandleDragSelection(event.clientX)){
    event.preventDefault();
  }
};
const onWindowCandleMouseUp = event => {
  if(linePanDrag.active){
    finishLinePanDrag();
    return;
  }

  if(candlePanDrag.active){
    finishCandlePanDrag();
    return;
  }

  if(!candleDragSelection.active) return;
  finishCandleDragSelection(event.clientX);
};
const onCandleCanvasWheel = event => {
  if((!event.ctrlKey && !event.shiftKey && !event.metaKey) || !selected) return;
  const canvas = event.currentTarget;
  if(canvas !== getActiveChartCanvas()) return;
  const primaryDelta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;

  if(!isCandlestickMode()){
    if(event.ctrlKey || event.metaKey){
      event.preventDefault();
      const rect = canvas.getBoundingClientRect();
      const focusRatio = rect.width > 0 ? (event.clientX - rect.left) / rect.width : 0.5;
      const zoomIn = primaryDelta < 0;
      if(zoomLineViewport(zoomIn, focusRatio)){
        refreshRealtimeLineViewport();
      }else if(!zoomIn){
        tryAutoExpandLineHistory("zoom-out");
      }
      return;
    }

    if(event.shiftKey){
      event.preventDefault();
      const direction = primaryDelta > 0 ? 1 : -1;
      if(panLineViewport(direction)){
        refreshRealtimeLineViewport();
      }else if(direction < 0){
        tryAutoExpandLineHistory("pan-left");
      }
    }
    return;
  }

  if(!drawerCandles?.length || drawerCandleSymbol !== selected) return;

  event.preventDefault();
  const rect = canvas.getBoundingClientRect();
  const focusRatio = rect.width > 0 ? (event.clientX - rect.left) / rect.width : 0.5;

  if(event.ctrlKey || event.metaKey){
    const zoomIn = primaryDelta < 0;
    if(zoomCandlestickViewport(zoomIn, focusRatio)){
      redrawActiveCandlestickChart();
      if(zoomIn){
        maybeLoadMinuteDetailForCandlestick();
      }
    }else if(zoomIn){
      maybeLoadMinuteDetailForCandlestick();
    }
    return;
  }

  if(event.shiftKey){
    const direction = primaryDelta > 0 ? 1 : -1;
    if(panCandlestickViewport(direction)){
      redrawActiveCandlestickChart();
    }
  }
};
el.drChart?.addEventListener("mousedown", onCandleCanvasMouseDown);
el.dockChart?.addEventListener("mousedown", onCandleCanvasMouseDown);
el.drChart?.addEventListener("click", onCandleCanvasClick);
el.dockChart?.addEventListener("click", onCandleCanvasClick);
el.drChart?.addEventListener("wheel", onCandleCanvasWheel, { passive: false });
el.dockChart?.addEventListener("wheel", onCandleCanvasWheel, { passive: false });
window.addEventListener("mousemove", onWindowCandleMouseMove);
window.addEventListener("mouseup", onWindowCandleMouseUp);
window.addEventListener("blur", () => {
  if(linePanDrag.active) finishLinePanDrag();
  if(candlePanDrag.active) finishCandlePanDrag();
  if(candleDragSelection.active) finishCandleDragSelection();
});
const onCandleInfoAction = event => {
  const trigger = event.target.closest('[data-action="clear-candle-select"]');
  if(!trigger || !selected) return;
  resetSelectedCandles(selected);
  renderCandleSelectionInfo();
  redrawActiveCandlestickChart();
};
el.drCandleInfo?.addEventListener("click", onCandleInfoAction);
el.dockCandleInfo?.addEventListener("click", onCandleInfoAction);
el.drInterval.addEventListener("change",()=>{
  syncChartIntervals(el.drInterval);
  candleCycleAuto = true;
  syncRecommendedCandleCycle(getActiveChartInterval(), { force: true });
  selectedRunlengthSegmentId = "__all";
  candleMinuteDetailLoading = false;
  resetCandleViewport();
  resetLineViewport();
  if(selected) resetSelectedCandles(selected);
  renderCandleSelectionInfo();
  if(selected)loadOHLCV(selected);
});
el.dockInterval?.addEventListener("change",()=>{
  syncChartIntervals(el.dockInterval);
  candleCycleAuto = true;
  syncRecommendedCandleCycle(getActiveChartInterval(), { force: true });
  selectedRunlengthSegmentId = "__all";
  candleMinuteDetailLoading = false;
  resetCandleViewport();
  resetLineViewport();
  if(selected) resetSelectedCandles(selected);
  renderCandleSelectionInfo();
  if(selected)loadOHLCV(selected);
});
const onCandleIntervalChange = sourceEl => {
  candleCycleAuto = false;
  syncCandleIntervals(sourceEl);
  selectedRunlengthSegmentId = "__all";
  candleMinuteDetailLoading = false;
  resetCandleViewport();
  if(selected) resetSelectedCandles(selected);
  renderCandleSelectionInfo();

  if(!selected) return;
  if(isCandlestickMode()){
    loadOHLCV(selected);
  }else{
    updatePriceChartTitle();
  }
};
el.drCandleInterval?.addEventListener("change",()=>onCandleIntervalChange(el.drCandleInterval));
el.dockCandleInterval?.addEventListener("change",()=>onCandleIntervalChange(el.dockCandleInterval));
el.dockRunlengthStrip?.addEventListener("click",event=>{
  const chip = event.target.closest(".rl-chip");
  if(!chip || !selected || !isCandlestickMode()) return;
  selectedRunlengthSegmentId = chip.dataset.seg || "__all";
  renderRunlengthStrip(drawerRunlengthSegments, selected);
  redrawActiveCandlestickChart();
});
document.getElementById('drFavBtn')?.addEventListener('click',()=>{if(selected)toggleWatchlist(selected)});
document.addEventListener("keydown",e=>{
  if(e.key === "Escape"){
    closeDrawer();
    closeStatPopup();
    closeNewsModal();
    closeSymbolModal();
    return;
  }
  if(e.shiftKey && !e.ctrlKey && !e.metaKey && (e.key === "ArrowLeft" || e.key === "ArrowRight")){
    if(!selected || !el.drawer?.classList.contains("open")) return;
    e.preventDefault();
    const direction = e.key === "ArrowRight" ? 1 : -1;
    if(isCandlestickMode()){
      if(panCandlestickViewport(direction)){
        redrawActiveCandlestickChart();
      }
    }else{
      if(panLineViewport(direction)){
        refreshRealtimeLineViewport();
      }else if(direction < 0){
        tryAutoExpandLineHistory("key-left");
      }
    }
    return;
  }
  if((e.ctrlKey || e.metaKey) && (e.key === "0" || e.code === "Digit0")){
    if(!selected || !el.drawer?.classList.contains("open")) return;
    e.preventDefault();
    if(isCandlestickMode()){
      candleMinuteDetailLoading = false;
      if(isAutoMinuteDetailActive()){
        resetCandleViewport();
        loadOHLCV(selected);
      }else{
        resetCandleViewport();
        redrawActiveCandlestickChart();
      }
    }else{
      resetLineViewport();
      refreshRealtimeLineViewport();
    }
  }
});
window.addEventListener("resize",()=>{
  if(!selected || !el.drawer?.classList.contains("open")) return;

  const modeBefore = isDockChartActive();
  const modeAfter = useExternalChartDock();
  if(modeBefore !== modeAfter){
    updateChartDockMode();
    loadOHLCV(selected);
    return;
  }

  if(drawerResizeTimer) clearTimeout(drawerResizeTimer);
  drawerResizeTimer = setTimeout(()=>{
    if(selected && el.drawer?.classList.contains("open")){
      redrawActivePriceChart();
    }
  }, 120);
});

// ── Stat card click → show list popup ────────────────────
document.getElementById('cardUp')?.addEventListener('click',()=>openStatPopup('up'));
document.getElementById('cardDown')?.addEventListener('click',()=>openStatPopup('down'));
document.getElementById('cardFlat')?.addEventListener('click',()=>openStatPopup('flat'));
document.getElementById('cardVol')?.addEventListener('click',()=>openStatPopup('volume'));
document.getElementById('statPopupClose')?.addEventListener('click',closeStatPopup);
document.getElementById('statPopupOverlay')?.addEventListener('click',closeStatPopup);

function openStatPopup(mode){
  const popup = document.getElementById('statPopup');
  const overlay = document.getElementById('statPopupOverlay');
  const title = document.getElementById('statPopupTitle');
  const body = document.getElementById('statPopupBody');
  if(!popup||!overlay) return;

  let arr = Object.values(stocks);
  if(marketFilter) arr = arr.filter(s=>matchesMarket(s.symbol));
  if(sectorFilter) arr = arr.filter(s=>getSector(s.symbol)===sectorFilter);
  let filtered;
  if(mode==='up'){
    filtered = arr.filter(s=>s.pct>0).sort((a,b)=>(b.pct||0)-(a.pct||0));
    title.textContent = `Cổ phiếu tăng giá (${filtered.length})`;
    title.style.color = 'var(--green)';
  } else if(mode==='down'){
    filtered = arr.filter(s=>s.pct<0).sort((a,b)=>(a.pct||0)-(b.pct||0));
    title.textContent = `Cổ phiếu giảm giá (${filtered.length})`;
    title.style.color = 'var(--red)';
  } else if(mode==='volume'){
    filtered = arr.filter(s=>(s.volume||0)>0).sort((a,b)=>(b.volume||0)-(a.volume||0));
    title.textContent = `Top khối lượng giao dịch (${filtered.length})`;
    title.style.color = 'var(--accent)';
  } else {
    filtered = arr.filter(s=>(s.pct||0)===0).sort((a,b)=>a.symbol.localeCompare(b.symbol));
    title.textContent = `Cổ phiếu đứng giá (${filtered.length})`;
    title.style.color = 'var(--text-2,#f59e0b)';
  }

  const isVolumeMode = mode==='volume';
  const headerHtml = `<div class="sp-row sp-header">
    <span class="sp-sym">Mã</span>
    <span class="sp-price">Giá</span>
    <span class="sp-change">${isVolumeMode?'KL':'Thay đổi'}</span>
    <span class="sp-pct">%</span>
    <span class="sp-exchange">Sàn</span>
  </div>`;

  if(!filtered.length){
    body.innerHTML = '<div class="sp-empty">Không có mã nào</div>';
  } else {
    body.innerHTML = headerHtml + filtered.map(s=>{
      const c = cls(s.pct);
      return `<div class="sp-row" onclick="closeStatPopup();openDrawer('${s.symbol}')">
        <span class="sp-sym">${s.symbol}</span>
        <span class="sp-price ${c}">${fmt(s.price)}</span>
        <span class="sp-change ${c}">${isVolumeMode?fmtV(s.volume):((s.change>=0?'+':'')+fmt(s.change))}</span>
        <span class="sp-pct ${c}">${(s.pct>=0?'+':'')+fmt(s.pct)}%</span>
        <span class="sp-exchange">${s.exchange||''}</span>
      </div>`;
    }).join('');
  }

  overlay.classList.add('open');
  popup.classList.add('open');
}

function closeStatPopup(){
  document.getElementById('statPopup')?.classList.remove('open');
  document.getElementById('statPopupOverlay')?.classList.remove('open');
}

// ⌘K / Ctrl+K → focus search
document.addEventListener("keydown",e=>{
  if((e.metaKey||e.ctrlKey)&&e.key==="k"){e.preventDefault();el.search.focus();el.search.select()}
});

// News tab filter buttons
document.getElementById('newsFilterBtn')?.addEventListener('click',()=>loadAllNews());
document.getElementById('newsClearBtn')?.addEventListener('click',()=>{
  const si=document.getElementById('newsSearchInput');
  const df=document.getElementById('newsDateFrom');
  const dt=document.getElementById('newsDateTo');
  if(si) si.value='';
  if(df) df.value='';
  if(dt) dt.value='';
  setDefaultNewsDateRange();
  loadAllNews();
});
document.getElementById('newsSearchInput')?.addEventListener('keydown',e=>{
  if(e.key==='Enter') loadAllNews();
});

function openSymbolModal(){
  document.getElementById('symbolModal')?.classList.add('open');
  document.getElementById('symbolModalOverlay')?.classList.add('open');
  setSymbolFormStatus('');
  const input = document.getElementById('symbolInput');
  if(input){
    input.value = '';
    input.focus();
  }
}

function closeSymbolModal(){
  document.getElementById('symbolModal')?.classList.remove('open');
  document.getElementById('symbolModalOverlay')?.classList.remove('open');
  document.getElementById('symbolForm')?.reset();
  setSymbolFormStatus('');
}

document.getElementById('addSymbolBtn')?.addEventListener('click', openSymbolModal);
document.getElementById('symbolModalClose')?.addEventListener('click', closeSymbolModal);
document.getElementById('symbolModalOverlay')?.addEventListener('click', closeSymbolModal);
document.getElementById('symbolCancelBtn')?.addEventListener('click', closeSymbolModal);
el.syncBtn?.addEventListener('click', ()=>resyncData(true));

document.getElementById('symbolForm')?.addEventListener('submit', async e=>{
  e.preventDefault();
  const input = document.getElementById('symbolInput');
  const market = document.getElementById('symbolMarket');
  const submitBtn = document.getElementById('symbolSubmitBtn');
  const symbol = input?.value?.trim()?.toUpperCase();
  const selectedMarket = market?.value || 'vn';
  if(!symbol) return;

  try{
    setSymbolFormStatus(`Dang them ${symbol} vao he thong...`, 'pending');
    if(submitBtn){ submitBtn.disabled = true; submitBtn.textContent = 'Dang them...'; }
    const r = await fetch(`${API}/api/system/symbols`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({symbol, market: selectedMarket}),
    });
    const j = await r.json().catch(()=>({}));
    if(!r.ok || j.status !== 'ok'){
      throw new Error(j.detail || `Khong the them ma (${r.status})`);
    }
    await loadSymbolRegistry();
    await resyncData(false);
    setSymbolFormStatus(
      `${symbol} da them vao ${selectedMarket === 'vn' ? 'Viet Nam' : 'The gioi'} o partition ${j.data.partition}.`,
      'ok'
    );
    toast(
      `${symbol} -> p${j.data.partition} (${j.data.topic}, ${j.data.topic_partitions} partitions)`,
      'ok'
    );
    setTimeout(()=>closeSymbolModal(), 700);
  }catch(err){
    console.error('add symbol error:', err);
    setSymbolFormStatus(`Loi: ${err.message || err}`, 'err');
    toast(`Khong the them ma: ${err.message || err}`, 'err');
  }finally{
    if(submitBtn){ submitBtn.disabled = false; submitBtn.textContent = 'Them ma'; }
  }
});

/* ═══════════════════════════════════════════════════════════
   MARKET OVERVIEW (Thị trường)
   ═══════════════════════════════════════════════════════════ */
let _ovData = null; // cached overview data
let sentimentChart = null; // sentiment chart instance
let _sentData = null; // cached sentiment data

// Single global market filter for overview tab (''=all, 'vn', 'world')
let _ovMarket = '';

function _ovMatchesMarket(sym){
  if(!_ovMarket) return true;
  if(_ovMarket==='vn') return VN_STOCKS.has(sym);
  if(_ovMarket==='world') return WORLD_STOCKS.has(sym);
  return true;
}

function _refreshAllOverview(){
  if(_ovData){
    renderBreadthChart(_ovData.breadth);
    renderTopTable();
    renderVolumeTop10Chart();
    renderOverviewSignalChart();
    renderOverviewAbnormalBoard();
  }
  if(_sentData) renderSentimentChart(_sentData);
}

// Wire up single global overview market filter
function _initOvMarketFilters(){
  document.getElementById('ovGlobalFilter')?.querySelectorAll('.ov-mf-btn').forEach(btn=>{
    btn.addEventListener('click',()=>{
      document.getElementById('ovGlobalFilter').querySelectorAll('.ov-mf-btn').forEach(b=>b.classList.remove('active'));
      btn.classList.add('active');
      _ovMarket = btn.dataset.ovmarket;
      _refreshAllOverview();
    });
  });
}

// Initialize filter on DOMContentLoaded
document.addEventListener('DOMContentLoaded',()=>_initOvMarketFilters());

async function loadMarketOverview(){
  try{
    const [rOv, rSent] = await Promise.all([
      fetch(`${API}/api/market/overview`),
      fetch(`${API}/api/sentiment/overview`),
    ]);
    const jOv = await rOv.json();
    const jSent = await rSent.json();
    if(jOv.status==="ok"){
      _ovData = jOv.data;
      if(jOv.data?.alerts){
        ingestChangepointAlerts(jOv.data);
      }
      renderBreadthChart(jOv.data.breadth);
      renderTopTable();
      renderVolumeTop10Chart();
      renderOverviewSignalChart();
      renderOverviewAbnormalBoard();
    }
    if(jSent.status==="ok"){
      _sentData = jSent.data;
      renderSentimentChart(jSent.data);
    }
  }catch(e){
    console.error('market overview error:',e);
  }
}

function renderBreadthChart(b){
  const canvas = document.getElementById('breadthChart');
  if(!canvas) return;
  if(breadthChart){breadthChart.destroy();breadthChart=null;}

  // If market filter is active, recompute breadth from filtered stocks
  let labels = b.labels;
  let values = b.values;
  let total = b.total;
  let advance = b.advancers;
  let decline = b.decliners;

  if(_ovMarket && _ovData && _ovData.stocks){
    const fStocks = _ovData.stocks.filter(s=>_ovMatchesMarket(s.symbol));
    // Recompute breadth buckets
    const buckets = [0,0,0,0,0,0,0,0,0,0,0]; // 11 buckets matching _bucketRange
    let adv=0, dec=0;
    fStocks.forEach(s=>{
      const p = s.pct||0;
      let idx;
      if(p<-7) idx=0;
      else if(p<-5) idx=1;
      else if(p<-3) idx=2;
      else if(p<-1) idx=3;
      else if(p<0) idx=4;
      else if(p===0) idx=5;
      else if(p<1) idx=6;
      else if(p<3) idx=7;
      else if(p<5) idx=8;
      else if(p<7) idx=9;
      else idx=10;
      buckets[idx]++;
      if(p>0) adv++;
      else if(p<0) dec++;
    });
    values = buckets;
    total = fStocks.length;
    advance = adv;
    decline = dec;
  }

  // Color each bar: red for negative buckets, yellow for 0%, green for positive
  const colors = labels.map(l=>{
    if(l==='0%') return '#f59e0b';
    if(l.includes('-') || l.startsWith('<')) return '#ef4444';
    return '#10b981';
  });

  document.getElementById('ovTotal').textContent = `Tổng: ${total}`;
  document.getElementById('ovDecline').textContent = `Giảm (Decliners): ${decline}`;
  document.getElementById('ovAdvance').textContent = `Tăng (Advancers): ${advance}`;

  breadthChart = new Chart(canvas, {
    type:'bar',
    data:{
      labels,
      datasets:[{
        data:values,
        backgroundColor:colors,
        borderRadius:4,
        maxBarThickness:42,
      }]
    },
    options:{
      responsive:true,
      maintainAspectRatio:false,
      onClick:(evt, elements)=>{
        if(elements.length){
          const idx = elements[0].index;
          showBreadthStocks(idx, labels[idx]);
        }
      },
      plugins:{
        legend:{display:false},
        tooltip:{
          backgroundColor:'#161b26',titleColor:'#f0f2f8',bodyColor:'#c3c8d8',
          borderColor:'#1f2739',borderWidth:1,
          callbacks:{
            label: ctx => `${ctx.parsed.y} mã — click để xem`
          }
        },
      },
      scales:{
        x:{
          ticks:{color:'#7d849b',font:{size:11}},
          grid:{display:false},
        },
        y:{
          beginAtZero:true,
          ticks:{color:'#4e556b',stepSize:1,precision:0},
          grid:{color:'rgba(31,39,57,.5)'},
        },
      },
    }
  });
}

function _bucketRange(idx){
  // Map bucket index to [min, max) percent range
  const ranges = [
    [-Infinity,-7], [-7,-5], [-5,-3], [-3,-1], [-1,0],
    [0,0],
    [0,1], [1,3], [3,5], [5,7], [7,Infinity]
  ];
  return ranges[idx] || [0,0];
}

function showBreadthStocks(bucketIdx, label){
  const container = document.getElementById('ovBreadthStocks');
  if(!container || !_ovData) return;

  const [lo, hi] = _bucketRange(bucketIdx);
  const matched = _ovData.stocks.filter(s=>{
    if(!_ovMatchesMarket(s.symbol)) return false;
    const p = s.pct || 0;
    if(bucketIdx===5) return p===0; // exact 0
    if(bucketIdx===0) return p < -7; // < -7%
    if(bucketIdx===10) return p >= 7; // >= 7%
    return p >= lo && p < hi;
  });

  if(!matched.length){
    container.innerHTML = `<div class="obs-header"><span class="obs-title">${label}: không có mã</span>
      <button class="obs-close" onclick="closeBreadthStocks()">&times;</button></div>`;
    container.classList.add('open');
    return;
  }

  container.innerHTML = `<div class="obs-header">
    <span class="obs-title">${label} — ${matched.length} mã</span>
    <button class="obs-close" onclick="closeBreadthStocks()">&times;</button>
  </div>
  <div class="obs-chips">${matched.map(s=>{
    const c = cls(s.pct);
    return `<div class="obs-chip" onclick="openDrawer('${s.symbol}')">
      <span class="obs-sym">${s.symbol}</span>
      <span class="obs-pct ${c}">${(s.pct>=0?'+':'')+fmt(s.pct)}%</span>
    </div>`;
  }).join('')}</div>`;
  container.classList.add('open');
}

function closeBreadthStocks(){
  const c = document.getElementById('ovBreadthStocks');
  if(c) c.classList.remove('open');
}

/* ── Sentiment Chart ─────────────────────────────────────── */
function renderSentimentChart(data){
  const canvas = document.getElementById('sentimentChart');
  if(!canvas || !data) return;
  if(sentimentChart){sentimentChart.destroy();sentimentChart=null;}

  // Recompute summary if market filter is active
  let {positive, negative, neutral} = data.summary;
  if(_ovMarket && data.stocks){
    positive=0; negative=0; neutral=0;
    data.stocks.forEach(s=>{
      if(!_ovMatchesMarket(s.symbol)) return;
      positive += s.positive||0;
      negative += s.negative||0;
      neutral  += s.neutral||0;
    });
  }

  sentimentChart = new Chart(canvas, {
    type:'doughnut',
    data:{
      labels:['Tích cực','Tiêu cực','Trung lập'],
      datasets:[{
        data:[positive, negative, neutral],
        backgroundColor:['#10b981','#ef4444','#6b7280'],
        borderWidth:0,
        hoverOffset:6,
      }]
    },
    options:{
      responsive:true,
      maintainAspectRatio:false,
      cutout:'60%',
      onClick:(evt, elements)=>{
        if(elements.length){
          const idx = elements[0].index;
          const mode = ['positive','negative','neutral'][idx];
          showSentimentStocks(mode);
        }
      },
      plugins:{
        legend:{display:false},
        tooltip:{
          backgroundColor:'#161b26',titleColor:'#f0f2f8',bodyColor:'#c3c8d8',
          borderColor:'#1f2739',borderWidth:1,
          callbacks:{
            label: ctx => {
              const total = ctx.dataset.data.reduce((a,b)=>a+b,0);
              const pct = total>0?((ctx.parsed/total)*100).toFixed(1):'0';
              return ` ${ctx.label}: ${ctx.parsed} (${pct}%) — click để xem`;
            }
          }
        },
      },
    }
  });
}

function showSentimentStocks(mode){
  const container = document.getElementById('ovSentimentStocks');
  if(!container || !_sentData) return;

  let filtered;
  let title;
  const mktFilter = s => _ovMatchesMarket(s.symbol);
  if(mode==='positive'){
    filtered = _sentData.stocks.filter(s=>s.positive>0 && mktFilter(s)).sort((a,b)=>b.avg_score-a.avg_score);
    title = `Mã có tin tích cực (${filtered.length})`;
  } else if(mode==='negative'){
    filtered = _sentData.stocks.filter(s=>s.negative>0 && mktFilter(s)).sort((a,b)=>a.avg_score-b.avg_score);
    title = `Mã có tin tiêu cực (${filtered.length})`;
  } else {
    filtered = _sentData.stocks.filter(s=>s.neutral>0 && mktFilter(s)).sort((a,b)=>b.neutral-a.neutral);
    title = `Mã có tin trung lập (${filtered.length})`;
  }

  if(!filtered.length){
    container.innerHTML = `<div class="obs-header"><span class="obs-title">${title}: không có mã</span>
      <button class="obs-close" onclick="closeSentimentStocks()">&times;</button></div>`;
    container.classList.add('open');
    return;
  }

  container.innerHTML = `<div class="obs-header">
    <span class="obs-title">${title}</span>
    <button class="obs-close" onclick="closeSentimentStocks()">&times;</button>
  </div>
  <div class="obs-chips">${filtered.map(s=>{
    // Use the selected sentiment mode color, not avg_score
    const modeColor = mode==='positive'?'up':mode==='negative'?'down':'flat';
    const sc = s.avg_score;
    return `<div class="obs-chip" onclick="showSentimentNews('${s.symbol}')">
      <span class="obs-sym">${s.symbol}</span>
      <span class="obs-pct ${modeColor}">${sc>=0?'+':''}${sc.toFixed(3)}</span>
      <span style="font-size:.65rem;color:var(--text-3)">${s.total} tin</span>
    </div>`;
  }).join('')}</div>
  <div class="sent-news-panel" id="sentNewsPanel"></div>`;
  container.classList.add('open');
}

function closeSentimentStocks(){
  const c = document.getElementById('ovSentimentStocks');
  if(c){c.classList.remove('open');c.innerHTML='';}
}

async function showSentimentNews(symbol){
  const panel = document.getElementById('sentNewsPanel');
  if(!panel) return;
  panel.innerHTML='<p class="muted" style="padding:10px">Đang tải tin …</p>';
  try{
    const r = await fetch(`${API}/api/news/${symbol}?limit=10`);
    const j = await r.json();
    if(j.status==='ok' && j.data.length){
      panel.innerHTML=`<h4 style="margin:10px 0 6px;font-size:.85rem;color:var(--accent)">${symbol} — Tin tức</h4>` +
        j.data.map(n=>{
          const sc=n.sentiment_score||0;
          const sCls=sc>0?'sent-pos':sc<0?'sent-neg':'sent-neu';
          const sLabel=sc>0?'Tích cực':sc<0?'Tiêu cực':'Trung lập';
          return `<div class="dn-item" style="margin-bottom:6px;padding:8px 12px;background:var(--bg-2);border-radius:6px">
            <div class="dn-title"><a href="${n.link||'#'}" target="_blank">${n.title||'Untitled'}</a></div>
            <div class="dn-meta"><span>${fmtDT(n.date)}</span><span class="${sCls}">${sLabel} (${sc.toFixed(2)})</span></div>
          </div>`;
        }).join('');
    } else {
      panel.innerHTML='<p class="muted" style="padding:10px">Không có tin tức</p>';
    }
  }catch(e){
    panel.innerHTML='<p class="muted" style="padding:10px">Lỗi tải tin</p>';
  }
}

/* ── Top 10 Volume Chart ──────────────────────────────────── */
function renderVolumeTop10Chart(){
  const canvas = document.getElementById('volumeTop10Chart');
  if(!canvas || !_ovData) return;
  if(volumeTop10Chart){volumeTop10Chart.destroy();volumeTop10Chart=null;}

  const filteredStocks = _ovData.stocks.filter(s=>_ovMatchesMarket(s.symbol));
  const top10 = [...filteredStocks].sort((a,b)=>(b.volume||0)-(a.volume||0)).slice(0,10).reverse();
  const labels = top10.map(s=>s.symbol);
  const volumes = top10.map(s=>s.volume||0);
  const colors = top10.map(s=>s.pct>=0?'rgba(16,185,129,.75)':'rgba(239,68,68,.75)');
  const borderColors = top10.map(s=>s.pct>=0?'#10b981':'#ef4444');

  volumeTop10Chart = new Chart(canvas, {
    type:'bar',
    data:{
      labels,
      datasets:[{
        label:'Khối lượng',
        data:volumes,
        backgroundColor:colors,
        borderColor:borderColors,
        borderWidth:1,
        borderRadius:6,
        maxBarThickness:32,
      }]
    },
    plugins:[{
      id:'volumeLabels',
      afterDatasetsDraw:(chart)=>{
        const ctx=chart.ctx;
        const meta=chart.getDatasetMeta(0);
        meta.data.forEach((bar,i)=>{
          const val=volumes[i];
          const isDark=document.documentElement.getAttribute('data-theme')==='dark';
          ctx.fillStyle=isDark?'#c3c8d8':'#374151';
          ctx.font='600 11px "JetBrains Mono", monospace';
          ctx.textAlign='left';
          ctx.textBaseline='middle';
          ctx.fillText(fmtV(val),bar.x+8,bar.y);
        });
      }
    }],
    options:{
      indexAxis:'y',
      responsive:true,
      maintainAspectRatio:false,
      plugins:{
        legend:{display:false},
        tooltip:{
          backgroundColor:'#161b26',titleColor:'#f0f2f8',bodyColor:'#c3c8d8',
          borderColor:'#1f2739',borderWidth:1,
          callbacks:{
            title:ctx=>ctx[0].label,
            label:ctx=>`Khối lượng: ${Number(ctx.parsed.x).toLocaleString()}`
          }
        },
      },
      scales:{
        x:{
          beginAtZero:true,
          ticks:{color:'#4e556b',callback:v=>fmtV(v)},
          grid:{color:'rgba(31,39,57,.5)'},
        },
        y:{
          ticks:{color:'#7d849b',font:{size:12,weight:'600',family:"'JetBrains Mono', monospace"}},
          grid:{display:false},
        },
      },
    }
  });
}

function renderTopTable(){
  const container = document.getElementById('ovTopContent');
  if(!container || !_ovData) return;

  const fStocks = _ovData.stocks.filter(s=>_ovMatchesMarket(s.symbol));

  let rows, sortField, ascending = false;
  if(currentTopTab==='gainers'){
    rows = [...fStocks].filter(s=>(s.pct||0)>0).sort((a,b)=>(b.pct||0)-(a.pct||0));
  } else if(currentTopTab==='losers'){
    rows = [...fStocks].filter(s=>(s.pct||0)<0).sort((a,b)=>(a.pct||0)-(b.pct||0));
  } else {
    rows = [...fStocks].sort((a,b)=>(b.volume||0)-(a.volume||0));
  }

  // Show top 15
  rows = rows.slice(0,15);

  if(!rows.length){
    container.innerHTML = '<p class="muted" style="padding:20px;text-align:center">Không có dữ liệu</p>';
    return;
  }

  const isVol = currentTopTab==='volume';
  container.innerHTML = `<table class="ov-top-table">
    <thead><tr>
      <th>#</th>
      <th>Mã</th>
      <th>Giá</th>
      <th>${isVol?'Khối lượng':'Thay đổi %'}</th>
      <th>Tổng KL</th>
    </tr></thead>
    <tbody>${rows.map((s,i)=>{
      const c = cls(s.pct);
      return `<tr onclick="openDrawer('${s.symbol}')">
        <td style="color:var(--text-3)">${i+1}</td>
        <td><span class="ot-sym">${s.symbol}</span><span class="ot-name">${getCompanyName(s.symbol)}</span></td>
        <td class="num ${c}">${fmt(s.price)}</td>
        <td class="num ${c}">${isVol?fmtV(s.volume):((s.pct>=0?'+':'')+fmt(s.pct)+'%')}</td>
        <td class="num">${fmtV(s.volume)}</td>
      </tr>`;
    }).join('')}</tbody>
  </table>`;
}

function renderOverviewSignalChart(){
  const canvas = document.getElementById('overviewSignalChart');
  const summary = document.getElementById('ovSignalSummary');
  if(!canvas || !_ovData) return;
  if(overviewSignalChart){ overviewSignalChart.destroy(); overviewSignalChart = null; }

  const rows = _ovData.stocks
    .filter(s => _ovMatchesMarket(s.symbol))
    .map(s => ({ symbol: s.symbol, signal: cpSignals[s.symbol] }))
    .filter(item => item.signal)
    .map(item => ({
      symbol: item.symbol,
      cp_prob: Number(item.signal.cp_prob || 0),
      whale_score: Number(item.signal.whale_score || 0),
      run_length: Number(item.signal.expected_run_length || 0),
      regime_label: item.signal.regime_label || 'stable',
    }))
    .sort((a,b)=>Math.max(b.whale_score,b.cp_prob)-Math.max(a.whale_score,a.cp_prob))
    .slice(0, 12);

  if(summary){
    summary.innerHTML = rows.length ? rows.slice(0,6).map(row => `
      <button class="ov-signal-chip" onclick="openDrawer('${row.symbol}')">
        <strong>${row.symbol}</strong>
        <span>${fmtProb(row.cp_prob)}</span>
        <span class="${row.regime_label==='whale-watch'?'up':row.regime_label==='transition'?'flat':'down'}">${row.regime_label}</span>
      </button>
    `).join('') : '<span class="muted">Chưa có BOCPD summary cho thị trường đang lọc.</span>';
  }

  if(!rows.length){
    drawCanvasEmpty(canvas, 'Chua co du lieu BOCPD thi truong');
    return;
  }

  overviewSignalChart = new Chart(canvas, {
    data: {
      labels: rows.map(r => r.symbol),
      datasets: [
        {
          type: 'bar',
          label: 'CP prob',
          data: rows.map(r => r.cp_prob * 100),
          backgroundColor: 'rgba(239,68,68,.22)',
          borderColor: '#ef4444',
          borderWidth: 1,
          borderRadius: 6,
          yAxisID: 'y',
        },
        {
          type: 'line',
          label: 'Whale score',
          data: rows.map(r => r.whale_score * 100),
          borderColor: '#f59e0b',
          backgroundColor: 'rgba(245,158,11,.12)',
          tension: .28,
          pointRadius: 3,
          pointHoverRadius: 5,
          borderWidth: 2,
          yAxisID: 'y',
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      onClick: (evt, elements) => {
        if(elements?.length){
          const idx = elements[0].index;
          if(rows[idx]) openDrawer(rows[idx].symbol);
        }
      },
      plugins: {
        legend: { labels: { color: '#7d849b', boxWidth: 10 } },
        tooltip: {
          callbacks: {
            afterBody: items => {
              const idx = items?.[0]?.dataIndex;
              if(idx == null) return '';
              return `E[r_t]: ${fmt(rows[idx].run_length, 2)}`;
            }
          }
        }
      },
      scales: {
        x: {
          ticks: { color: '#7d849b', font: { family: "'JetBrains Mono', monospace" } },
          grid: { display: false },
        },
        y: {
          beginAtZero: true,
          ticks: { color: '#4e556b', callback: v => `${v.toFixed ? v.toFixed(1) : v}%` },
          grid: { color: 'rgba(31,39,57,.5)' },
          title: { display: true, text: '%', color: '#7d849b' },
        },
      },
    },
  });
}

function renderOverviewAbnormalBoard(){
  const container = document.getElementById('ovSignalAlerts');
  const countEl = document.getElementById('ovAlertCount');
  if(!container) return;

  const rows = (cpAlerts || [])
    .filter(row => _ovMatchesMarket(row.symbol))
    .slice(0, 8);

  if(countEl){
    const mlSummary = cpAlertSummary?.ml_forecast || null;
    if(!rows.length){
      countEl.textContent = 'Khong co canh bao';
    }else if(mlSummary && Number(mlSummary.predicted || 0) > 0){
      countEl.textContent = `${rows.length} ma can theo doi · ML ${mlSummary.up_forecast_count || 0} tang / ${mlSummary.down_forecast_count || 0} giam`;
    }else{
      countEl.textContent = `${rows.length} ma can theo doi`;
    }
  }

  if(!rows.length){
    container.innerHTML = `
      <div class="ov-alert-empty">
        <strong>Chua thay dau hieu bat thuong ro rang</strong>
        <span>Module search va ML forecast dang theo doi lien tuc cp_prob, whale_score, r_t va huong gia sau bat thuong.</span>
      </div>
    `;
    return;
  }

  container.innerHTML = rows.map((row, idx) => {
    const biasClass =
      row.bias === 'pump-watch' ? 'pump'
      : row.bias === 'dump-watch' ? 'dump'
      : 'volatile';
    const regimeClass =
      row.regime_label === 'whale-watch' ? 'up'
      : row.regime_label === 'transition' ? 'flat'
      : 'down';
    const pctClass = cls(Number(row.pct || 0));
    const mlDirection = String(row.ml_direction || '').toLowerCase();
    const mlClass = mlDirection === 'up' ? 'up' : mlDirection === 'down' ? 'down' : 'flat';
    const mlDirectionLabel = mlDirection === 'up' ? 'Tang'
      : mlDirection === 'down' ? 'Giam'
      : 'Chua ro';
    const mlProbUp = Number(row.ml_prob_up || 0);
    const mlProbDown = Number(row.ml_prob_down || 0);
    const mlExpectedSessions = Number(row.ml_expected_sessions || 0);
    const mlExpectedText = mlExpectedSessions > 0 ? `${fmt(mlExpectedSessions, 1)} phien` : '--';
    const mlText = row.ml_text || `Du kien ${mlExpectedText} ${mlDirectionLabel.toLowerCase()}`;
    const tags = Array.isArray(row.reason_tags) ? row.reason_tags : [];
    return `
      <button class="ov-alert-card ${biasClass}" onclick="openDrawer('${row.symbol}')">
        <div class="ov-alert-rank">${idx + 1}</div>
        <div class="ov-alert-main">
          <div class="ov-alert-topline">
            <span class="ov-alert-symbol">${row.symbol}</span>
            <span class="ov-alert-company">${getCompanyName(row.symbol)}</span>
            <span class="cp-pill ${regimeClass}">${row.regime_label || 'watch'}</span>
          </div>
          <div class="ov-alert-meta">
            <span class="ov-alert-bias ${biasClass}">${row.bias_label || 'Bien dong bat thuong'}</span>
            <span class="ov-alert-pct ${pctClass}">${Number(row.pct || 0) >= 0 ? '+' : ''}${fmt(Number(row.pct || 0))}%</span>
            <span class="ov-alert-time">${fmtDT(row.event_time)}</span>
          </div>
          <div class="ov-alert-tags">
            ${tags.map(tag => `<span class="ov-alert-tag">${tag}</span>`).join('')}
          </div>
          <div class="ov-alert-reason">${row.reason_text || 'BOCPD dang danh dau thay doi che do giao dich'}</div>
          <div class="ov-alert-ml ${mlClass}">ML forecast: ${mlText}</div>
        </div>
        <div class="ov-alert-side">
          <div class="ov-alert-score">${Math.round(Number(row.suspicion_score || 0) * 100)}</div>
          <div class="ov-alert-score-label">Diem nghi van</div>
          <div class="ov-alert-metric">ML ${mlDirectionLabel} ~ ${mlExpectedText}</div>
          <div class="ov-alert-metric">ML ↑ ${fmtProb(mlProbUp)} ↓ ${fmtProb(mlProbDown)}</div>
          <div class="ov-alert-metric">Whale ${fmtProb(row.whale_score)}</div>
          <div class="ov-alert-metric">CP ${fmtProb(row.cp_prob)}</div>
          <div class="ov-alert-metric">r_t ${fmt(Number(row.expected_run_length || 0), 1)}</div>
        </div>
      </button>
    `;
  }).join('');
}

/* Theme toggle is defined inline in index.html <head> for reliability */

/* ═══════════════════════════════════════════════════════════
   WATCHLIST TAB
   ═══════════════════════════════════════════════════════════ */
let wlPriceChart = null;
let wlAllNews = [];
const chartColors = ['#f44336','#2196f3','#4caf50','#ff9800','#9c27b0','#00bcd4','#e91e63','#8bc34a','#ffc107','#673ab7'];
let wlCardRefreshFrame = null;
const wlCardRefreshQueue = new Set();
let wlSignalSignature = "";

function watchlistCardSelector(sym){
  const escaped = String(sym || "").replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  return `.wl-stock-card[data-sym="${escaped}"]`;
}

function scheduleWatchlistCardRefresh(symbols){
  (symbols || []).forEach(sym => {
    if(sym) wlCardRefreshQueue.add(String(sym));
  });
  if(!wlCardRefreshQueue.size || wlCardRefreshFrame != null) return;

  wlCardRefreshFrame = requestAnimationFrame(() => {
    wlCardRefreshFrame = null;
    const pending = [...wlCardRefreshQueue];
    wlCardRefreshQueue.clear();
    pending.forEach(sym => updateWatchlistCard(sym));
  });
}

function updateWatchlistCard(sym){
  const card = document.querySelector(watchlistCardSelector(sym));
  const s = stocks[sym];
  if(!card || !s) return;

  const c = cls(s?.pct);
  const vol = s?.volume ? fmtV(s.volume) : '--';
  const cp = cpSignals[sym] || null;
  const cpClass = !cp ? 'unknown' : cp.regime_label === 'whale-watch' ? 'up' : cp.regime_label === 'transition' ? 'flat' : 'down';
  const cpLabel = !cp ? 'Chua co' : cp.regime_label === 'whale-watch' ? 'Whale' : cp.regime_label === 'transition' ? 'Transition' : 'Stable';
  const ml = getSymbolMlForecast(sym);

  const priceEl = card.querySelector('[data-role="price"]');
  if(priceEl){
    priceEl.className = `wl-card-price ${c}`;
    priceEl.textContent = fmt(s.price);
  }

  const pctEl = card.querySelector('[data-role="pct"]');
  if(pctEl){
    pctEl.className = `wl-card-change ${c}`;
    pctEl.textContent = `${s.pct >= 0 ? '+' : ''}${fmt(s.pct)}%`;
  }

  const cpLabelEl = card.querySelector('[data-role="cpLabel"]');
  if(cpLabelEl){
    cpLabelEl.className = `cp-pill ${cpClass}`;
    cpLabelEl.textContent = cpLabel;
  }

  const cpMetaEl = card.querySelector('[data-role="cpMeta"]');
  if(cpMetaEl){
    cpMetaEl.textContent = cp ? `CP ${fmtProb(cp.cp_prob)} · r ${fmt(cp.expected_run_length,1)}` : 'BOCPD --';
  }

  const mlEl = card.querySelector('[data-role="ml"]');
  if(mlEl){
    mlEl.className = `wl-card-mono wl-card-ml ${ml ? ml.klass : ''}`.trim();
    mlEl.textContent = ml
      ? `ML ${ml.directionLabel} ${ml.expectedText} · up ${fmtProb(ml.probUp)} / down ${fmtProb(ml.probDown)}`
      : 'ML --';
  }

  const volEl = card.querySelector('[data-role="vol"]');
  if(volEl){
    volEl.textContent = vol;
  }

  const chgEl = card.querySelector('[data-role="chgAbs"]');
  if(chgEl){
    chgEl.className = `wl-stat-value ${c}`;
    chgEl.textContent = `${s.change >= 0 ? '+' : ''}${fmt(s.change)}`;
  }
}

function loadWatchlist(){
  const content = document.getElementById('watchlistContent');
  const newsSection = document.getElementById('wlNewsSection');
  const chartSection = document.getElementById('wlChartSection');
  const signalSection = document.getElementById('wlSignalSection');
  const wlInfo = document.getElementById('wlInfo');
  if(!content) return;

  const wlStocks = [...watchlist].filter(sym=>stocks[sym]);
  wlInfo.textContent = `${wlStocks.length} mã`;

  if(!wlStocks.length){
    content.innerHTML = '<p class="muted">Chưa có mã nào trong danh sách quan tâm. Hãy bấm ☆ ở cột cuối bảng giá để thêm.</p>';
    if(newsSection) newsSection.style.display='none';
    if(chartSection) chartSection.style.display='none';
    if(signalSection) signalSection.style.display='none';
    return;
  }

  // Render watchlist stock cards with labels
  content.innerHTML = `<div class="wl-stocks-grid">${wlStocks.map(sym=>{
    const s = stocks[sym];
    const c = cls(s?.pct);
    const vol = s?.volume ? fmtV(s.volume) : '--';
    const cp = cpSignals[sym] || null;
    const cpClass = !cp ? 'unknown' : cp.regime_label === 'whale-watch' ? 'up' : cp.regime_label === 'transition' ? 'flat' : 'down';
    const cpLabel = !cp ? 'Chua co' : cp.regime_label === 'whale-watch' ? 'Whale' : cp.regime_label === 'transition' ? 'Transition' : 'Stable';
    const ml = getSymbolMlForecast(sym);
    return `<div class="wl-stock-card" data-sym="${sym}" onclick="openDrawer('${sym}')">
      <div class="wl-card-top">
        <div>
          <div class="wl-card-sym">${sym}</div>
          <div class="wl-card-name">${getCompanyName(sym)}</div>
        </div>
        <button class="wl-remove-btn" onclick="event.stopPropagation();toggleWatchlist('${sym}');loadWatchlist()" title="Bỏ quan tâm">✕</button>
      </div>
      <div class="wl-card-row">
        <div class="wl-card-label">Giá hiện tại</div>
        <div class="wl-card-price ${c}" data-role="price">${s?fmt(s.price):'--'}</div>
      </div>
      <div class="wl-card-row">
        <div class="wl-card-label">Thay đổi</div>
        <div class="wl-card-change ${c}" data-role="pct">${s?((s.pct>=0?'+':'')+fmt(s.pct)+'%'):'--'}</div>
      </div>
      <div class="wl-card-signal">
        <span class="cp-pill ${cpClass}" data-role="cpLabel">${cpLabel}</span>
        <span class="wl-card-mono" data-role="cpMeta">${cp ? `CP ${fmtProb(cp.cp_prob)} · r ${fmt(cp.expected_run_length,1)}` : 'BOCPD --'}</span>
      </div>
      <div class="wl-card-mono wl-card-ml ${ml ? ml.klass : ''}" data-role="ml">
        ${ml ? `ML ${ml.directionLabel} ${ml.expectedText} · up ${fmtProb(ml.probUp)} / down ${fmtProb(ml.probDown)}` : 'ML --'}
      </div>
      <div class="wl-card-stats">
        <div class="wl-stat-item">
          <div class="wl-stat-value" data-role="vol">${vol}</div>
          <div class="wl-stat-label">KLGD</div>
        </div>
        <div class="wl-stat-item">
          <div class="wl-stat-value ${c}" data-role="chgAbs">${s?(s.change>=0?'+':'')+fmt(s.change):'--'}</div>
          <div class="wl-stat-label">+/- điểm</div>
        </div>
      </div>
    </div>`;
  }).join('')}</div>`;

  // Show chart and news sections
  if(chartSection) chartSection.style.display='block';
  if(signalSection) signalSection.style.display='block';
  if(newsSection) newsSection.style.display='block';
  
  // Load chart and news
  loadWatchlistChart(wlStocks);
  renderWatchlistSignalChart(wlStocks);
  loadWatchlistNews(wlStocks);
}

// Watchlist multi-line chart
async function loadWatchlistChart(symbols){
  const ctx = document.getElementById('wlPriceChart')?.getContext('2d');
  const canvas = document.getElementById('wlPriceChart');
  const legendEl = document.getElementById('wlChartLegend');
  if(!ctx) return;

  const interval = document.getElementById('wlChartInterval')?.value || '1h';
  
  // Fetch price data for each symbol
  const datasets = [];
  let allLabels = [];
  
  for(let i=0; i<symbols.length && i<10; i++){
    const sym = symbols[i];
    try{
      const resp = await fetch(`${API}/api/stocks/ohlcv/${encodeURIComponent(sym)}?interval=${encodeURIComponent(interval)}`);
      const data = await resp.json();
      if(data.status==='ok' && data.data?.length){
        const points = normalizePriceSeries(data.data);
        const firstPrice = points[0]?.y;
        if(!points.length || !Number.isFinite(firstPrice) || firstPrice === 0){
          continue;
        }
        const normalized = points.map(point => ({
          x: point.x,
          y: ((point.y - firstPrice) / firstPrice) * 100,
        }));

        datasets.push({
          label: sym,
          data: normalized,
          borderColor: chartColors[i % chartColors.length],
          backgroundColor: 'transparent',
          borderWidth: 2,
          tension: 0.3,
          pointRadius: 0,
          pointHoverRadius: 4,
        });
      }
    }catch(e){
      console.error(`Chart data error for ${sym}:`, e);
    }
  }

  if(!datasets.length){
    if(wlPriceChart){
      wlPriceChart.destroy();
      wlPriceChart = null;
    }
    if(canvas) drawCanvasEmpty(canvas, 'Chua co du lieu bieu do');
    if(legendEl) legendEl.innerHTML = '<span class="muted">Không có dữ liệu biểu đồ</span>';
    return;
  }

  // Destroy old chart
  if(wlPriceChart) wlPriceChart.destroy();

  // Create chart
  wlPriceChart = new Chart(ctx, {
    type: 'line',
    data: {
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y>=0?'+':''}${ctx.parsed.y.toFixed(2)}%`
          }
        }
      },
      scales: {
        x: {
          type: 'time',
          time: { tooltipFormat: 'dd/MM HH:mm' },
          grid: { color: 'rgba(150,150,150,.1)' },
          ticks: { color: '#888', maxTicksLimit: 8 }
        },
        y: { 
          grid: { color: 'rgba(150,150,150,.1)' }, 
          ticks: { 
            color: '#888',
            callback: v => (v>=0?'+':'')+v.toFixed(1)+'%'
          }
        }
      }
    }
  });

  // Render legend
  if(legendEl){
    legendEl.innerHTML = datasets.map(ds => 
      `<div class="wl-legend-item">
        <span class="wl-legend-color" style="background:${ds.borderColor}"></span>
        <span>${ds.label}</span>
      </div>`
    ).join('');
  }
}

function renderWatchlistSignalChart(symbols){
  const canvas = document.getElementById('wlSignalChart');
  const legendEl = document.getElementById('wlSignalLegend');
  if(!canvas) return;

  const rows = symbols
    .map(sym => ({ symbol: sym, signal: cpSignals[sym] }))
    .filter(item => item.signal)
    .map(item => ({
      symbol: item.symbol,
      cp_prob: Number(item.signal.cp_prob || 0) * 100,
      run_length: Number(item.signal.expected_run_length || 0),
      whale_score: Number(item.signal.whale_score || 0) * 100,
      regime_label: item.signal.regime_label || 'stable',
      ml: getSymbolMlForecast(item.symbol),
    }));

  const signature = rows.map(row =>
    `${row.symbol}:${row.cp_prob.toFixed(4)}:${row.run_length.toFixed(4)}:${row.whale_score.toFixed(4)}:${row.regime_label}`
  ).join('|');

  if(legendEl){
    legendEl.innerHTML = rows.length
      ? rows.map(row => `<div class="wl-legend-item">
          <span class="cp-pill ${row.regime_label==='whale-watch'?'up':row.regime_label==='transition'?'flat':'down'}">${row.symbol}</span>
          <span>CP ${row.cp_prob.toFixed(2)}% · r ${row.run_length.toFixed(1)}${row.ml ? ` · ML ${row.ml.directionLabel} ${row.ml.expectedText}` : ''}</span>
        </div>`).join('')
      : '<span class="muted">Chưa có tín hiệu BOCPD cho watchlist.</span>';
  }

  if(!rows.length){
    wlSignalSignature = "";
    if(wlSignalChart){ wlSignalChart.destroy(); wlSignalChart = null; }
    drawCanvasEmpty(canvas, 'Chua co du lieu BOCPD cho watchlist');
    return;
  }

  if(wlSignalChart && signature === wlSignalSignature){
    return;
  }

  wlSignalSignature = signature;
  if(wlSignalChart){ wlSignalChart.destroy(); wlSignalChart = null; }

  wlSignalChart = new Chart(canvas, {
    data: {
      labels: rows.map(r => r.symbol),
      datasets: [
        {
          type: 'bar',
          label: 'CP prob',
          data: rows.map(r => r.cp_prob),
          backgroundColor: 'rgba(239,68,68,.22)',
          borderColor: '#ef4444',
          borderWidth: 1,
          borderRadius: 6,
          yAxisID: 'y',
        },
        {
          type: 'line',
          label: 'E[r_t]',
          data: rows.map(r => r.run_length),
          borderColor: '#60a5fa',
          backgroundColor: 'rgba(96,165,250,.12)',
          tension: .25,
          pointRadius: 3,
          borderWidth: 2,
          yAxisID: 'y1',
        },
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      onClick: (evt, elements) => {
        if(elements?.length){
          const idx = elements[0].index;
          if(rows[idx]) openDrawer(rows[idx].symbol);
        }
      },
      plugins: {
        legend: { labels: { color: '#7d849b', boxWidth: 10 } },
        tooltip: {
          callbacks: {
            afterBody: items => {
              const idx = items?.[0]?.dataIndex;
              if(idx == null) return '';
              return `Whale score: ${rows[idx].whale_score.toFixed(2)}%`;
            }
          }
        }
      },
      scales: {
        x: {
          ticks: { color: '#7d849b', font: { family: "'JetBrains Mono', monospace" } },
          grid: { display: false },
        },
        y: {
          beginAtZero: true,
          position: 'left',
          ticks: { color: '#4e556b', callback: v => `${Number(v).toFixed(2)}%` },
          grid: { color: 'rgba(150,150,150,.1)' },
        },
        y1: {
          beginAtZero: true,
          position: 'right',
          ticks: { color: '#7d849b' },
          grid: { drawOnChartArea: false },
        }
      }
    }
  });
}

// Chart interval change handler
document.getElementById('wlChartInterval')?.addEventListener('change', ()=>{
  const wlStocks = [...watchlist].filter(sym=>stocks[sym]);
  if(wlStocks.length) loadWatchlistChart(wlStocks);
});

// Helper: detect news source from link
function getNewsSource(link){
  if(!link) return {type:'other', name:'Unknown'};
  const url = link.toLowerCase();
  if(url.includes('yahoo.com') || url.includes('finance.yahoo')) return {type:'yahoo', name:'Yahoo Finance'};
  if(url.includes('news.google') || url.includes('google.com/news')) {
    // Try to extract publisher from Google News URL
    const match = link.match(/url=([^&]+)/);
    if(match) {
      try{
        const realUrl = decodeURIComponent(match[1]);
        const domain = new URL(realUrl).hostname.replace('www.','');
        return {type:'google', name: domain};
      }catch(e){}
    }
    return {type:'google', name:'Google News'};
  }
  // Try to extract domain as source
  try{
    const domain = new URL(link).hostname.replace('www.','');
    return {type:'other', name: domain};
  }catch(e){}
  return {type:'other', name:'Web'};
}

async function loadWatchlistNews(symbols){
  const grid = document.getElementById('wlNewsGrid');
  const filterSelect = document.getElementById('wlNewsFilter');
  if(!grid) return;
  grid.innerHTML = '<p class="muted">Đang tải tin tức…</p>';

  try{
    // Fetch news for each symbol
    const newsPromises = symbols.slice(0,10).map(sym=>
      fetch(`${API}/api/news/${sym}?limit=5`).then(r=>r.json()).catch(()=>({status:'error',data:[]}))
    );
    const results = await Promise.all(newsPromises);
    
    wlAllNews = [];
    results.forEach((res, i) => {
      if(res.status==='ok' && res.data){
        res.data.forEach(n => {
          n._wlSymbol = symbols[i];
          wlAllNews.push(n);
        });
      }
    });

    // Sort by date descending
    wlAllNews.sort((a,b)=>new Date(b.date)-new Date(a.date));
    wlAllNews = wlAllNews.slice(0, 50);

    // Update filter options
    if(filterSelect){
      const currentVal = filterSelect.value;
      filterSelect.innerHTML = '<option value="all">Tất cả mã</option>' +
        symbols.map(sym=>`<option value="${sym}">${sym}</option>`).join('');
      filterSelect.value = currentVal || 'all';
    }

    renderWatchlistNews('all');
  }catch(e){
    grid.innerHTML = '<p class="muted">Lỗi tải tin tức</p>';
  }
}

function renderWatchlistNews(filterSymbol){
  const grid = document.getElementById('wlNewsGrid');
  if(!grid) return;

  let newsToShow = wlAllNews;
  if(filterSymbol && filterSymbol !== 'all'){
    newsToShow = wlAllNews.filter(n => (n.stock_code || n._wlSymbol) === filterSymbol);
  }

  if(!newsToShow.length){
    grid.innerHTML = '<p class="muted">Không có tin tức cho mã này</p>';
    return;
  }

  grid.innerHTML = newsToShow.map(n=>{
    const sc = n.sentiment_score||0;
    const sCls = sc>0?"sent-pos":sc<0?"sent-neg":"sent-neu";
    const sLabel = sc>0?"Tích cực":sc<0?"Tiêu cực":"Trung lập";
    const snippet = (n.content||"").slice(0,200);
    const source = getNewsSource(n.link);
    const srcClass = source.type === 'yahoo' ? 'nc-source-yahoo' : source.type === 'google' ? 'nc-source-google' : 'nc-source-other';
    
    return `<div class="news-card" onclick="openNewsModal(${JSON.stringify(n).replace(/"/g,'&quot;')})">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px">
        <span class="nc-code">${n.stock_code||n._wlSymbol}</span>
        <span class="nc-source ${srcClass}">${source.type === 'yahoo' ? '📰 Yahoo' : source.type === 'google' ? '🔍 Google' : '🌐 Web'}</span>
      </div>
      ${source.name && source.name !== 'Yahoo Finance' && source.name !== 'Google News' ? `<div class="nc-publisher">📌 ${source.name}</div>` : ''}
      <div class="nc-title">${n.title||"Untitled"}</div>
      ${snippet?`<div class="nc-snippet">${snippet}…</div>`:""}
      <div class="nc-meta"><span>${fmtDT(n.date)}</span><span class="${sCls}">${sLabel}</span></div>
    </div>`;
  }).join('');
}

// News filter change handler
document.getElementById('wlNewsFilter')?.addEventListener('change', function(){
  renderWatchlistNews(this.value);
});

/* ═══════════════════════════════════════════════════════════
   NEWS MODAL
   ═══════════════════════════════════════════════════════════ */
function openNewsModal(newsItem){
  const modal = document.getElementById('newsModal');
  const overlay = document.getElementById('newsModalOverlay');
  const nmTitle = document.getElementById('nmTitle');
  const nmStock = document.getElementById('nmStock');
  const nmMeta = document.getElementById('nmMeta');
  const nmContent = document.getElementById('nmContent');
  const nmLink = document.getElementById('nmLink');
  const resetBtn = document.getElementById('nmResetBtn');
  if(!modal||!overlay) return;

  // Reset translation state
  originalNewsContent = '';
  originalNewsTitle = '';
  if (resetBtn) resetBtn.style.display = 'none';

  nmStock.textContent = newsItem.stock_code || '';
  nmTitle.textContent = newsItem.title || 'Untitled';
  
  const sc = newsItem.sentiment_score||0;
  const sCls = sc>0?"sent-pos":sc<0?"sent-neg":"sent-neu";
  const sLabel = sc>0?"Tích cực":sc<0?"Tiêu cực":"Trung lập";
  
  // Add source info to meta
  const source = getNewsSource(newsItem.link);
  const srcLabel = source.type === 'yahoo' ? '📰 Yahoo Finance' : source.type === 'google' ? '🔍 Google News' : '🌐 ' + source.name;
  nmMeta.innerHTML = `<span>${fmtDT(newsItem.date)}</span><span class="${sCls}">${sLabel} (${sc.toFixed(2)})</span><span style="margin-left:auto;font-size:.75rem;color:var(--text-2)">${srcLabel}</span>`;
  
  nmContent.innerHTML = newsItem.content || '<p class="muted">Không có nội dung chi tiết</p>';
  nmLink.href = newsItem.link || '#';
  nmLink.style.display = newsItem.link ? '' : 'none';

  overlay.classList.add('open');
  modal.classList.add('open');
}

function closeNewsModal(){
  document.getElementById('newsModal')?.classList.remove('open');
  document.getElementById('newsModalOverlay')?.classList.remove('open');
}

// News modal close listeners
document.getElementById('nmClose')?.addEventListener('click', closeNewsModal);
document.getElementById('newsModalOverlay')?.addEventListener('click', closeNewsModal);

/* ═══════════════════════════════════════════════════════════
   TRANSLATION
   ═══════════════════════════════════════════════════════════ */
let originalNewsContent = '';
let originalNewsTitle = '';

async function translateText(text, fromLang, toLang) {
  if (!text || fromLang === toLang) return text;

  try{
    const resp = await fetch(`${API}/api/translate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        text,
        from_lang: fromLang,
        to_lang: toLang,
      }),
    });
    const data = await resp.json().catch(()=>({}));
    if(resp.ok && data.status === 'ok'){
      return data.data?.translated_text || text;
    }
    throw new Error(data.detail || 'Translation API error');
  }catch(e){
    console.error('Translation error:', e);
    throw e;
  }
}

// Translate button handler
document.getElementById('nmTranslateBtn')?.addEventListener('click', async function() {
  const btn = this;
  const fromLang = document.getElementById('nmLangFrom')?.value || 'en';
  const toLang = document.getElementById('nmLangTo')?.value || 'vi';
  const nmContent = document.getElementById('nmContent');
  const nmTitle = document.getElementById('nmTitle');
  const resetBtn = document.getElementById('nmResetBtn');
  
  if (!nmContent) return;
  
  // Store original if not already stored
  if (!originalNewsContent) {
    originalNewsContent = nmContent.innerHTML;
    originalNewsTitle = nmTitle?.textContent || '';
  }
  
  // Show loading state
  btn.classList.add('nm-translating');
  btn.disabled = true;
  
  try {
    // Translate content (strip HTML, translate, preserve structure)
    const plainContent = nmContent.innerText;
    const translatedContent = await translateText(plainContent, fromLang, toLang);
    nmContent.innerHTML = `<p>${translatedContent.replace(/\n/g, '</p><p>')}</p>`;
    
    // Translate title
    if (nmTitle && originalNewsTitle) {
      const translatedTitle = await translateText(originalNewsTitle, fromLang, toLang);
      nmTitle.textContent = translatedTitle;
    }
    
    // Show reset button
    if (resetBtn) resetBtn.style.display = '';
  } catch (e) {
    console.error('Translation failed:', e);
    toast('Dich khong thanh cong', 'err');
  } finally {
    btn.classList.remove('nm-translating');
    btn.disabled = false;
  }
});

// Reset button handler
document.getElementById('nmResetBtn')?.addEventListener('click', function() {
  const nmContent = document.getElementById('nmContent');
  const nmTitle = document.getElementById('nmTitle');
  
  if (nmContent && originalNewsContent) {
    nmContent.innerHTML = originalNewsContent;
  }
  if (nmTitle && originalNewsTitle) {
    nmTitle.textContent = originalNewsTitle;
  }
  
  // Hide reset button
  this.style.display = 'none';
});

/* ═══════════════════════════════════════════════════════════
   INIT
   ═══════════════════════════════════════════════════════════ */
loadSymbolRegistry().finally(()=>{
  setPriceChartMode("line", { rerender: false, force: true });
  connect();
  resyncData(false);
  if(cpSummaryTimer) clearInterval(cpSummaryTimer);
  cpSummaryTimer = setInterval(()=>loadChangepointSummary(true), 12000);
});
