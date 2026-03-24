/* ╔══════════════════════════════════════════════════════════════╗
   ║  Stock Dashboard — Real-Time Client                         ║
   ╚══════════════════════════════════════════════════════════════╝ */

const API   = window.location.origin;
const WS_PROTO = window.location.protocol === 'https:' ? 'wss' : 'ws';
const WS_URL= `${WS_PROTO}://${window.location.host}/ws`;

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
  drawer    : $("drawer"),
  overlay   : $("drawerOverlay"),
  drSymbol  : $("drSymbol"),
  drPrice   : $("drPrice"),
  drChange  : $("drChange"),
  drInfo    : $("drInfo"),
  drInterval: $("drInterval"),
  drChartTitle: $("drChartTitle"),
  drChart   : $("drChart"),
  drCpInfo  : $("drCpInfo"),
  drCpChart : $("drCpChart"),
  drNews    : $("drNews"),
  drawerClose: $("drawerClose"),
  toasts    : $("toasts"),
  moBody    : $("moBody"),
  moTotalVal: $("moTotalVal"),
  symbolFormStatus: $("symbolFormStatus"),
};
let moTimer = null;  // matched orders auto-refresh timer
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
    const d = new Date(value);
    return isNaN(d) ? null : d;
  }

  if(typeof value === "string"){
    const raw = value.trim();
    if(!raw) return null;
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
      if(!time || close == null || !Number.isFinite(Number(close))) return null;
      return {
        x: time,
        y: Number(close),
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.x - b.x);
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
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0,0,canvas.width,canvas.height);
  ctx.fillStyle = "#4e556b";
  ctx.font = "13px Inter";
  ctx.textAlign = "center";
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
  const wlStocks = [...watchlist].filter(sym=>stocks[sym]);
  if(wlStocks.length) renderWatchlistSignalChart(wlStocks);
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
      el.syncBtn.textContent = "Dong bo lai";
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

  if(selected && changed.has(selected)) updateDrawerPrice();
  if(document.querySelector(".tab.active")?.dataset.tab === "watchlist"){
    const hasWatchlistChanges = [...watchlist].some(sym => changed.has(sym));
    if(hasWatchlistChanges) loadWatchlist();
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
  updateDrawerPrice();
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
  el.overlay.classList.remove("open");
  selected=null;
  if(moTimer){clearInterval(moTimer);moTimer=null;}
  if(cpTimer){clearInterval(cpTimer);cpTimer=null;}
  if(cpChart){cpChart.destroy();cpChart=null;}
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

/* ── Chart ───────────────────────────────────────────────── */
function loadOHLCV(sym){
  const iv=el.drInterval.value;
  if(el.drChartTitle){
    el.drChartTitle.textContent = `Biểu đồ giá`;
  }
  fetch(`${API}/api/stocks/ohlcv/${encodeURIComponent(sym)}?interval=${encodeURIComponent(iv)}`)
    .then(r=>r.json())
    .then(j=>{
      if(j.status!=="ok") return;
      const meta = j.meta || {};
      if(el.drChartTitle){
        if(meta.fallback_used && meta.resolved_interval){
          el.drChartTitle.textContent = `Biểu đồ giá (${meta.resolved_interval} fallback từ ${meta.requested_interval})`;
        }else if(meta.resolved_interval){
          el.drChartTitle.textContent = `Biểu đồ giá (${meta.resolved_interval})`;
        }else{
          el.drChartTitle.textContent = `Biểu đồ giá`;
        }
      }
      renderChart(j.data,sym);
    })
    .catch(err=>{
      console.error("loadOHLCV error:", err);
      if(el.drChartTitle){
        el.drChartTitle.textContent = "Biểu đồ giá";
      }
      renderChart([], sym);
    });
}

function renderChart(data,sym){
  const canvas=el.drChart; if(!canvas)return;
  if(chart){chart.destroy();chart=null}
  const series = normalizePriceSeries(data);
  if(!series.length){
    drawCanvasEmpty(canvas, "Chua co du lieu OHLCV");
    return;
  }
  const closes=series.map(point=>point.y);
  const isUp=closes.length>=2&&closes[closes.length-1]>=closes[0];
  const color=isUp?"#10b981":"#ef4444";
  const bg=isUp?"rgba(16,185,129,.08)":"rgba(239,68,68,.08)";

  chart=new Chart(canvas,{
    type:"line",
    data:{datasets:[{label:`${sym} Close`,data:series,borderColor:color,backgroundColor:bg,fill:true,tension:.35,pointRadius:0,borderWidth:2}]},
    options:{
      responsive:true,maintainAspectRatio:false,
      interaction:{mode:"index",intersect:false},
      plugins:{legend:{display:false},tooltip:{backgroundColor:"#161b26",titleColor:"#f0f2f8",bodyColor:"#7d849b",borderColor:"#1f2739",borderWidth:1}},
      scales:{
        x:{type:"time",ticks:{color:"#4e556b",maxTicksLimit:7},grid:{color:"rgba(31,39,57,.5)"}},
        y:{ticks:{color:"#4e556b"},grid:{color:"rgba(31,39,57,.5)"}},
      },
    },
  });
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
      renderChangepointChart(historyJson.data || [], sym);
    } else {
      renderChangepointChart([], sym);
    }
  }catch(err){
    console.error("loadChangepoint error:", err);
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
function loadMatchedOrders(sym){
  el.moBody.innerHTML='<tr><td colspan="4" class="muted" style="text-align:center;padding:20px">Đang tải …</td></tr>';
  el.moTotalVal.textContent='--';
  // Clear previous timer
  if(moTimer){clearInterval(moTimer);moTimer=null;}

  const doLoad = () => {
    // Always use REST API for reliability (avoids WS race with price_update broadcasts)
    fetch(`${API}/api/stocks/matched-orders/${sym}?limit=50`)
      .then(r=>r.json())
      .then(j=>{
        if(j.status==="ok") renderMatchedOrders(j.data, j.total_count);
        else console.warn("matched-orders bad response:", j);
      })
      .catch(err=>console.error("matched-orders fetch error:", err));
  };
  doLoad();
  // Auto-refresh every 3s while drawer is open
  moTimer=setInterval(()=>{if(selected===sym)doLoad();else{clearInterval(moTimer);moTimer=null;}},3000);
}

function renderMatchedOrders(data, totalCount){
  if(!data||!data.length){
    el.moBody.innerHTML='<tr><td colspan="4" class="muted" style="text-align:center;padding:20px">Chưa có dữ liệu</td></tr>';
    el.moTotalVal.textContent='0';
    return;
  }
  el.moBody.innerHTML=data.map(d=>{
    const ts = d.timestamp ? new Date(Number(d.timestamp)) : null;
    const timeStr = ts && !isNaN(ts) ? ts.toLocaleTimeString("vi-VN",{hour:"2-digit",minute:"2-digit",second:"2-digit"}) : "--";
    const price = d.price;
    const lastSize = d.last_size;
    const change = d.change;
    const pCls = change > 0 ? "mo-price-up" : change < 0 ? "mo-price-down" : "mo-price-flat";
    const cCls = change > 0 ? "mo-change-up" : change < 0 ? "mo-change-down" : "mo-change-flat";
    const borderCls = change > 0 ? "mo-row-border-up" : change < 0 ? "mo-row-border-down" : "";
    return `<tr class="${borderCls}">
      <td>${timeStr}</td>
      <td class="${pCls}">${price!=null?fmt(price,2):"--"}</td>
      <td class="mo-size">${lastSize!=null?Number(lastSize).toLocaleString():"0"}</td>
      <td class="${cCls}">${change!=null?fmt(change,2):"--"}</td>
    </tr>`;
  }).join("");
  el.moTotalVal.textContent = totalCount!=null ? Number(totalCount).toLocaleString() : data.length.toLocaleString();
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
el.drawerClose.addEventListener("click",closeDrawer);
el.overlay.addEventListener("click",closeDrawer);
el.drInterval.addEventListener("change",()=>{if(selected)loadOHLCV(selected)});
document.getElementById('drFavBtn')?.addEventListener('click',()=>{if(selected)toggleWatchlist(selected)});
document.addEventListener("keydown",e=>{if(e.key==="Escape"){closeDrawer();closeStatPopup();closeNewsModal();closeSymbolModal()}});

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
          const isDark=!document.documentElement.getAttribute('data-theme')||document.documentElement.getAttribute('data-theme')==='dark';
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
    return `<div class="wl-stock-card" onclick="openDrawer('${sym}')">
      <div class="wl-card-top">
        <div>
          <div class="wl-card-sym">${sym}</div>
          <div class="wl-card-name">${getCompanyName(sym)}</div>
        </div>
        <button class="wl-remove-btn" onclick="event.stopPropagation();toggleWatchlist('${sym}');loadWatchlist()" title="Bỏ quan tâm">✕</button>
      </div>
      <div class="wl-card-row">
        <div class="wl-card-label">Giá hiện tại</div>
        <div class="wl-card-price ${c}">${s?fmt(s.price):'--'}</div>
      </div>
      <div class="wl-card-row">
        <div class="wl-card-label">Thay đổi</div>
        <div class="wl-card-change ${c}">${s?((s.pct>=0?'+':'')+fmt(s.pct)+'%'):'--'}</div>
      </div>
      <div class="wl-card-signal">
        <span class="cp-pill ${cpClass}">${cpLabel}</span>
        <span class="wl-card-mono">${cp ? `CP ${fmtProb(cp.cp_prob)} · r ${fmt(cp.expected_run_length,1)}` : 'BOCPD --'}</span>
      </div>
      <div class="wl-card-mono wl-card-ml ${ml ? ml.klass : ''}">
        ${ml ? `ML ${ml.directionLabel} ${ml.expectedText} · up ${fmtProb(ml.probUp)} / down ${fmtProb(ml.probDown)}` : 'ML --'}
      </div>
      <div class="wl-card-stats">
        <div class="wl-stat-item">
          <div class="wl-stat-value">${vol}</div>
          <div class="wl-stat-label">KLGD</div>
        </div>
        <div class="wl-stat-item">
          <div class="wl-stat-value ${c}">${s?(s.change>=0?'+':'')+fmt(s.change):'--'}</div>
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
  if(wlSignalChart){ wlSignalChart.destroy(); wlSignalChart = null; }

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

  if(legendEl){
    legendEl.innerHTML = rows.length
      ? rows.map(row => `<div class="wl-legend-item">
          <span class="cp-pill ${row.regime_label==='whale-watch'?'up':row.regime_label==='transition'?'flat':'down'}">${row.symbol}</span>
          <span>CP ${row.cp_prob.toFixed(2)}% · r ${row.run_length.toFixed(1)}${row.ml ? ` · ML ${row.ml.directionLabel} ${row.ml.expectedText}` : ''}</span>
        </div>`).join('')
      : '<span class="muted">Chưa có tín hiệu BOCPD cho watchlist.</span>';
  }

  if(!rows.length){
    drawCanvasEmpty(canvas, 'Chua co du lieu BOCPD cho watchlist');
    return;
  }

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
  connect();
  resyncData(false);
  if(cpSummaryTimer) clearInterval(cpSummaryTimer);
  cpSummaryTimer = setInterval(()=>loadChangepointSummary(true), 12000);
});
