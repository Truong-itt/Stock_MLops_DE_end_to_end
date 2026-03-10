/* ╔══════════════════════════════════════════════════════════════╗
   ║  Stock Dashboard — Real-Time Client                         ║
   ╚══════════════════════════════════════════════════════════════╝ */

const API   = window.location.origin;
const WS_URL= `ws://${window.location.host}/ws`;

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

// ─── Stock lists for market filter ─────────────────────────
const VN_STOCKS = new Set([
  'VCB','BID','FPT','HPG','CTG','VHM','TCB','VPB','VNM','MBB',
  'GAS','ACB','MSN','GVR','LPB','SSB','STB','VIB','MWG','HDB',
  'PLX','POW','SAB','BCM','PDR','KDH','NVL','DGC','SHB','EIB',
  'VIC','REE','VJC','GMD','TPB','VRE','VCI','SSI','HCM','VGC',
  'DPM','KBC','DCM','VND','PNJ','HNG','PVD','DHG','NT2','DIG',
]);
const WORLD_STOCKS = new Set([
  'AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','BRK-B','LLY','AVGO',
  'JPM','V','UNH','WMT','MA','XOM','JNJ','PG','HD','COST',
  'NFLX','AMD','INTC','DIS','PYPL','BA','CRM','ORCL','CSCO','ABT',
]);

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
  clock     : $("clock"),
  sTotal    : $("sTotal"),   sUp: $("sUp"),   sDown: $("sDown"),
  sFlat     : $("sFlat"),    sVol: $("sVol"), sTime: $("sTime"),
  body      : $("stockBody"),
  rowCount  : $("rowCount"),
  search    : $("searchInput"),
  sort      : $("sortSelect"),
  tabs      : $("tabs"),
  newsGrid  : $("newsGrid"),
  drawer    : $("drawer"),
  overlay   : $("drawerOverlay"),
  drSymbol  : $("drSymbol"),
  drPrice   : $("drPrice"),
  drChange  : $("drChange"),
  drInfo    : $("drInfo"),
  drInterval: $("drInterval"),
  drChart   : $("drChart"),
  drNews    : $("drNews"),
  drawerClose: $("drawerClose"),
  toasts    : $("toasts"),
  moBody    : $("moBody"),
  moTotalVal: $("moTotalVal"),
};
let moTimer = null;  // matched orders auto-refresh timer
let breadthChart = null;  // market breadth chart instance
let volumeTop10Chart = null; // top 10 volume chart instance
let currentTopTab = 'gainers'; // current top tab

/* ═══════════════════════════════════════════════════════════
   UTILITIES
   ═══════════════════════════════════════════════════════════ */
const fmt  = (n,d=2) => n==null||isNaN(n)?"--":Number(n).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const fmtV = v => {if(v==null)return"--";if(v>=1e9)return(v/1e9).toFixed(2)+"B";if(v>=1e6)return(v/1e6).toFixed(2)+"M";if(v>=1e3)return(v/1e3).toFixed(1)+"K";return v.toString()};
const fmtTime = t => {if(!t)return"--";const d=new Date(t);return isNaN(d)?"--":d.toLocaleTimeString("vi-VN",{hour:"2-digit",minute:"2-digit",second:"2-digit"})};
const fmtDate = t => {if(!t)return"--";const d=new Date(t);return isNaN(d)?t:d.toLocaleDateString("vi-VN")};
const fmtDT   = t => {if(!t)return"--";const d=new Date(t);return isNaN(d)?t:d.toLocaleString("vi-VN")};
const cls = v => v>0?"up":v<0?"down":"flat";

function toast(msg, type="ok"){
  const t=document.createElement("div");
  t.className=`toast ${type}`;t.textContent=msg;
  el.toasts.appendChild(t);
  setTimeout(()=>{t.style.opacity="0";t.style.transform="translateX(100%)";setTimeout(()=>t.remove(),300)},3500);
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
    el.body.innerHTML=`<tr><td colspan="12" class="empty-row">${Object.keys(stocks).length?"Không tìm thấy":'<div class="spinner"></div>Đang tải …'}</td></tr>`;
    return;
  }

  el.body.innerHTML=rows.map(s=>{
    const c=cls(s.pct);
    const isFav = isInWatchlist(s.symbol);
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
      <td class="num ${openFlash}" onclick="openDrawer('${s.symbol}')">${fmt(s.open)}</td>
      <td class="num ${highFlash}" onclick="openDrawer('${s.symbol}')">${fmt(s.high)}</td>
      <td class="num ${lowFlash}" onclick="openDrawer('${s.symbol}')">${fmt(s.low)}</td>
      <td class="num ${volFlash}" onclick="openDrawer('${s.symbol}')">${fmtV(s.volume)}</td>
      <td class="num ${vwapFlash}" onclick="openDrawer('${s.symbol}')">${s.vwap!=null?fmt(s.vwap,2):"--"}</td>
      <td class="date-col" onclick="openDrawer('${s.symbol}')">${fmtDate(s.date)}</td>
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
  loadMatchedOrders(sym);
  loadDrawerNews(sym);
}

function closeDrawer(){
  el.drawer.classList.remove("open");
  el.overlay.classList.remove("open");
  selected=null;
  if(moTimer){clearInterval(moTimer);moTimer=null;}
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
  if(ws&&ws.readyState===1){
    ws.send(JSON.stringify({type:"get_ohlcv",symbol:sym,interval:iv}));
  } else {
    fetch(`${API}/api/stocks/ohlcv/${sym}?interval=${iv}`)
      .then(r=>r.json()).then(j=>{if(j.status==="ok")renderChart(j.data,sym)}).catch(()=>{});
  }
}

function renderChart(data,sym){
  const canvas=el.drChart; if(!canvas)return;
  if(chart){chart.destroy();chart=null}
  if(!data||!data.length){
    const ctx=canvas.getContext("2d");
    ctx.clearRect(0,0,canvas.width,canvas.height);
    ctx.fillStyle="#4e556b";ctx.font="13px Inter";ctx.textAlign="center";
    ctx.fillText("Chưa có dữ liệu OHLCV",canvas.width/2,canvas.height/2);
    return;
  }
  const labels=data.map(d=>new Date(d.ts));
  const closes=data.map(d=>d.close);
  const isUp=closes.length>=2&&closes[closes.length-1]>=closes[0];
  const color=isUp?"#10b981":"#ef4444";
  const bg=isUp?"rgba(16,185,129,.08)":"rgba(239,68,68,.08)";

  chart=new Chart(canvas,{
    type:"line",
    data:{labels,datasets:[{label:`${sym} Close`,data:closes,borderColor:color,backgroundColor:bg,fill:true,tension:.35,pointRadius:0,borderWidth:2}]},
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
document.addEventListener("keydown",e=>{if(e.key==="Escape"){closeDrawer();closeStatPopup();closeNewsModal()}});

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

/* ═══════════════════════════════════════════════════════════
   MARKET OVERVIEW (Thị trường)
   ═══════════════════════════════════════════════════════════ */
let _ovData = null; // cached overview data
let sentimentChart = null; // sentiment chart instance
let _sentData = null; // cached sentiment data

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
      renderBreadthChart(jOv.data.breadth);
      renderTopTable();
      renderVolumeTop10Chart();
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

  const labels = b.labels;  // e.g. ['<-7%','-7~-5%',...,'>7%']
  const values = b.values;
  const total  = b.total;
  const advance= b.advancers;
  const decline= b.decliners;

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

  const {positive, negative, neutral} = data.summary;

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
        legend:{
          position:'right',
          labels:{color:'var(--text-1,#c3c8d8)',font:{size:12},padding:14,usePointStyle:true,pointStyleWidth:10}
        },
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
  if(mode==='positive'){
    filtered = _sentData.stocks.filter(s=>s.positive>0).sort((a,b)=>b.avg_score-a.avg_score);
    title = `Mã có tin tích cực (${filtered.length})`;
  } else if(mode==='negative'){
    filtered = _sentData.stocks.filter(s=>s.negative>0).sort((a,b)=>a.avg_score-b.avg_score);
    title = `Mã có tin tiêu cực (${filtered.length})`;
  } else {
    filtered = _sentData.stocks.filter(s=>s.neutral>0).sort((a,b)=>b.neutral-a.neutral);
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
    const sc = s.avg_score;
    const c = sc>0?'up':sc<0?'down':'flat';
    return `<div class="obs-chip" onclick="showSentimentNews('${s.symbol}')">
      <span class="obs-sym">${s.symbol}</span>
      <span class="obs-pct ${c}">${sc>=0?'+':''}${sc.toFixed(3)}</span>
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

  const top10 = [..._ovData.stocks].sort((a,b)=>(b.volume||0)-(a.volume||0)).slice(0,10).reverse();
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

  let rows, sortField, ascending = false;
  if(currentTopTab==='gainers'){
    rows = [..._ovData.stocks].filter(s=>(s.pct||0)>0).sort((a,b)=>(b.pct||0)-(a.pct||0));
  } else if(currentTopTab==='losers'){
    rows = [..._ovData.stocks].filter(s=>(s.pct||0)<0).sort((a,b)=>(a.pct||0)-(b.pct||0));
  } else {
    rows = [..._ovData.stocks].sort((a,b)=>(b.volume||0)-(a.volume||0));
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
  const wlInfo = document.getElementById('wlInfo');
  if(!content) return;

  const wlStocks = [...watchlist].filter(sym=>stocks[sym]);
  wlInfo.textContent = `${wlStocks.length} mã`;

  if(!wlStocks.length){
    content.innerHTML = '<p class="muted">Chưa có mã nào trong danh sách quan tâm. Hãy bấm ☆ ở cột cuối bảng giá để thêm.</p>';
    if(newsSection) newsSection.style.display='none';
    if(chartSection) chartSection.style.display='none';
    return;
  }

  // Render watchlist stock cards with labels
  content.innerHTML = `<div class="wl-stocks-grid">${wlStocks.map(sym=>{
    const s = stocks[sym];
    const c = cls(s?.pct);
    const vol = s?.vol ? (s.vol >= 1000000 ? (s.vol/1000000).toFixed(1) + 'M' : (s.vol >= 1000 ? (s.vol/1000).toFixed(0) + 'K' : s.vol)) : '--';
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
  if(newsSection) newsSection.style.display='block';
  
  // Load chart and news
  loadWatchlistChart(wlStocks);
  loadWatchlistNews(wlStocks);
}

// Watchlist multi-line chart
async function loadWatchlistChart(symbols){
  const ctx = document.getElementById('wlPriceChart')?.getContext('2d');
  const legendEl = document.getElementById('wlChartLegend');
  if(!ctx) return;

  const interval = document.getElementById('wlChartInterval')?.value || '1h';
  
  // Fetch price data for each symbol
  const datasets = [];
  const labels = new Set();
  
  for(let i=0; i<symbols.length && i<10; i++){
    const sym = symbols[i];
    try{
      const resp = await fetch(`${API}/api/stock/${sym}/prices?interval=${interval}&limit=50`);
      const data = await resp.json();
      if(data.status==='ok' && data.data?.length){
        // Normalize prices to percentage change from first value
        const firstPrice = data.data[0].close || data.data[0].price;
        const normalized = data.data.map(d => ({
          time: new Date(d.ts || d.timestamp).toLocaleTimeString('vi',{hour:'2-digit',minute:'2-digit'}),
          value: firstPrice ? ((d.close || d.price) - firstPrice) / firstPrice * 100 : 0
        }));
        normalized.forEach(d => labels.add(d.time));
        datasets.push({
          label: sym,
          data: normalized.map(d => d.value),
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
    if(legendEl) legendEl.innerHTML = '<span class="muted">Không có dữ liệu biểu đồ</span>';
    return;
  }

  // Destroy old chart
  if(wlPriceChart) wlPriceChart.destroy();

  // Create chart
  wlPriceChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [...labels],
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
        x: { grid: { color: 'rgba(150,150,150,.1)' }, ticks: { color: '#888', maxTicksLimit: 8 } },
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
  nmMeta.innerHTML = `<span>${fmtDT(newsItem.date)}</span><span class="${sCls}">${sLabel} (${sc.toFixed(2)})</span>`;
  
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
  
  // Split text into chunks (API has 500 char limit per request)
  const chunks = [];
  const maxLen = 450;
  let remaining = text;
  while (remaining.length > 0) {
    if (remaining.length <= maxLen) {
      chunks.push(remaining);
      break;
    }
    // Find a good break point
    let breakPoint = remaining.lastIndexOf('. ', maxLen);
    if (breakPoint < 100) breakPoint = remaining.lastIndexOf(' ', maxLen);
    if (breakPoint < 100) breakPoint = maxLen;
    chunks.push(remaining.slice(0, breakPoint + 1));
    remaining = remaining.slice(breakPoint + 1);
  }
  
  // Translate each chunk
  const translated = [];
  for (const chunk of chunks) {
    try {
      const url = `https://api.mymemory.translated.net/get?q=${encodeURIComponent(chunk)}&langpair=${fromLang}|${toLang}`;
      const resp = await fetch(url);
      const data = await resp.json();
      if (data.responseStatus === 200 && data.responseData?.translatedText) {
        translated.push(data.responseData.translatedText);
      } else {
        translated.push(chunk); // fallback to original
      }
    } catch (e) {
      console.error('Translation error:', e);
      translated.push(chunk);
    }
  }
  return translated.join(' ');
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
    alert('Dịch không thành công');
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
connect();
