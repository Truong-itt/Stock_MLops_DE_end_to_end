/* ╔══════════════════════════════════════════════════════════════╗
   ║  Stock Dashboard — Real-Time Client                         ║
   ╚══════════════════════════════════════════════════════════════╝ */

const API   = window.location.origin;
const WS_URL= `ws://${window.location.host}/ws`;

// ─── State ──────────────────────────────────────────────────
let stocks      = {};          // keyed by symbol
let prevPrices  = {};          // for flash detection
let ws          = null;
let retryTimer  = null;
let pingTimer   = null;        // ping interval id
let selected    = null;        // currently viewed symbol
let chart       = null;
let sortMode    = "symbol";
let dataSource  = "daily";     // "latest" | "daily" | "merged"

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
};

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
      open:   null, high: null, low: null,
      volume: row.day_volume,
      vwap:   null,
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
    case "heartbeat": case "pong": break;
  }
}

function ingest(rows, full){
  if(!rows)return;
  if(full){ prevPrices={}; stocks={}; }
  const changed=new Set();
  rows.forEach(r=>{
    const n = norm(r);
    const sym = n.symbol;
    if(!sym) return;
    // Skip rows where price is still null (no data at all)
    if(n.price==null && stocks[sym]) return;
    if(stocks[sym]) prevPrices[sym] = stocks[sym].price;
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
  const arr=Object.values(stocks);
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
    el.body.innerHTML=`<tr><td colspan="10" class="empty-row">${Object.keys(stocks).length?"Không tìm thấy":'<div class="spinner"></div>Đang tải …'}</td></tr>`;
    return;
  }

  el.body.innerHTML=rows.map(s=>{
    const c=cls(s.pct);
    let flash="";
    if(changed&&changed.has(s.symbol)){
      const pp=prevPrices[s.symbol];
      if(pp!=null) flash=s.price>pp?"flash-up":s.price<pp?"flash-down":"";
    }
    return `<tr class="${flash}" onclick="openDrawer('${s.symbol}')">
      <td class="sticky-col"><span class="sym">${s.symbol}</span></td>
      <td class="num ${c}">${fmt(s.price)}</td>
      <td class="num ${c}">${(s.change>=0?"+":"")+fmt(s.change)}</td>
      <td class="num ${c}">${(s.pct>=0?"+":"")+fmt(s.pct)}%</td>
      <td class="num">${fmt(s.open)}</td>
      <td class="num">${fmt(s.high)}</td>
      <td class="num">${fmt(s.low)}</td>
      <td class="num">${fmtV(s.volume)}</td>
      <td class="num">${s.vwap!=null?fmt(s.vwap,2):"--"}</td>
      <td class="date-col">${fmtDate(s.date)}</td>
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
  loadDrawerNews(sym);
}

function closeDrawer(){
  el.drawer.classList.remove("open");
  el.overlay.classList.remove("open");
  selected=null;
}

function updateDrawerPrice(){
  const s=stocks[selected];
  if(!s)return;
  const c=cls(s.pct);
  el.drSymbol.textContent=s.symbol;
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
    return `<div class="dn-item">
      <div class="dn-title"><a href="${n.link||"#"}" target="_blank">${n.title||"Untitled"}</a></div>
      <div class="dn-meta"><span>${fmtDT(n.date)}</span><span class="${sCls}">${sLabel} (${sc.toFixed(2)})</span></div>
    </div>`;
  }).join("");
}

function renderDrawerDaily(data, sym){ /* optional future use */ }

/* ═══════════════════════════════════════════════════════════
   NEWS TAB
   ═══════════════════════════════════════════════════════════ */
async function loadAllNews(){
  el.newsGrid.innerHTML='<p class="muted">Đang tải tin tức …</p>';
  try{
    const r=await fetch(`${API}/api/news?limit=60`);
    const j=await r.json();
    if(j.status==="ok"&&j.data.length){
      el.newsGrid.innerHTML=j.data.map(n=>{
        const sc=n.sentiment_score||0;
        const sCls=sc>0?"sent-pos":sc<0?"sent-neg":"sent-neu";
        const sLabel=sc>0?"Tích cực":sc<0?"Tiêu cực":"Trung lập";
        const snippet=(n.content||"").slice(0,180);
        return `<div class="news-card">
          <span class="nc-code">${n.stock_code}</span>
          <div class="nc-title"><a href="${n.link||"#"}" target="_blank">${n.title||"Untitled"}</a></div>
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
  if(tab==="news") loadAllNews();
});

/* ═══════════════════════════════════════════════════════════
   EVENT LISTENERS
   ═══════════════════════════════════════════════════════════ */
el.search.addEventListener("input",()=>renderTable());
el.sort.addEventListener("change",e=>{sortMode=e.target.value;renderTable()});
el.drawerClose.addEventListener("click",closeDrawer);
el.overlay.addEventListener("click",closeDrawer);
el.drInterval.addEventListener("change",()=>{if(selected)loadOHLCV(selected)});
document.addEventListener("keydown",e=>{if(e.key==="Escape")closeDrawer()});

// ⌘K / Ctrl+K → focus search
document.addEventListener("keydown",e=>{
  if((e.metaKey||e.ctrlKey)&&e.key==="k"){e.preventDefault();el.search.focus();el.search.select()}
});

/* ═══════════════════════════════════════════════════════════
   INIT
   ═══════════════════════════════════════════════════════════ */
connect();
