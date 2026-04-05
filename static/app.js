const teamSelect = document.getElementById('teamSelect');
const oppSelect = document.getElementById('oppSelect');
const playerSearchInput = document.getElementById('playerSearch');
const searchResults = document.getElementById('searchResults');
const selectedPlayerBadge = document.getElementById('selectedPlayer');
let selectedPlayerMatchupTile = document.getElementById('selectedPlayerMatchupTile');
const playerGrid = document.getElementById('playerGrid');
const rosterTitle = document.getElementById('rosterTitle');
const rosterMeta = document.getElementById('rosterMeta');
const recentPlayersSection = document.getElementById('recentPlayersSection');
const recentPlayersContainer = document.getElementById('recentPlayers');
const clearRecentBtn = document.getElementById('clearRecentBtn');
const analyzeBtn = document.getElementById('analyzeBtn');
const betFinderBtn = document.getElementById('betFinderBtn');
const lineInput = document.getElementById('lineInput');
const gamesSelect = document.getElementById('gamesSelect');
const seasonInput = document.getElementById('seasonInput');
const chartTitle = document.getElementById('chartTitle');
const chartSubtitle = document.getElementById('chartSubtitle');
const chartChips = document.getElementById('chartChips');
const statusPill = document.getElementById('statusPill');
const actionBanner = document.getElementById('actionBanner');
const actionBannerTitle = document.getElementById('actionBannerTitle');
const actionBannerDetail = document.getElementById('actionBannerDetail');
const actionBannerState = document.getElementById('actionBannerState');
const avgValue = document.getElementById('avgValue');
const hitRateValue = document.getElementById('hitRateValue');
const hitCountValue = document.getElementById('hitCountValue');
const seasonValue = document.getElementById('seasonValue');
const streakValue = document.getElementById('streakValue');
const lastGameValue = document.getElementById('lastGameValue');
const gamesTableBody = document.getElementById('gamesTableBody');
const gameLogMeta = document.getElementById('gameLogMeta');
const recentLogTab = document.getElementById('recentLogTab');
const h2hLogTab = document.getElementById('h2hLogTab');
const propButtonsWrap = document.getElementById('propButtons');
const themeToggle = document.getElementById('themeToggle');
const matchupPanel = document.getElementById('matchupPanel');
const betFinderMeta = document.getElementById('betFinderMeta');
const betFinderResults = document.getElementById('betFinderResults');
const matchupLeanBadge = document.getElementById('matchupLeanBadge');
const matchupBody = document.getElementById('matchupBody');
const analyzerMatchupLeanBadge = document.getElementById('analyzerMatchupLeanBadge');
const analyzerMatchupBody = document.getElementById('analyzerMatchupBody');
const marketTextarea = document.getElementById('marketTextarea');
const marketTemplateBtn = document.getElementById('marketTemplateBtn');
const marketClearBtn = document.getElementById('marketClearBtn');
const marketScanBtn = document.getElementById('marketScanBtn');
const marketResults = document.getElementById('marketResults');
const marketMeta = document.getElementById('marketMeta');
const marketSortSelect = document.getElementById('marketSortSelect');
const marketFilterChips = document.getElementById('marketFilterChips');
const marketExpertFilterChips = document.getElementById('marketExpertFilterChips');
const oddsApiKeyInput = document.getElementById('oddsApiKeyInput');
const oddsSportSelect = document.getElementById('oddsSportSelect');
const oddsRegionsInput = document.getElementById('oddsRegionsInput');
const oddsFormatSelect = document.getElementById('oddsFormatSelect');
const oddsMarketsInput = document.getElementById('oddsMarketsInput');
const oddsEventSelect = document.getElementById('oddsEventSelect');
const oddsLoadEventsBtn = document.getElementById('oddsLoadEventsBtn');
const oddsImportScanBtn = document.getElementById('oddsImportScanBtn');
const oddsQuotaMeta = document.getElementById('oddsQuotaMeta');
const oddsApiKeyMeta = document.getElementById('oddsApiKeyMeta');
const oddsApiStatus = document.getElementById('oddsApiStatus');
const workspaceTitle = document.getElementById('workspaceTitle');
const workspaceSubtitle = document.getElementById('workspaceSubtitle');
const workspaceEyebrow = document.getElementById('workspaceEyebrow');
const navItems = document.querySelectorAll('.nav-item');
const dashboardViews = document.querySelectorAll('.dashboard-view');
const quickViewButtons = document.querySelectorAll('[data-go-view]');
const betFinderViewRunBtn = document.getElementById('betFinderViewRunBtn');
const overviewCurrentCard = document.getElementById('overviewCurrentCard');
const sidebarMiniPlayer = document.getElementById('sidebarMiniPlayer');
const sidebarToggle = document.getElementById('sidebarToggle');
const mobileSidebarToggle = document.getElementById('mobileSidebarToggle');
const todayGamesBoard = document.getElementById('todayGamesBoard');
const todayGamesMeta = document.getElementById('todayGamesMeta');
const overviewTodayGames = document.getElementById('overviewTodayGames');
const overviewTodayMeta = document.getElementById('overviewTodayMeta');
const overviewBestBets = document.getElementById('overviewBestBets');
const overviewBestBetsMeta = document.getElementById('overviewBestBetsMeta');
const interpretationTone = document.getElementById('interpretationTone');
const interpretationBody = document.getElementById('interpretationBody');
const opportunityTone = document.getElementById('opportunityTone');
const opportunityBody = document.getElementById('opportunityBody');
const environmentTone = document.getElementById('environmentTone');
const environmentBody = document.getElementById('environmentBody');

const RECENT_PLAYERS_KEY = 'nba-props-recent-players';
const THEME_KEY = 'nba-props-theme';
const SIDEBAR_COLLAPSED_KEY = 'nba-props-sidebar-collapsed';
const MARKET_RESULTS_KEY = 'nba-props-latest-market-results';
const MARKET_EXPERT_FILTERS_KEY = 'nba-props-market-expert-filters';
const ODDS_API_KEY_STORAGE = 'nba-props-odds-api-key';
const ODDS_API_SETTINGS_STORAGE = 'nba-props-odds-api-settings';
const LAST_PLAYER_KEY = 'nba-props-last-player';
const LAST_STAT_KEY   = 'nba-props-last-stat';
const FALLBACK_HEADSHOT = encodeURIComponent(`
  <svg xmlns="http://www.w3.org/2000/svg" width="240" height="240" viewBox="0 0 240 240">
    <defs>
      <linearGradient id="g" x1="0" x2="1" y1="0" y2="1">
        <stop offset="0%" stop-color="#243452" />
        <stop offset="100%" stop-color="#121b31" />
      </linearGradient>
    </defs>
    <rect width="240" height="240" rx="42" fill="url(#g)" />
    <circle cx="120" cy="88" r="40" fill="#5f9bff" fill-opacity="0.92" />
    <path d="M50 202c12-31 40-51 70-51s58 20 70 51" fill="#5f9bff" fill-opacity="0.92" />
  </svg>
`);

const TEAM_COLORS = {
  ATL: { accent: '#e03a3e', accent2: '#fdb927', rgb: '224, 58, 62' },
  BOS: { accent: '#007a33', accent2: '#ba9653', rgb: '0, 122, 51' },
  BKN: { accent: '#111111', accent2: '#7c8b99', rgb: '85, 98, 112' },
  CHA: { accent: '#1d1160', accent2: '#00788c', rgb: '29, 17, 96' },
  CHI: { accent: '#ce1141', accent2: '#000000', rgb: '206, 17, 65' },
  CLE: { accent: '#860038', accent2: '#fdbb30', rgb: '134, 0, 56' },
  DAL: { accent: '#00538c', accent2: '#b8c4ca', rgb: '0, 83, 140' },
  DEN: { accent: '#0e2240', accent2: '#fec524', rgb: '14, 34, 64' },
  DET: { accent: '#c8102e', accent2: '#1d42ba', rgb: '200, 16, 46' },
  GSW: { accent: '#1d428a', accent2: '#ffc72c', rgb: '29, 66, 138' },
  HOU: { accent: '#ce1141', accent2: '#000000', rgb: '206, 17, 65' },
  IND: { accent: '#002d62', accent2: '#fdbb30', rgb: '0, 45, 98' },
  LAC: { accent: '#c8102e', accent2: '#1d428a', rgb: '200, 16, 46' },
  LAL: { accent: '#552583', accent2: '#fdb927', rgb: '85, 37, 131' },
  MEM: { accent: '#5d76a9', accent2: '#12173f', rgb: '93, 118, 169' },
  MIA: { accent: '#98002e', accent2: '#f9a01b', rgb: '152, 0, 46' },
  MIL: { accent: '#00471b', accent2: '#eee1c6', rgb: '0, 71, 27' },
  MIN: { accent: '#0c2340', accent2: '#78be20', rgb: '12, 35, 64' },
  NOP: { accent: '#0c2340', accent2: '#c8102e', rgb: '12, 35, 64' },
  NYK: { accent: '#006bb6', accent2: '#f58426', rgb: '0, 107, 182' },
  OKC: { accent: '#007ac1', accent2: '#ef3b24', rgb: '0, 122, 193' },
  ORL: { accent: '#0077c0', accent2: '#c4ced4', rgb: '0, 119, 192' },
  PHI: { accent: '#006bb6', accent2: '#ed174c', rgb: '0, 107, 182' },
  PHX: { accent: '#1d1160', accent2: '#e56020', rgb: '29, 17, 96' },
  POR: { accent: '#e03a3e', accent2: '#000000', rgb: '224, 58, 62' },
  SAC: { accent: '#5a2d81', accent2: '#63727a', rgb: '90, 45, 129' },
  SAS: { accent: '#8a8d8f', accent2: '#000000', rgb: '138, 141, 143' },
  TOR: { accent: '#ce1141', accent2: '#000000', rgb: '206, 17, 65' },
  UTA: { accent: '#002b5c', accent2: '#f9a01b', rgb: '0, 43, 92' },
  WAS: { accent: '#002b5c', accent2: '#e31837', rgb: '0, 43, 92' }
};

let selectedPlayer = null;
let selectedStat = 'PTS';
let chart = null;
let searchTimeout = null;
let rosterPlayers = [];
let selectedTeam = null;
let lastPayload = null;
let activeView = 'overview';
let statusBannerTimer = null;
let activeWorkCount = 0;
let latestTodayGamesPayload = null;
let currentGameLogPayload = null;
let activeGameLogView = 'recent';
let currentMarketResultsPayload = null;
let currentMarketSort = localStorage.getItem('nba-props-market-sort') || 'best_ev';
let currentMarketSortDirection = localStorage.getItem('nba-props-market-sort-direction') || 'desc';
let currentMarketFilter = localStorage.getItem('nba-props-market-filter') || 'all';
let currentExpertFilters = loadStoredExpertFilters();
const currentExpertSettings = { min_minutes: 22, min_fga: 10, min_h2h_games: 2, min_h2h_hit_rate: 60 };

const VIEW_META = {
  overview: {
    eyebrow: 'Dashboard workspace',
    title: 'Overview',
    subtitle: 'A structured view of the selected player, current matchup, and key prop indicators.'
  },
  today: {
    eyebrow: 'Daily slate',
    title: `Today's Games`,
    subtitle: 'Review the current slate, game status, and team-level injury report context in one place.'
  },
  analyzer: {
    eyebrow: 'Player lab',
    title: 'Player Analyzer',
    subtitle: 'Control the inputs, browse rosters, and inspect trend charts plus recent game logs.'
  },
  betfinder: {
    eyebrow: 'Team rankings',
    title: 'Bet Finder',
    subtitle: 'Use your current team, prop, and line to rank the strongest recent overs on that roster.'
  },
  market: {
    eyebrow: 'Board analysis',
    title: 'Market Scanner',
    subtitle: 'Paste a prop board, compare value, and jump straight into the best candidate.'
  },
  parlay: {
    eyebrow: 'Parlay builder',
    title: 'Parlay Builder',
    subtitle: 'Scrape every game, rank props by hit rate, and auto-build the strongest 2–6 leg parlay.'
  },
  tracker: {
    eyebrow: 'Live tracking',
    title: 'Prop Tracker',
    subtitle: 'Add your active bets and watch each bar fill toward the line in real time.'
  }
};

function switchView(view, options = {}) {
  if (!VIEW_META[view]) return;

  const previousView = activeView;
  const shouldScroll = options.scroll ?? (view !== previousView);
  activeView = view;

  dashboardViews.forEach(section => {
    section.classList.toggle('active', section.dataset.view === view);
  });

  if (window.innerWidth < 1280) {
    document.body.classList.remove('sidebar-mobile-open');
  }

  navItems.forEach(item => {
    item.classList.toggle('active', item.dataset.view === view);
  });

  const meta = VIEW_META[view];
  if (workspaceEyebrow) workspaceEyebrow.textContent = meta.eyebrow;
  if (workspaceTitle) workspaceTitle.textContent = meta.title;
  if (workspaceSubtitle) workspaceSubtitle.textContent = meta.subtitle;

  document.body.dataset.activeView = view;
  if (view === 'today') {
    loadTodayGames();
  }
  if (view === 'analyzer') {
    ensureTeamsLoaded();
  }
  if (shouldScroll) {
    window.scrollTo({ top: 0, behavior: options.instant ? 'auto' : 'smooth' });
  }
}

function setSidebarCollapsed(collapsed) {
  document.body.classList.toggle('sidebar-collapsed', collapsed);
  localStorage.setItem(SIDEBAR_COLLAPSED_KEY, collapsed ? '1' : '0');
  if (sidebarToggle) {
    sidebarToggle.querySelector('.sidebar-toggle-icon').textContent = collapsed ? '⟩' : '⟨';
    sidebarToggle.setAttribute('aria-label', collapsed ? 'Expand sidebar' : 'Collapse sidebar');
  }
}

function applySavedSidebarState() {
  const collapsed = localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === '1';
  setSidebarCollapsed(collapsed);
}

function loadStoredMarketResults() {
  try {
    return JSON.parse(localStorage.getItem(MARKET_RESULTS_KEY) || 'null');
  } catch {
    return null;
  }
}

function saveLatestMarketResults(payload) {
  const snapshot = {
    updated_at: new Date().toISOString(),
    results: (payload.results || []).slice(0, 8)
  };
  localStorage.setItem(MARKET_RESULTS_KEY, JSON.stringify(snapshot));
}

/**
 * Convert injury-report name format "Last, First" -> "First Last" for display.
 * If name has no comma it is returned as-is.
 */
function formatPlayerName(name) {
  if (!name) return '';
  const comma = name.indexOf(',');
  if (comma === -1) return name;
  const last = name.slice(0, comma).trim();
  const first = name.slice(comma + 1).trim();
  return first ? `${first} ${last}` : last;
}

function formatStoredTime(value) {
  if (!value) return 'Latest saved board unavailable.';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'Latest saved board available.';
  return `Updated ${date.toLocaleString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })}`;
}

function loadStoredExpertFilters() {
  try {
    const raw = JSON.parse(localStorage.getItem(MARKET_EXPERT_FILTERS_KEY) || '{}');
    return {
      favorable_matchup: Boolean(raw.favorable_matchup || raw.bottom10_pos),
      neutral_matchup: Boolean(raw.neutral_matchup),
      tough_matchup: Boolean(raw.tough_matchup),
      stable_minutes: Boolean(raw.stable_minutes),
      stable_fga: Boolean(raw.stable_fga),
      h2h_over: Boolean(raw.h2h_over),
      rest_edge: Boolean(raw.rest_edge),
      avoid_b2b: Boolean(raw.avoid_b2b),
      fast_pace: Boolean(raw.fast_pace),
      avoid_slow_pace: Boolean(raw.avoid_slow_pace),
    };
  } catch {
    return {
      favorable_matchup: false,
      neutral_matchup: false,
      tough_matchup: false,
      stable_minutes: false,
      stable_fga: false,
      h2h_over: false,
      rest_edge: false,
      avoid_b2b: false,
      fast_pace: false,
      avoid_slow_pace: false,
    };
  }
}

function saveExpertFilters() {
  localStorage.setItem(MARKET_EXPERT_FILTERS_KEY, JSON.stringify(currentExpertFilters));
}



function getExpertFilterDefinitions() {
  return [
    ['favorable_matchup', 'Favorable Matchup'],
    ['neutral_matchup', 'Neutral Matchup'],
    ['tough_matchup', 'Tough Matchup'],
    ['stable_minutes', 'Stable Minutes'],
    ['stable_fga', 'Stable FGA'],
    ['h2h_over', 'H2H Success'],
    ['rest_edge', 'Rest Edge'],
    ['avoid_b2b', 'No Back-to-Back'],
    ['fast_pace', 'Fast Pace'],
    ['avoid_slow_pace', 'Avoid Slow Pace'],
  ];
}

function getReadablePositionLabel(matchup = {}) {
  const code = String(matchup?.position_code || '').trim().toUpperCase();
  const label = String(matchup?.position_label || '').trim();
  if (code === 'G') return 'Guard';
  if (code === 'F') return 'Forward';
  if (code === 'C') return 'Center';
  if (label) {
    if (/guards?/i.test(label)) return 'Guard';
    if (/forwards?/i.test(label)) return 'Forward';
    if (/centers?/i.test(label)) return 'Center';
    return label.replace(/s$/i, '');
  }
  return 'Position';
}

function hasActiveExpertFilters() {
  return Object.values(currentExpertFilters || {}).some(Boolean);
}

function getExpertFilterSummary() {
  const active = getExpertFilterDefinitions()
    .filter(([key]) => currentExpertFilters?.[key])
    .map(([, label]) => label);
  return active.length ? `Expert: ${active.join(' • ')}` : 'Expert: All';
}


function parseEstimatedSpreadValue(spreadLabel = '') {
  const value = String(spreadLabel || '').trim();
  if (!value) return null;
  const match = value.match(/est\.\s*([+-]?\d+(?:\.\d+)?)/i);
  return match ? Number(match[1]) : null;
}

function getMarketExpertSnapshot(item) {
  const matchup = item?.analysis?.matchup?.vs_position || item?.matchup?.vs_position || {};
  const opportunity = item?.analysis?.opportunity || {};
  const h2h = item?.analysis?.h2h || {};
  const environment = item?.analysis?.environment || {};
  const defRank = Number(matchup?.def_rank || 0);
  const deltaPct = Number(matchup?.delta_pct || 0);
  const leanTone = String(matchup?.lean_tone || '').toLowerCase();
  const lean = String(matchup?.lean || '').toLowerCase();
  let matchupState = 'neutral';

  // Use backend matchup judgment first so the Expert Angle badge matches the Matchup column.
  if (leanTone === 'good' || lean.includes('favorable')) matchupState = 'favorable';
  else if (leanTone === 'bad' || lean.includes('tough')) matchupState = 'tough';
  else if (Number.isFinite(deltaPct)) {
    if (deltaPct >= 5) matchupState = 'favorable';
    else if (deltaPct <= -5) matchupState = 'tough';
  }

  // Fallback only when no lean/delta signal exists.
  if (matchupState === 'neutral' && !leanTone && defRank > 0) {
    if (defRank <= 10) matchupState = 'favorable';
    else if (defRank >= 21) matchupState = 'tough';
  }

  return {
    matchup,
    opportunity,
    h2h,
    environment,
    defRank,
    deltaPct,
    leanTone,
    matchupState,
    minutesLast5: Number(opportunity?.minutes_last5 || 0),
    minutesTrend: String(opportunity?.minutes_trend || '').toLowerCase(),
    fgaLast5: Number(opportunity?.fga_last5 || 0),
    fgaTrend: String(opportunity?.volume_trend || '').toLowerCase(),
    h2hGames: Number(h2h?.games_count || 0),
    h2hHits: Number(h2h?.hit_count || 0),
    h2hHitRate: Number(h2h?.hit_rate || 0),
    restDays: Number(environment?.rest_days || 0),
    isBackToBack: Boolean(environment?.is_back_to_back),
    paceBucket: String(environment?.pace_bucket || '').toLowerCase(),
    paceLabel: String(environment?.pace_label || '').trim(),
    spreadBucket: String(environment?.spread_bucket || '').toLowerCase(),
    spreadLabel: String(environment?.spread_label || '').trim(),
    projectedSpread: Number.isFinite(Number(environment?.projected_spread)) ? Number(environment.projected_spread) : parseEstimatedSpreadValue(environment?.spread_label || ''),
  };
}


function getMarketExpertAngles(item) {
  const angles = [];
  const snapshot = getMarketExpertSnapshot(item);
  const posLabel = getReadablePositionLabel(snapshot.matchup);

  if (snapshot.defRank > 0 || snapshot.matchup?.lean) {
    let matchupKey = 'neutral_matchup';
    let matchupTone = 'neutral';
    let matchupPrefix = 'Neutral';
    if (snapshot.matchupState === 'favorable') {
      matchupKey = 'favorable_matchup';
      matchupTone = 'favorable';
      matchupPrefix = 'Favorable';
    } else if (snapshot.matchupState === 'tough') {
      matchupKey = 'tough_matchup';
      matchupTone = 'tough';
      matchupPrefix = 'Tough';
    }
    const rankNote = snapshot.defRank > 0 ? ` (Rank #${snapshot.defRank})` : '';
    const matchupLabel = `${matchupPrefix} vs ${posLabel}${rankNote}`;
    angles.push({ key: matchupKey, label: matchupLabel, tone: matchupTone, kind: 'matchup' });
  }

  if (snapshot.minutesLast5 > 0) {
    const minuteTone = snapshot.minutesTrend === 'down' ? 'neutral' : 'info';
    angles.push({ key: 'stable_minutes', label: `Minutes lately • ${snapshot.minutesLast5.toFixed(1)}`, tone: minuteTone });
  }
  if (snapshot.fgaLast5 > 0) {
    const fgaTone = snapshot.fgaTrend === 'down' ? 'neutral' : 'info';
    angles.push({ key: 'stable_fga', label: `FGA lately • ${snapshot.fgaLast5.toFixed(1)}`, tone: fgaTone });
  }
  if (snapshot.h2hGames > 0) {
    angles.push({ key: 'h2h_over', label: `H2H • ${snapshot.h2hHits}/${snapshot.h2hGames} (${snapshot.h2hHitRate.toFixed(0)}%)`, tone: snapshot.h2hHitRate >= currentExpertSettings.min_h2h_hit_rate ? 'positive' : 'neutral' });
  }
  if (!snapshot.isBackToBack && snapshot.restDays >= 2) {
    angles.push({ key: 'rest_edge', label: `Rest edge • ${snapshot.restDays} days`, tone: 'positive' });
  }
  if (!snapshot.isBackToBack) angles.push({ key: 'avoid_b2b', label: 'No back-to-back', tone: 'info' });
  if (snapshot.paceBucket) {
    const paceTone = snapshot.paceBucket === 'fast' ? 'positive' : (snapshot.paceBucket === 'slow' ? 'tough' : 'neutral');
    angles.push({ key: snapshot.paceBucket === 'fast' ? 'fast_pace' : 'avoid_slow_pace', label: snapshot.paceLabel || `Pace • ${snapshot.paceBucket}`, tone: paceTone });
  }
  if (snapshot.projectedSpread !== null && Number.isFinite(snapshot.projectedSpread)) {
    const spreadTone = Math.abs(snapshot.projectedSpread) >= 8 ? 'tough' : 'info';
    const spreadLabel = snapshot.spreadLabel || `Est. spread • ${snapshot.projectedSpread > 0 ? '+' : ''}${snapshot.projectedSpread.toFixed(1)}`;
    angles.push({ key: 'spread_threshold', label: spreadLabel, tone: spreadTone });
  }
  return angles;
}

function passesExpertFilters(item) {
  const snapshot = getMarketExpertSnapshot(item);
  if (currentExpertFilters.favorable_matchup && snapshot.matchupState !== 'favorable') return false;
  if (currentExpertFilters.neutral_matchup && snapshot.matchupState !== 'neutral') return false;
  if (currentExpertFilters.tough_matchup && snapshot.matchupState !== 'tough') return false;
  if (currentExpertFilters.stable_minutes && !(snapshot.minutesLast5 >= currentExpertSettings.min_minutes && snapshot.minutesTrend !== 'down')) return false;
  if (currentExpertFilters.stable_fga && !(snapshot.fgaLast5 >= currentExpertSettings.min_fga && snapshot.fgaTrend !== 'down')) return false;
  if (currentExpertFilters.h2h_over && !(snapshot.h2hGames >= currentExpertSettings.min_h2h_games && snapshot.h2hHitRate >= currentExpertSettings.min_h2h_hit_rate)) return false;
  if (currentExpertFilters.rest_edge && !(snapshot.restDays >= 2 && !snapshot.isBackToBack)) return false;
  if (currentExpertFilters.avoid_b2b && snapshot.isBackToBack) return false;
  if (currentExpertFilters.fast_pace && snapshot.paceBucket !== 'fast') return false;
  if (currentExpertFilters.avoid_slow_pace && snapshot.paceBucket === 'slow') return false;
  return true;
}

function renderMarketExpertFilterChips() {
  if (!marketExpertFilterChips) return;
  marketExpertFilterChips.innerHTML = getExpertFilterDefinitions().map(([key, label]) => `
    <button class="market-filter-chip expert-chip ${currentExpertFilters[key] ? 'active' : ''}" type="button" data-expert-filter-key="${key}">${label}</button>
  `).join('');
}

function renderOverviewSelection() {
  if (!overviewCurrentCard || !sidebarMiniPlayer) return;

  if (!selectedPlayer) {
    overviewCurrentCard.innerHTML = `
      <div class="overview-current-avatar placeholder-avatar">NBA</div>
      <div class="overview-current-copy">
        <span class="selected-player-label">Waiting for a selection</span>
        <strong>No player selected</strong>
        <small>Choose a team and player in the analyzer to fill this card.</small>
      </div>
    `;

    sidebarMiniPlayer.innerHTML = `
      <div class="sidebar-mini-avatar placeholder-avatar">NBA</div>
      <div>
        <strong>No player selected</strong>
        <small>Pick a team and player to begin.</small>
      </div>
    `;
    return;
  }

  const subLine = [
    selectedPlayer.team_name || selectedPlayer.team_abbreviation || '',
    selectedPlayer.position || '',
    selectedPlayer.jersey ? `#${selectedPlayer.jersey}` : ''
  ].filter(Boolean).join(' • ');

  const contextData = getSelectedPlayerContextData();
  const availability = contextData.availability || selectedPlayer.availability || null;
  const nextGame = contextData.matchup?.next_game || null;
  const vsPosition = contextData.matchup?.vs_position || null;
  const environment = contextData.environment || null;

  const matchupTone = (vsPosition?.lean_tone || 'neutral').toLowerCase();
  const matchupLabel = vsPosition?.lean || 'Matchup pending';
  const venueText = nextGame ? (nextGame.is_home ? 'Home' : 'Away') : (environment?.venue_label || 'Venue TBD');
  const restText = Number.isInteger(environment?.rest_days) ? `${environment.rest_days} day${environment.rest_days === 1 ? '' : 's'} rest` : 'Rest TBD';
  const opponentText = nextGame?.opponent_abbreviation || nextGame?.opponent_name || 'Opponent TBD';
  const gameDateText = nextGame?.game_date ? formatNextGameDate(nextGame.game_date) : 'Date TBD';

  overviewCurrentCard.innerHTML = `
    <div class="overview-snapshot-shell">
      <div class="overview-snapshot-main">
        <img class="overview-current-avatar-img" src="${getPlayerImage(selectedPlayer.id)}" alt="${escapeHtml(selectedPlayer.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
        <div class="overview-current-copy overview-snapshot-copy">
          <span class="selected-player-label">Current focus</span>
          <strong>${escapeHtml(selectedPlayer.full_name)}</strong>
          <small>${escapeHtml(subLine || (selectedPlayer.is_active ? 'Active player' : 'Player'))}</small>
          <div class="overview-snapshot-chip-row">
            ${availability ? `<span class="overview-snapshot-chip status">${renderAvailabilityBadge(availability, true)}</span>` : ''}
            <span class="overview-snapshot-chip ${matchupTone}">${escapeHtml(matchupLabel)}</span>
            <span class="overview-snapshot-chip muted">${escapeHtml(venueText)}</span>
            <span class="overview-snapshot-chip muted">${escapeHtml(restText)}</span>
          </div>
        </div>
      </div>
      <div class="overview-snapshot-meta-grid">
        <div class="overview-snapshot-meta-card">
          <span class="small-label">Next matchup</span>
          <strong>${escapeHtml(opponentText)}</strong>
          <small>${escapeHtml(gameDateText)}</small>
        </div>
        <div class="overview-snapshot-meta-card">
          <span class="small-label">Position matchup</span>
          <strong>${escapeHtml(vsPosition?.rank_label || 'No rank')}</strong>
          <small>${escapeHtml(vsPosition?.position_label || 'Position split pending')}</small>
        </div>
      </div>
    </div>
  `;

  sidebarMiniPlayer.innerHTML = `
    <img class="sidebar-mini-avatar-img" src="${getPlayerImage(selectedPlayer.id)}" alt="${escapeHtml(selectedPlayer.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
    <div>
      <strong>${escapeHtml(selectedPlayer.full_name)}</strong>
      <small>${escapeHtml(subLine || 'Selected player')}</small>
    </div>
  `;
}

function getFallbackHeadshot() {
  return `data:image/svg+xml;charset=UTF-8,${FALLBACK_HEADSHOT}`;
}

function getPlayerImage(playerId) {
  return `https://cdn.nba.com/headshots/nba/latest/1040x760/${playerId}.png`;
}

function escapeHtml(text) {
  return String(text ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function getStatusMeta(text) {
  const raw = String(text || 'Idle');
  const normalized = raw.toLowerCase();

  if (normalized.includes('loading market pick')) {
    return { phase: 'working', state: 'Working', title: 'Opening this market pick', detail: 'Loading the player, line, and current trend view into the analyzer.', pill: 'working' };
  }
  if (normalized.includes('scanning market')) {
    return { phase: 'working', state: 'Scanning', title: 'Scanning the market board', detail: 'Comparing edge, EV, availability, and matchup context across every pasted prop.', pill: 'working' };
  }
  if (normalized.includes('finding bets')) {
    return { phase: 'working', state: 'Working', title: 'Finding the best team props', detail: 'Ranking the current roster using hit rate, opportunity, and recent form.', pill: 'working' };
  }
  if (normalized.includes('loading roster')) {
    return { phase: 'working', state: 'Loading', title: 'Loading team roster', detail: 'Pulling player cards, headshots, and current season roster information.', pill: 'working' };
  }
  if (normalized.includes('loading teams')) {
    return { phase: 'working', state: 'Loading', title: 'Preparing the dashboard', detail: 'Fetching the active NBA teams so the analyzer is ready to use.', pill: 'working' };
  }
  if (normalized.includes('loading')) {
    return { phase: 'working', state: 'Working', title: 'Running player analysis', detail: 'Checking recent form, matchup, availability, and opportunity trends for this prop.', pill: 'working' };
  }
  if (normalized.includes('ready')) {
    return { phase: 'success', state: 'Ready', title: 'Update complete', detail: 'The dashboard finished processing your latest request.', pill: 'ready' };
  }
  if (normalized.includes('roster ready')) {
    return { phase: 'success', state: 'Ready', title: 'Roster loaded', detail: 'The analyzer is ready for player selection.', pill: 'ready' };
  }
  if (normalized.includes('player selected')) {
    return { phase: 'success', state: 'Ready', title: 'Player selected', detail: 'Choose a prop and line when you are ready to run the analysis.', pill: 'ready' };
  }
  if (normalized.includes('error')) {
    return { phase: 'error', state: 'Issue', title: 'Something needs attention', detail: 'The request could not finish. Please try again or adjust the input.', pill: 'error' };
  }
  return { phase: 'idle', state: 'Idle', title: 'Standing by', detail: 'Select a player, scan a board, or open today\'s games to begin.', pill: 'idle' };
}

function showActionBanner(meta) {
  if (!actionBanner || !actionBannerTitle || !actionBannerDetail || !actionBannerState) return;
  actionBannerTitle.textContent = meta.title;
  actionBannerDetail.textContent = meta.detail;
  actionBannerState.textContent = meta.state;
  actionBanner.dataset.phase = meta.phase;
  actionBanner.classList.add('is-visible');
}

function hideActionBanner() {
  if (!actionBanner) return;
  actionBanner.classList.remove('is-visible');
  actionBanner.dataset.phase = 'idle';
}

function setStatus(text) {
  const meta = getStatusMeta(text);
  if (statusPill) {
    statusPill.textContent = text;
    statusPill.dataset.state = meta.pill;
  }

  if (statusBannerTimer) {
    clearTimeout(statusBannerTimer);
    statusBannerTimer = null;
  }

  if (meta.phase === 'working') {
    activeWorkCount += 1;
    showActionBanner(meta);
    return;
  }

  if (meta.phase === 'success') {
    activeWorkCount = Math.max(0, activeWorkCount - 1);
    showActionBanner(meta);
    statusBannerTimer = window.setTimeout(() => {
      if (activeWorkCount === 0) hideActionBanner();
    }, 1400);
    return;
  }

  if (meta.phase === 'error') {
    activeWorkCount = Math.max(0, activeWorkCount - 1);
    showActionBanner(meta);
    statusBannerTimer = window.setTimeout(() => {
      if (activeWorkCount === 0) hideActionBanner();
    }, 2600);
    return;
  }

  activeWorkCount = 0;
  hideActionBanner();
}


let currentAccentTeam = null;

function hexToRgbTuple(hex) {
  const cleaned = String(hex || '').replace('#', '').trim();
  const normalized = cleaned.length === 3
    ? cleaned.split('').map((ch) => ch + ch).join('')
    : cleaned.padEnd(6, '0').slice(0, 6);
  const value = parseInt(normalized, 16);
  return {
    r: (value >> 16) & 255,
    g: (value >> 8) & 255,
    b: value & 255
  };
}

function rgbTupleToHex({ r, g, b }) {
  const clamp = (n) => Math.max(0, Math.min(255, Math.round(Number(n) || 0)));
  return `#${[clamp(r), clamp(g), clamp(b)].map((v) => v.toString(16).padStart(2, '0')).join('')}`;
}

function rgbStringFromHex(hex) {
  const { r, g, b } = hexToRgbTuple(hex);
  return `${r}, ${g}, ${b}`;
}

function mixHex(hexA, hexB, weight = 0.5) {
  const a = hexToRgbTuple(hexA);
  const b = hexToRgbTuple(hexB);
  const w = Math.max(0, Math.min(1, Number(weight) || 0));
  return rgbTupleToHex({
    r: a.r * (1 - w) + b.r * w,
    g: a.g * (1 - w) + b.g * w,
    b: a.b * (1 - w) + b.b * w
  });
}

function relativeLuminance(hex) {
  const { r, g, b } = hexToRgbTuple(hex);
  const normalize = (value) => {
    const channel = value / 255;
    return channel <= 0.03928 ? channel / 12.92 : Math.pow((channel + 0.055) / 1.055, 2.4);
  };
  return (0.2126 * normalize(r)) + (0.7152 * normalize(g)) + (0.0722 * normalize(b));
}

function ensureReadableAccent(hex, theme = 'dark') {
  let safe = hex || '#4da3ff';
  const minLum = theme === 'light' ? 0.09 : 0.18;
  const maxLum = theme === 'light' ? 0.28 : 0.62;
  let lum = relativeLuminance(safe);
  let guard = 0;

  while (lum > maxLum && guard < 10) {
    safe = mixHex(safe, '#10203f', theme === 'light' ? 0.3 : 0.18);
    lum = relativeLuminance(safe);
    guard += 1;
  }

  while (lum < minLum && guard < 20) {
    safe = mixHex(safe, '#7fc7ff', theme === 'light' ? 0.1 : 0.2);
    lum = relativeLuminance(safe);
    guard += 1;
  }

  return safe;
}

function getReadableTeamPalette(teamAbbreviation, theme = 'dark') {
  const base = TEAM_COLORS[teamAbbreviation] || { accent: '#4da3ff', accent2: '#8571ff', rgb: '77, 163, 255' };
  const accent = ensureReadableAccent(base.accent, theme);
  let accent2 = ensureReadableAccent(base.accent2 || base.accent, theme === 'light' ? 'light' : 'dark');

  if (Math.abs(relativeLuminance(accent) - relativeLuminance(accent2)) < 0.05) {
    accent2 = theme === 'light'
      ? mixHex(accent2, '#ffffff', 0.12)
      : mixHex(accent2, '#b58cff', 0.16);
    accent2 = ensureReadableAccent(accent2, theme);
  }

  return {
    accent,
    accent2,
    rgb: rgbStringFromHex(accent),
    accent2Rgb: rgbStringFromHex(accent2)
  };
}

function updateAccentForCurrentTheme(teamAbbreviation) {
  const theme = document.body.classList.contains('light-theme') ? 'light' : 'dark';
  const palette = getReadableTeamPalette(teamAbbreviation, theme);
  document.documentElement.style.setProperty('--accent', palette.accent);
  document.documentElement.style.setProperty('--accent-2', palette.accent2);
  document.documentElement.style.setProperty('--accent-rgb', palette.rgb);
  document.documentElement.style.setProperty('--accent-2-rgb', palette.accent2Rgb);
}

function setTheme(theme) {
  document.body.classList.toggle('light-theme', theme === 'light');
  localStorage.setItem(THEME_KEY, theme);
  themeToggle.querySelector('.theme-icon').textContent = theme === 'light' ? '🌙' : '☀';
  themeToggle.querySelector('.theme-text').textContent = theme === 'light' ? 'Dark' : 'Light';
  updateAccentForCurrentTheme(currentAccentTeam);

  if (lastPayload) {
    renderChart(lastPayload);
  }
}

function applySavedTheme() {
  const saved = localStorage.getItem(THEME_KEY);
  const prefersLight = window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches;
  setTheme(saved || (prefersLight ? 'light' : 'dark'));
}

if (window.matchMedia) {
  const media = window.matchMedia('(prefers-color-scheme: light)');
  const handleThemePreferenceChange = (event) => {
    if (!localStorage.getItem(THEME_KEY)) {
      setTheme(event.matches ? 'light' : 'dark');
    }
  };
  if (typeof media.addEventListener === 'function') {
    media.addEventListener('change', handleThemePreferenceChange);
  } else if (typeof media.addListener === 'function') {
    media.addListener(handleThemePreferenceChange);
  }
}

function applyTeamAccent(teamAbbreviation) {
  currentAccentTeam = teamAbbreviation || null;
  updateAccentForCurrentTheme(currentAccentTeam);

  if (lastPayload) {
    renderChart(lastPayload);
  }
}

function getStatLabel(stat) {
  const labels = {
    PTS: 'Points',
    REB: 'Rebounds',
    AST: 'Assists',
    '3PM': '3-Pointers Made',
    STL: 'Steals',
    BLK: 'Blocks',
    PRA: 'Points + Rebounds + Assists',
    PR: 'Points + Rebounds',
    PA: 'Points + Assists',
    RA: 'Rebounds + Assists'
  };
  return labels[stat] || stat;
}


function getComboStatParts(stat) {
  const partsMap = {
    PRA: ['PTS', 'REB', 'AST'],
    PR: ['PTS', 'REB'],
    PA: ['PTS', 'AST'],
    RA: ['REB', 'AST']
  };
  return partsMap[stat] || [];
}

function getComboStatColor(stat, index = 0) {
  const palette = {
    PTS: 'rgba(59, 130, 246, 0.82)',
    REB: 'rgba(16, 185, 129, 0.82)',
    AST: 'rgba(168, 85, 247, 0.82)'
  };
  const fallback = ['rgba(59, 130, 246, 0.82)', 'rgba(16, 185, 129, 0.82)', 'rgba(168, 85, 247, 0.82)'];
  return palette[stat] || fallback[index % fallback.length];
}

function getActivePropChip() {
  return propButtonsWrap.querySelector(`.prop-chip[data-stat="${selectedStat}"]`);
}

function setActiveProp(stat) {
  selectedStat = stat;
  propButtonsWrap.querySelectorAll('.prop-chip').forEach(chip => {
    chip.classList.toggle('active', chip.dataset.stat === stat);
  });
}

function loadRecentPlayers() {
  try {
    return JSON.parse(localStorage.getItem(RECENT_PLAYERS_KEY) || '[]');
  } catch {
    return [];
  }
}

function saveRecentPlayer(player) {
  const existing = loadRecentPlayers().filter(item => item.id !== player.id);
  const trimmed = [
    {
      id: player.id,
      full_name: player.full_name,
      is_active: player.is_active,
      team_abbreviation: player.team_abbreviation || '',
      team_name: player.team_name || '',
      team_id: player.team_id || '',
      position: player.position || '',
      jersey: player.jersey || ''
    },
    ...existing
  ].slice(0, 6);

  localStorage.setItem(RECENT_PLAYERS_KEY, JSON.stringify(trimmed));
  renderRecentPlayers();
}

function clearRecentPlayersState({ resetCurrent = true } = {}) {
  localStorage.removeItem(RECENT_PLAYERS_KEY);
  renderRecentPlayers();

  if (resetCurrent) {
    selectedPlayer = null;
    playerSearchInput.value = '';
    renderSelectedPlayer();
    updateSelectedCardStyles();
    resetDashboardForNoSelection();
    renderGameLogTab('recent');
  }
}

function renderRecentPlayers() {
  const recent = loadRecentPlayers();

  if (!recent.length) {
    recentPlayersSection.classList.add('hidden');
    recentPlayersContainer.innerHTML = '';
    return;
  }

  recentPlayersSection.classList.remove('hidden');
  recentPlayersContainer.innerHTML = recent.map(player => `
    <button class="mini-player-card" data-id="${player.id}" data-name="${escapeHtml(player.full_name)}" data-active="${player.is_active}" data-team-abbr="${escapeHtml(player.team_abbreviation || '')}" data-team-name="${escapeHtml(player.team_name || '')}" data-team-id="${escapeHtml(player.team_id || '')}" data-position="${escapeHtml(player.position || '')}" data-jersey="${escapeHtml(player.jersey || '')}">
      <img src="${getPlayerImage(player.id)}" alt="${escapeHtml(player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
      <span>${escapeHtml(player.full_name)}</span>
      <small>${escapeHtml(player.team_abbreviation || 'Recent')}</small>
    </button>
  `).join('');

  recentPlayersContainer.querySelectorAll('.mini-player-card').forEach(card => {
    card.addEventListener('click', () => {
      switchView('analyzer');
      setSelectedPlayer({
        id: Number(card.dataset.id),
        full_name: card.dataset.name,
        is_active: card.dataset.active === 'true',
        team_abbreviation: card.dataset.teamAbbr,
        team_name: card.dataset.teamName,
        team_id: card.dataset.teamId ? Number(card.dataset.teamId) : null,
        position: card.dataset.position,
        jersey: card.dataset.jersey
      });
    });
  });
}

function updateSelectedCardStyles() {
  document.querySelectorAll('.player-card').forEach(card => {
    const isSelected = selectedPlayer && Number(card.dataset.id) === selectedPlayer.id;
    card.classList.toggle('selected', Boolean(isSelected));
  });
}

function getSelectedPlayerContextData() {
  const payload = lastPayload && String(lastPayload?.player?.id || '') === String(selectedPlayer?.id || '') ? lastPayload : null;
  return {
    matchup: selectedPlayer?.matchup || payload?.matchup || null,
    environment: selectedPlayer?.environment || payload?.environment || null,
    availability: selectedPlayer?.availability || payload?.availability || null
  };
}

function renderSelectedPlayerContext() {
  if (!selectedPlayer) return '';
  const { matchup, environment, availability } = getSelectedPlayerContextData();
  const nextGame = matchup?.next_game || null;
  const vsPosition = matchup?.vs_position || null;
  if (!nextGame && !vsPosition && !environment) return '';

  const matchupLabel = nextGame?.matchup_label || nextGame?.opponent_name || 'Upcoming game';
  const opponentContext = vsPosition
    ? `${vsPosition.lean || 'Neutral'} • ${vsPosition.position_label || 'Position'} • ${typeof vsPosition.opponent_value === 'number' ? vsPosition.opponent_value.toFixed(2) : (vsPosition.opponent_value ?? '—')} ${getStatLabel(vsPosition.stat).toLowerCase()}`
    : 'Defense-vs-position pending';
  const venueLabel = environment?.venue_label || (nextGame ? (nextGame.is_home ? 'Home game' : 'Away game') : 'TBD');
  const scheduleLabel = environment?.schedule_summary || (nextGame?.game_date ? formatNextGameDate(nextGame.game_date) : 'Upcoming spot');
  const restDays = environment?.rest_days ?? nextGame?.rest_days;
  const gamesIn7 = environment?.games_in_7 ?? nextGame?.games_in_last_7;
  const backToBack = environment?.is_back_to_back ?? nextGame?.is_back_to_back;
  const availabilityNote = availability?.reason || availability?.note || '';
  const restStrong = restDays === null || restDays === undefined ? '—' : `${restDays} day${Number(restDays) === 1 ? '' : 's'} rest`;
  const loadSmall = backToBack ? 'Back-to-back spot' : `Games in last 7: ${gamesIn7 ?? '—'}`;

  return `
    <div class="selected-player-context">
      <div class="selected-player-context-header">
        <span class="selected-player-section-label">Next Matchup</span>
        <strong>${escapeHtml(matchupLabel)}</strong>
        <small>${escapeHtml(nextGame?.game_date ? formatNextGameDate(nextGame.game_date) : 'Date TBA')}${nextGame?.game_time ? ` • ${escapeHtml(nextGame.game_time)}` : ''}</small>
      </div>
      <div class="selected-player-context-grid">
        <div class="selected-player-context-chip">
          <span class="small-label">Opponent context</span>
          <strong>${escapeHtml(nextGame?.opponent_name || nextGame?.opponent_abbreviation || 'Unavailable')}</strong>
          <small>${escapeHtml(opponentContext)}</small>
        </div>
        <div class="selected-player-context-chip">
          <span class="small-label">Venue</span>
          <strong>${escapeHtml(venueLabel)}</strong>
          <small>${escapeHtml(scheduleLabel)}</small>
        </div>
        <div class="selected-player-context-chip">
          <span class="small-label">Rest / Load</span>
          <strong>${escapeHtml(restStrong)}</strong>
          <small>${escapeHtml(loadSmall)}</small>
        </div>
      </div>
      ${availabilityNote ? `<div class="selected-player-meta-row context-availability-row">${renderAvailabilityBadge(availability, true)}<small>${escapeHtml(availabilityNote)}</small></div>` : ''}
    </div>
  `;
}

function renderSelectedPlayer() {
  if (!selectedPlayer) {
    selectedPlayerBadge.className = 'selected-player selected-player-empty';
    selectedPlayerBadge.innerHTML = `
      <div class="selected-player-avatar placeholder-avatar">NBA</div>
      <div class="selected-player-copy">
        <span class="selected-player-label">Waiting for a selection</span>
        <strong>No player selected</strong>
        <small>Choose a team, then click a player card to start.</small>
      </div>
    `;
    renderOverviewSelection();
    return;
  }

  const subLine = [
    selectedPlayer.team_name || selectedPlayer.team_abbreviation || '',
    selectedPlayer.position || '',
    selectedPlayer.jersey ? `#${selectedPlayer.jersey}` : ''
  ].filter(Boolean).join(' • ');
  const contextData = getSelectedPlayerContextData();
  const availabilitySource = contextData.availability || selectedPlayer.availability;
  const availabilityHtml = availabilitySource
    ? `<div class="selected-player-meta-row">${renderAvailabilityBadge(availabilitySource)}<small>${escapeHtml(availabilitySource.reason || availabilitySource.note || '')}</small></div>`
    : '';

  selectedPlayerBadge.className = 'selected-player';
  selectedPlayerBadge.innerHTML = `
    <img src="${getPlayerImage(selectedPlayer.id)}" alt="${escapeHtml(selectedPlayer.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
    <div class="selected-player-copy">
      <span class="selected-player-label">Selected player</span>
      <strong>${escapeHtml(selectedPlayer.full_name)}</strong>
      <small>${escapeHtml(subLine || (selectedPlayer.is_active ? 'Active player' : 'Player'))}</small>
      ${availabilityHtml}
      ${renderSelectedPlayerContext()}
    </div>
  `;
  renderOverviewSelection();
}

function clearAnalysisForNewSelection() {
  resetDashboardForNoSelection();

  if (interpretationTone) {
    interpretationTone.className = 'spotlight-pill neutral';
    interpretationTone.textContent = 'Waiting for analysis';
  }

  if (opportunityTone) {
    opportunityTone.className = 'spotlight-pill neutral';
    opportunityTone.textContent = 'Waiting for analysis';
  }

  if (environmentTone) {
    environmentTone.className = 'spotlight-pill neutral';
    environmentTone.textContent = 'Waiting for analysis';
  }

  if (interpretationBody) {
    interpretationBody.className = 'interpretation-body';
    interpretationBody.innerHTML = `
      <div class="insight-summary neutral">
        <span class="insight-summary-label">Quick read</span>
        <strong>Waiting for analysis</strong>
        <p>Select a player and run the analyzer to generate a simple interpretation.</p>
      </div>
      <ul class="insight-bullet-list compact-bullets">
        <li>Recent trend, matchup, and opportunity notes will appear here.</li>
      </ul>
    `;
  }

  if (opportunityBody) {
    opportunityBody.className = 'opportunity-body';
    opportunityBody.innerHTML = `
      <div class="opportunity-chip-grid refined-opportunity-grid">
        <div class="opportunity-chip">
          <span class="small-label">Minutes</span>
          <strong>—</strong>
          <small>Waiting for analysis</small>
        </div>
        <div class="opportunity-chip">
          <span class="small-label">FGA</span>
          <strong>—</strong>
          <small>Waiting for analysis</small>
        </div>
        <div class="opportunity-chip">
          <span class="small-label">3PA</span>
          <strong>—</strong>
          <small>Waiting for analysis</small>
        </div>
        <div class="opportunity-chip">
          <span class="small-label">FTA</span>
          <strong>—</strong>
          <small>Waiting for analysis</small>
        </div>
      </div>
      <div class="opportunity-summary-wrap refined-opportunity-wrap">
        <div class="insight-summary neutral compact-summary">
          <span class="insight-summary-label">Opportunity read</span>
          <p class="opportunity-summary">Analyze a player prop to load minutes, attempts, and team context.</p>
        </div>
        <div class="team-context-box neutral">
          <strong>Team context</strong>
          <p>No team-availability context yet.</p>
          <small>Latest same-team absences will appear here after analysis.</small>
        </div>
      </div>
    `;
  }

  if (environmentBody) {
    environmentBody.className = 'environment-body';
    environmentBody.innerHTML = `
      <div class="environment-chip-grid">
        <div class="environment-chip">
          <span class="small-label">Venue</span>
          <strong>—</strong>
          <small>Waiting for analysis</small>
        </div>
        <div class="environment-chip">
          <span class="small-label">Rest</span>
          <strong>—</strong>
          <small>Waiting for analysis</small>
        </div>
        <div class="environment-chip">
          <span class="small-label">Back-to-back</span>
          <strong>—</strong>
          <small>Waiting for analysis</small>
        </div>
        <div class="environment-chip">
          <span class="small-label">Games in 7</span>
          <strong>—</strong>
          <small>Waiting for analysis</small>
        </div>
      </div>
      <div class="insight-summary neutral compact-summary">
        <span class="insight-summary-label">Schedule read</span>
        <p class="opportunity-summary">Analyze a player prop to see rest, back-to-back risk, and the upcoming spot.</p>
      </div>
    `;
  }
}

function setSelectedPlayer(player) {
  const previousId = selectedPlayer?.id ?? null;
  const nextId = player?.id ?? null;
  const changedPlayer = previousId !== nextId;

  selectedPlayer = player;

  if (changedPlayer) {
    clearAnalysisForNewSelection();
    setStatus('Player selected');
  }

  renderSelectedPlayer();
  playerSearchInput.value = player.full_name;
  searchResults.classList.add('hidden');
  updateSelectedCardStyles();
  saveRecentPlayer(player);

  if (player.team_abbreviation) {
    applyTeamAccent(player.team_abbreviation);
  }

  // Persist last player across refreshes
  try { localStorage.setItem(LAST_PLAYER_KEY, JSON.stringify({
    id: player.id, full_name: player.full_name, is_active: player.is_active,
    team_abbreviation: player.team_abbreviation || '', team_name: player.team_name || '',
    team_id: player.team_id || null, position: player.position || '', jersey: player.jersey || ''
  })); } catch { /* ignore */ }
}

function renderRosterSkeleton() {
  playerGrid.classList.remove('empty-grid');
  playerGrid.classList.remove('has-scroll');
  playerGrid.innerHTML = Array.from({ length: 8 }).map(() => '<div class="skeleton-card"></div>').join('');
}

function updateRosterScrollState(players, season) {
  requestAnimationFrame(() => {
    const hasScroll = playerGrid.scrollHeight > playerGrid.clientHeight + 8;
    playerGrid.classList.toggle('has-scroll', hasScroll);

    if (selectedTeam) {
      rosterMeta.textContent = `${players.length} players • ${season}${hasScroll ? ' • Scroll for more' : ''}`;
    }
  });
}

async function loadTeams(force = false) {
  if (!force && teamSelect && teamSelect.options.length > 1) {
    return;
  }

  const response = await fetch('/api/teams');
  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    teamSelect.innerHTML = '<option value="">Failed to load teams</option>';
    throw new Error(data.detail || 'Failed to load teams.');
  }

  const teams = Array.isArray(data) ? data : (Array.isArray(data.results) ? data.results : (Array.isArray(data.teams) ? data.teams : []));

  teamSelect.innerHTML = '';
  const placeholder = document.createElement('option');
  placeholder.value = '';
  placeholder.textContent = 'Choose a team';
  teamSelect.appendChild(placeholder);

  teams.forEach(team => {
    const option = document.createElement('option');
    option.value = String(team.id);
    option.textContent = team.full_name || `${team.city || ''} ${team.nickname || ''}`.trim() || String(team.id);
    option.dataset.abbreviation = team.abbreviation || '';
    option.dataset.name = team.full_name || option.textContent;
    teamSelect.appendChild(option);
  });

  // Also populate the opponent override selector
  if (oppSelect) {
    oppSelect.innerHTML = '';
    const autoOpt = document.createElement('option');
    autoOpt.value = '';
    autoOpt.textContent = 'Auto (next scheduled)';
    oppSelect.appendChild(autoOpt);
    teams.forEach(team => {
      const option = document.createElement('option');
      option.value = String(team.id);
      option.textContent = `${team.abbreviation || ''} — ${team.full_name || ''}`.trim();
      option.dataset.abbreviation = team.abbreviation || '';
      oppSelect.appendChild(option);
    });
  }
}

async function ensureTeamsLoaded(force = false) {
  if (!teamSelect) return;
  if (!force && teamSelect.options.length > 1) return;
  try {
    await loadTeams(force);
  } catch (error) {
    console.error(error);
  }
}

async function loadRoster(teamId) {
  const params = new URLSearchParams();
  const season = seasonInput.value.trim();
  if (season) params.set('season', season);

  const query = params.toString();
  const response = await fetch(`/api/teams/${teamId}/roster${query ? `?${query}` : ''}`);
  const payload = await response.json();

  if (!response.ok) {
    throw new Error(payload.detail || 'Failed to load roster.');
  }

  rosterPlayers = payload.results || [];
  selectedTeam = payload.team;
  applyTeamAccent(payload.team?.abbreviation);
  renderRoster(payload.team, payload.season, rosterPlayers);
}

function renderRoster(team, season, players) {
  rosterTitle.textContent = team.full_name + ' roster';
  const outCnt  = players.filter(p => p.is_unavailable).length;
  const riskCnt = players.filter(p => p.is_risky && !p.is_unavailable).length;
  let injNote = '';
  if (outCnt)  injNote += ' • ' + outCnt + ' out';
  if (riskCnt) injNote += (outCnt ? ', ' : ' • ') + riskCnt + ' questionable';
  rosterMeta.innerHTML =
    players.length + ' players • ' + season + injNote +
    '&nbsp;<button class="inj-report-btn" onclick="toggleInjuryPanel(' + team.id + ')">Injury Report</button>';

  if (!players.length) {
    playerGrid.classList.remove('has-scroll');
    playerGrid.classList.add('empty-grid');
    playerGrid.innerHTML = `
      <div class="empty-roster-state empty-state-panel compact">
        <div class="empty-icon">🧍</div>
        <strong>No players found for this team and season.</strong>
        <span>Try another season or another team.</span>
      </div>
    `;
    return;
  }

  playerGrid.classList.remove('empty-grid');
  playerGrid.innerHTML = players.map(player => {
    const injSt  = player.injury_status || '';
    const isOut  = !!player.is_unavailable;
    const isRisk = !!player.is_risky && !isOut;
    const cardCls = isOut ? 'player-card player-card--out' : isRisk ? 'player-card player-card--risk' : 'player-card';
    const badge   = isOut
      ? `<span class="inj-badge out">${escapeHtml(injSt)}</span>`
      : isRisk
        ? `<span class="inj-badge risk">${escapeHtml(injSt)}</span>`
        : '';
    return `<button class="${cardCls}" data-id="${player.id}">
      <img src="${getPlayerImage(player.id)}" alt="${escapeHtml(player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
      <div class="player-card-head">
        <div class="player-card-name">${escapeHtml(player.full_name)}</div>
        <span class="jersey-pill">${escapeHtml(player.jersey || '--')}</span>
      </div>
      <div class="player-card-meta">${escapeHtml(player.position || 'N/A')} ${badge}</div>
      <div class="player-card-team">${escapeHtml(player.team_abbreviation || team.abbreviation)}</div>
    </button>`;
  }).join('');

  playerGrid.querySelectorAll('.player-card').forEach((card, index) => {
    card.addEventListener('click', () => {
      const player = players[index];
      setSelectedPlayer(player);
    });
  });

  updateSelectedCardStyles();
  updateRosterScrollState(players, season);
}

// ── Injury report slide-down panel ───────────────────────────────────────
let _injPanelTeamId = null;

async function toggleInjuryPanel(teamId) {
  let panel = document.getElementById('rosterInjuryPanel');
  if (!panel) {
    panel = document.createElement('div');
    panel.id = 'rosterInjuryPanel';
    panel.className = 'roster-injury-panel';
    const grid = document.getElementById('playerGrid');
    if (grid && grid.parentNode) grid.parentNode.insertBefore(panel, grid);
  }
  if (_injPanelTeamId === teamId && panel.classList.contains('open')) {
    panel.classList.remove('open');
    _injPanelTeamId = null;
    return;
  }
  _injPanelTeamId = teamId;
  panel.classList.add('open');
  panel.innerHTML = '<div class="inj-panel-loading">Loading injury report…</div>';
  try {
    const resp = await fetch('/api/teams/' + teamId + '/injury-report');
    if (!resp.ok) throw new Error('Failed');
    const d = await resp.json();
    const ps = d.players || [];
    const rl = d.report_label ? '<span class="inj-report-date">' + escapeHtml(d.report_label) + '</span>' : '';
    if (!ps.length) {
      panel.innerHTML = '<div class="inj-panel-clean">' + escapeHtml(d.team_name) + ' — no players listed on injury report. ' + rl + '</div>';
      return;
    }
    panel.innerHTML =
      '<div class="inj-panel-header">' +
        '<strong>' + escapeHtml(d.team_name) + ' Injury Report</strong>' + rl +
      '</div>' +
      '<div class="inj-panel-rows">' +
        ps.map(function(p) {
          const cls = p.is_unavailable ? 'out' : p.is_risky ? 'risk' : 'ok';
          return '<div class="inj-panel-row ' + cls + '">' +
            '<span class="ipn">' + escapeHtml(formatPlayerName(p.name)) + '</span>' +
            '<span class="ips inj-badge ' + cls + '">' + escapeHtml(p.status) + '</span>' +
            '<span class="ipr">' + escapeHtml(p.reason || '') + '</span>' +
          '</div>';
        }).join('') +
      '</div>';
  } catch(e) {
    panel.innerHTML = '<div class="inj-panel-loading">Could not load injury report.</div>';
  }
}

async function searchPlayers(query) {
  const response = await fetch(`/api/players/search?q=${encodeURIComponent(query)}`);
  if (!response.ok) throw new Error('Failed to search players.');
  return response.json();
}

function renderSearchResults(results) {
  if (!results.length) {
    searchResults.innerHTML = '<div class="search-item"><div class="search-copy"><div>No players found.</div><small>Try another spelling.</small></div></div>';
    searchResults.classList.remove('hidden');
    return;
  }

  searchResults.innerHTML = results.map(player => `
    <div class="search-item" data-id="${player.id}" data-name="${escapeHtml(player.full_name)}" data-active="${player.is_active}">
      <img src="${getPlayerImage(player.id)}" alt="${escapeHtml(player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
      <div class="search-copy">
        <div>${escapeHtml(player.full_name)}</div>
        <small>${player.is_active ? 'Active player' : 'Inactive player'}</small>
      </div>
    </div>
  `).join('');

  searchResults.classList.remove('hidden');

  searchResults.querySelectorAll('.search-item').forEach(item => {
    item.addEventListener('click', () => {
      const rosterMatch = rosterPlayers.find(player => player.id === Number(item.dataset.id));
      setSelectedPlayer(rosterMatch || {
        id: Number(item.dataset.id),
        full_name: item.dataset.name,
        is_active: item.dataset.active === 'true',
        team_abbreviation: selectedTeam?.abbreviation || '',
        team_name: selectedTeam?.full_name || '',
        team_id: selectedTeam?.id || null
      });
    });
  });
}

function renderOverviewBestBets() {
  if (!overviewBestBets || !overviewBestBetsMeta) return;

  const stored = loadStoredMarketResults();
  const results = stored?.results || [];
  overviewBestBetsMeta.textContent = results.length ? formatStoredTime(stored.updated_at) : 'Populated by your latest Market Scanner run.';

  if (!results.length) {
    overviewBestBets.className = 'overview-best-bets-list empty-state-panel compact';
    overviewBestBets.innerHTML = `
      <div class="empty-icon">⭐</div>
      <strong>No best bets saved yet.</strong>
      <span>Run Market Scanner to pin the strongest current board edges here.</span>
    `;
    return;
  }

  overviewBestBets.className = 'overview-best-bets-list';
  overviewBestBets.innerHTML = results.slice(0, 4).map((item, index) => `
    <button class="overview-best-bet-card ${item.best_bet.confidence_tone || ''}" data-index="${index}" type="button">
      <div class="overview-best-bet-head">
        <span class="overview-rank">#${index + 1}</span>
        <span class="finder-badge ${item.best_bet.confidence_tone || ''}">${item.best_bet.display_side || item.best_bet.side} • ${item.best_bet.confidence}${item.best_bet.confidence_score ? ` ${item.best_bet.confidence_score}` : ''}</span>
      </div>
      <strong>${escapeHtml(item.player.full_name)}</strong>
      <small>${escapeHtml(item.market.stat)} ${item.market.line} • ${escapeHtml(item.player.team || '')}${item.player.opponent ? ` vs ${escapeHtml(item.player.opponent)}` : ''}</small>
      <div class="overview-best-bet-metrics">
        <span>Edge ${item.best_bet.edge ?? '—'}%</span>
        <span>EV ${item.best_bet.ev ?? '—'}%</span>
        <span>${item.analysis.hit_rate}% hit</span>
      </div>
      <p>${escapeHtml(item.best_bet.confidence_summary || 'Latest scanner leader.')}</p>
    </button>
  `).join('');

  overviewBestBets.querySelectorAll('.overview-best-bet-card').forEach(card => {
    card.addEventListener('click', async () => {
      const item = results[Number(card.dataset.index)];
      if (!item) return;
      await focusMarketPlayer(item);
    });
  });
}

function buildTodayGameCard(game, compact = false) {
  const statusClass = game.status_category || 'scheduled';
  const homeSummary = game.home.availability?.headline || 'Clean report';
  const awaySummary = game.away.availability?.headline || 'Clean report';
  const scoreLine = game.status_category === 'scheduled'
    ? `<div class="today-game-time">${escapeHtml(game.status_text)}</div>`
    : `<div class="today-game-scoreline"><span>${game.away.score}</span><small>-</small><span>${game.home.score}</span></div>`;

  // Injury summary chips for each team
  function injChips(teamData) {
    const players = teamData.injury_players || [];
    if (!players.length) return '<span class="today-inj-clean">\u2713 Clean</span>';
    return players.slice(0, 4).map(function(p) {
      const cls = p.is_unavailable ? 'out' : 'risky';
      return '<span class="today-inj-chip ' + cls + '" title="' + escapeHtml(p.injury_reason || '') + '">'
        + escapeHtml(p.short_name || formatPlayerName(p.full_name)) + '</span>';
    }).join('') + (players.length > 4 ? '<span class="today-inj-more">+' + (players.length - 4) + '</span>' : '');
  }

  const awayInj  = injChips(game.away);
  const homeInj  = injChips(game.home);
  const gameKey  = (game.away.team_id || '') + '-' + (game.home.team_id || '');

  return `
    <article class="today-game-card ${compact ? 'compact' : ''} ${statusClass}" data-game-key="${gameKey}">
      <div class="today-game-head">
        <span class="small-badge ${statusClass}">${escapeHtml(game.status_text)}</span>
        <span class="small-meta">${escapeHtml(game.game_label)}</span>
      </div>
      <div class="today-game-main">
        <div class="today-team-row">
          <div>
            <strong>${escapeHtml(game.away.abbreviation)}</strong>
            <small>${escapeHtml(awaySummary)}</small>
          </div>
          ${scoreLine}
          <div class="today-team-home">
            <strong>${escapeHtml(game.home.abbreviation)}</strong>
            <small>${escapeHtml(homeSummary)}</small>
          </div>
        </div>
        ${compact ? '' : `
        <div class="today-game-actions">
          <button class="mini-team-btn" type="button" data-team-id="${game.away.team_id}">Open ${escapeHtml(game.away.abbreviation)}</button>
          <button class="mini-team-btn" type="button" data-team-id="${game.home.team_id}">Open ${escapeHtml(game.home.abbreviation)}</button>
          <button class="today-inj-toggle-btn" type="button" data-game-key="${gameKey}">\uD83E\uDE79 Injuries</button>
        </div>
        <div class="today-inj-panel" id="inj-panel-${gameKey}" style="display:none">
          <div class="today-inj-team-row">
            <div class="today-inj-team-col">
              <span class="today-inj-team-label">${escapeHtml(game.away.abbreviation)} (Away)</span>
              <div class="today-inj-chips">${awayInj}</div>
            </div>
            <div class="today-inj-team-col">
              <span class="today-inj-team-label">${escapeHtml(game.home.abbreviation)} (Home)</span>
              <div class="today-inj-chips">${homeInj}</div>
            </div>
          </div>
        </div>`}
      </div>
    </article>
  `;
}

async function openTeamFromSlate(teamId) {
  if (!teamId) return;
  teamSelect.value = String(teamId);
  await loadRoster(Number(teamId));
  switchView('analyzer');
}

function bindSlateTeamButtons(root) {
  root.querySelectorAll('.mini-team-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      try {
        await openTeamFromSlate(btn.dataset.teamId);
      } catch (error) {
        console.error(error);
        alert(error.message || 'Failed to open team roster.');
      }
    });
  });

  // Injury toggle button per game card
  root.querySelectorAll('.today-inj-toggle-btn').forEach(btn => {
    btn.addEventListener('click', function() {
      const gameKey = btn.dataset.gameKey;
      const panel   = document.getElementById('inj-panel-' + gameKey);
      if (!panel) return;
      const open = panel.style.display !== 'none';
      panel.style.display = open ? 'none' : '';
      btn.textContent = open ? '\uD83E\uDE79 Injuries' : '\u25B2 Hide';
    });
  });
}

function renderTodayGames(payload) {
  latestTodayGamesPayload = payload;
  const games = payload.games || [];
  if (todayGamesMeta) {
    todayGamesMeta.textContent = payload.fallback_used
      ? `No games on ${payload.requested_date}. Showing next slate on ${payload.resolved_date}. ${payload.report_label ? `• Report ${payload.report_label}` : ''}`
      : `${games.length} game${games.length === 1 ? '' : 's'} on ${payload.resolved_date}${payload.report_label ? ` • Report ${payload.report_label}` : ''}`;
  }
  if (overviewTodayMeta) {
    overviewTodayMeta.textContent = payload.fallback_used
      ? `Next slate: ${payload.resolved_date}`
      : `${games.length} game${games.length === 1 ? '' : 's'} on ${payload.resolved_date}`;
  }

  if (!games.length) {
    const emptyHtml = `
      <div class="empty-state-panel compact today-game-empty">
        <div class="empty-icon">🗓️</div>
        <strong>No games on the active slate.</strong>
        <span>When the NBA schedule posts games, they will appear here with report context.</span>
      </div>`;
    if (todayGamesBoard) todayGamesBoard.innerHTML = emptyHtml;
    if (overviewTodayGames) overviewTodayGames.innerHTML = emptyHtml;
    return;
  }

  if (todayGamesBoard) {
    todayGamesBoard.innerHTML = games.map(game => buildTodayGameCard(game, false)).join('');
    bindSlateTeamButtons(todayGamesBoard);
  }

  if (overviewTodayGames) {
    overviewTodayGames.className = 'today-overview-list';
    overviewTodayGames.innerHTML = games.slice(0, 4).map(game => buildTodayGameCard(game, true)).join('');
  }
}

async function loadTodayGames(force = false) {
  if (latestTodayGamesPayload && !force) {
    renderTodayGames(latestTodayGamesPayload);
    return;
  }

  if (todayGamesMeta) todayGamesMeta.textContent = "Loading today's NBA slate...";
  if (overviewTodayMeta) overviewTodayMeta.textContent = "Fetching today's slate...";
  try {
    const response = await fetch('/api/todays-games');
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || "Failed to load today's games.");
    }
    renderTodayGames(payload);
  } catch (error) {
    console.error(error);
    const fallbackMessage = error.message || "Failed to load today's slate.";
    if (todayGamesBoard) {
      todayGamesBoard.innerHTML = `
        <div class="empty-state-panel compact today-game-empty">
          <div class="empty-icon">⚠️</div>
          <strong>Could not load today&#39;s games.</strong>
          <span>${escapeHtml(fallbackMessage)}</span>
        </div>`;
    }
    if (overviewTodayGames) {
      overviewTodayGames.innerHTML = `
        <div class="empty-state-panel compact today-game-empty">
          <div class="empty-icon">⚠️</div>
          <strong>Could not load the slate.</strong>
          <span>${escapeHtml(fallbackMessage)}</span>
        </div>`;
    }
  }
}

function renderBetFinderEmpty(message = 'Choose a team, set your prop line, then click Bet Finder.') {
  betFinderResults.className = 'bet-finder-state empty-state-panel compact';
  betFinderResults.innerHTML = `
    <div class="empty-icon">🎯</div>
    <strong>No bet finder results yet.</strong>
    <span>${escapeHtml(message)}</span>
  `;
}

function formatSignedMetric(value, suffix = '') {
  const num = Number(value);
  if (!Number.isFinite(num)) return '—';
  return `${num >= 0 ? '+' : ''}${num.toFixed(1)}${suffix}`;
}

function getFinderTone(hitRate) {
  if (hitRate >= 80) return 'elite';
  if (hitRate >= 70) return 'good';
  if (hitRate >= 60) return 'warm';
  return 'neutral';
}

function getFinderLabel(hitRate) {
  if (hitRate >= 80) return 'Elite';
  if (hitRate >= 70) return 'Strong';
  if (hitRate >= 60) return 'Playable';
  return 'Thin';
}

/* ── Mini sparkline SVG ─────────────────────────────────────────────── */
function buildSparklineSvg(values, line, width = 80, height = 28) {
  if (!values || values.length < 2) return '';
  const min = Math.min(...values, line) * 0.9;
  const max = Math.max(...values, line) * 1.1 || 1;
  const toX = (i) => (i / (values.length - 1)) * width;
  const toY = (v) => height - ((v - min) / (max - min)) * height;
  const pts = values.map((v, i) => `${toX(i).toFixed(1)},${toY(v).toFixed(1)}`).join(' ');
  const lineY = toY(line).toFixed(1);
  const dots = values.map((v, i) => {
    const hit = v >= line;
    const cx = toX(i).toFixed(1);
    const cy = toY(v).toFixed(1);
    return `<circle cx="${cx}" cy="${cy}" r="2.5" fill="${hit ? 'var(--good)' : 'var(--bad)'}" />`;
  }).join('');
  return `<svg viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" class="sparkline-svg" xmlns="http://www.w3.org/2000/svg">
    <line x1="0" y1="${lineY}" x2="${width}" y2="${lineY}" stroke="var(--warning)" stroke-width="1" stroke-dasharray="3,2" opacity="0.7"/>
    <polyline points="${pts}" fill="none" stroke="var(--accent)" stroke-width="1.5" stroke-linejoin="round"/>
    ${dots}
  </svg>`;
}

function renderBetFinderResults(payload) {
  const results = payload.results || [];

  if (!results.length) {
    renderBetFinderEmpty('No players on this roster met the sample requirement for the current prop line.');
    return;
  }

  betFinderMeta.textContent = `${payload.team.full_name} • ${getStatLabel(payload.stat)} ${payload.line} • Last ${payload.last_n} • Min ${payload.min_games} games`;
  betFinderResults.className = 'bet-finder-grid sportsbook-grid';
  betFinderResults.innerHTML = results.map((item, index) => {
    const tone = getFinderTone(item.hit_rate);
    const sideGuess = item.average >= payload.line ? 'OVER' : 'UNDER';
    const sideClass = sideGuess.toLowerCase();
    return `
      <button class="finder-card sportsbook-tile ${tone}" type="button"
        data-id="${item.player.id}"
        data-name="${escapeHtml(item.player.full_name)}"
        data-team-id="${item.player.team_id}"
        data-team-name="${escapeHtml(item.player.team_name || '')}"
        data-team-abbr="${escapeHtml(item.player.team_abbreviation || '')}"
        data-position="${escapeHtml(item.player.position || '')}"
        data-jersey="${escapeHtml(item.player.jersey || '')}">
        <div class="finder-ticket-head">
          <div class="finder-ticket-meta">
            <span class="finder-ticket-label"><span class="ticket-dot"></span>Ranked prop slip</span>
            <div class="finder-player">
              <img src="${getPlayerImage(item.player.id)}" alt="${escapeHtml(item.player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
              <div>
                <div class="finder-main-line">
                  <strong>${escapeHtml(item.player.full_name)}</strong>
                  <span class="finder-side-pill ${sideClass}">${sideGuess}</span>
                </div>
                <span>${escapeHtml(item.player.position || 'Position N/A')} • ${escapeHtml(item.player.team_abbreviation || '')}</span>
                <small class="market-slip-subtext">${getStatLabel(payload.stat)} line ${payload.line} • Last ${payload.last_n}</small>
              </div>
            </div>
          </div>
          <div class="finder-rank">#${index + 1}</div>
        </div>
        <div class="finder-chip-row">
          <span class="finder-chip">${item.hit_rate.toFixed(1)}% hit</span>
          <span class="finder-chip">${item.hit_count}/${item.games_count}</span>
          <span class="finder-chip">Avg ${item.average.toFixed(1)}</span>
          <span class="finder-chip ${item.avg_edge >= 0 ? 'positive' : 'negative'}">Edge ${formatSignedMetric(item.avg_edge)}</span>
        </div>
        <div class="finder-ticket-grid">
          <div class="finder-ticket-stat">
            <span>Last game</span>
            <strong>${item.last_value.toFixed(1)}</strong>
            <small>Most recent output</small>
          </div>
          <div class="finder-ticket-stat">
            <span>Over streak</span>
            <strong>${item.hit_streak}</strong>
            <small>Current run</small>
          </div>
          <div class="finder-ticket-stat">
            <span>Projection edge</span>
            <strong>${formatSignedMetric(item.avg_edge)}</strong>
            <small>Average vs line</small>
          </div>
          <div class="finder-ticket-stat">
            <span>Read</span>
            <strong>${getFinderLabel(item.hit_rate)}</strong>
            <small>Quick confidence</small>
          </div>
        </div>
        <div class="finder-ticket-footer">
          <span class="finder-badge ${tone}">${getFinderLabel(item.hit_rate)}</span>
          <span class="finder-odds-pill">Tap to open analyzer</span>
        </div>
      </button>
    `;
  }).join('');

  betFinderResults.querySelectorAll('.finder-card').forEach(card => {
    card.addEventListener('click', async () => {
      setSelectedPlayer({
        id: Number(card.dataset.id),
        full_name: card.dataset.name,
        is_active: true,
        team_abbreviation: card.dataset.teamAbbr,
        team_name: card.dataset.teamName,
        team_id: card.dataset.teamId ? Number(card.dataset.teamId) : null,
        position: card.dataset.position,
        jersey: card.dataset.jersey
      });
      await analyzePlayerProp();
    });
  });
}

async function runBetFinder() {
  switchView('betfinder');
  const teamId = teamSelect.value;
  if (!teamId) {
    alert('Please choose a team first.');
    return;
  }

  betFinderBtn.disabled = true;
  if (betFinderViewRunBtn) betFinderViewRunBtn.disabled = true;
  setStatus('Finding bets');
  betFinderMeta.textContent = 'Scanning the selected roster using your current prop settings...';
  betFinderResults.className = 'bet-finder-state empty-state-panel compact';
  betFinderResults.innerHTML = `
    <div class="empty-icon">⏳</div>
    <strong>Finding the best recent overs...</strong>
    <span>This checks the currently selected team roster so it stays faster and safer.</span>
  `;

  try {
    const params = new URLSearchParams({
      team_id: teamId,
      stat: selectedStat,
      line: lineInput.value,
      last_n: gamesSelect.value
    });

    const season = seasonInput.value.trim();
    if (season) params.set('season', season);

    const response = await fetch(`/api/bet-finder?${params.toString()}`);
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.detail || 'Failed to run Bet Finder.');
    }

    renderBetFinderResults(payload);
    setStatus('Bet Finder ready');
  } catch (error) {
    console.error(error);
    renderBetFinderEmpty(error.message || 'Bet Finder failed. Please try again.');
    setStatus('Error');
  } finally {
    betFinderBtn.disabled = false;
    if (betFinderViewRunBtn) betFinderViewRunBtn.disabled = false;
  }
}

function computeOverStreak(games) {
  let streak = 0;
  for (let i = games.length - 1; i >= 0; i -= 1) {
    if (games[i].hit) streak += 1;
    else break;
  }
  return streak;
}

function getChartTextColor() {
  return getComputedStyle(document.documentElement).getPropertyValue('--text').trim();
}

function getMutedColor() {
  return getComputedStyle(document.documentElement).getPropertyValue('--muted').trim();
}

function getCssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function getLeanClass(tone) {
  if (tone === 'good') return 'good';
  if (tone === 'bad') return 'bad';
  return 'neutral';
}

function getAvailabilityToneClass(tone) {
  if (tone === 'good') return 'good';
  if (tone === 'bad') return 'bad';
  if (tone === 'warning') return 'warning';
  return 'neutral';
}

function renderAvailabilityBadge(availability, compact = false) {
  if (!availability) return '';
  const toneClass = getAvailabilityToneClass(availability.tone);
  const tagClass = compact ? 'availability-tag compact' : 'availability-tag';
  const reason = escapeHtml(availability.reason || availability.note || 'No official note');
  const reportLabel = escapeHtml(availability.report_label || 'Latest report time unavailable');
  return `<span class="${tagClass} ${toneClass}" title="${reason} • ${reportLabel}">${escapeHtml(availability.status || 'Unknown')}</span>`;
}

function getMatchupTargets() {
  return [
    { badge: matchupLeanBadge, body: matchupBody },
    { badge: analyzerMatchupLeanBadge, body: analyzerMatchupBody }
  ].filter(target => target.badge && target.body);
}

function formatDelta(value) {
  return value > 0 ? `+${value}` : `${value}`;
}


async function hydrateAnalyzerFromPropSelection(prop) {
  if (!prop || !prop.player_id) return false;

  try {
    if (typeof ensureTeamsLoaded === 'function') {
      await ensureTeamsLoaded();
    }

    const teamId = Number(prop.team_id || 0) || null;
    let rosterMatch = null;
    if (teamId && typeof teamSelect !== 'undefined' && teamSelect) {
      if (String(teamSelect.value) !== String(teamId)) {
        teamSelect.value = String(teamId);
      }
      try {
        await loadRoster(Number(teamId));
      } catch (e) {
        console.warn('Roster load failed while hydrating analyzer from prop selection:', e);
      }
      rosterMatch = Array.isArray(rosterPlayers)
        ? rosterPlayers.find(p => String(p.id) === String(prop.player_id))
        : null;
    }

    const embeddedMatchup = prop.matchup && typeof prop.matchup === 'object' ? prop.matchup : null;
    const embeddedEnvironment = prop.environment && typeof prop.environment === 'object' ? prop.environment : null;
    const embeddedAvailability = prop.availability && typeof prop.availability === 'object' ? prop.availability : null;
    const embeddedNextGame = embeddedMatchup?.next_game || null;
    const overrideTeamId = Number(prop.opponent_team_id || embeddedNextGame?.opponent_team_id || 0) || null;

    if (typeof oppSelect !== 'undefined' && oppSelect) {
      if (overrideTeamId) {
        const hasOption = Array.from(oppSelect.options || []).some(opt => String(opt.value) === String(overrideTeamId));
        if (!hasOption) {
          const option = document.createElement('option');
          option.value = String(overrideTeamId);
          option.textContent = embeddedNextGame?.opponent_abbreviation || prop.opponent_abbreviation || `Team ${overrideTeamId}`;
          oppSelect.appendChild(option);
        }
        oppSelect.value = String(overrideTeamId);
        oppSelect.classList.add('override-active');
      } else {
        oppSelect.value = '';
        oppSelect.classList.remove('override-active');
      }
    }

    const selectedPayload = {
      ...(rosterMatch || {}),
      id: Number(prop.player_id),
      full_name: prop.player_name || rosterMatch?.full_name || '',
      is_active: rosterMatch?.is_active ?? true,
      team_id: teamId || rosterMatch?.team_id || null,
      team_abbreviation: prop.team_abbreviation || rosterMatch?.team_abbreviation || '',
      team_name: prop.team_name || rosterMatch?.team_name || '',
      position: prop.player_position || rosterMatch?.position || '',
      jersey: prop.player_jersey || rosterMatch?.jersey || '',
      availability: embeddedAvailability || rosterMatch?.availability || null,
      matchup: embeddedMatchup || null,
      environment: embeddedEnvironment || null,
    };

    if (typeof setSelectedPlayer === 'function') {
      setSelectedPlayer(selectedPayload);
    }
    if (typeof setActiveProp === 'function') setActiveProp(prop.stat);
    if (typeof lineInput !== 'undefined' && lineInput) lineInput.value = prop.line;
    if (typeof switchView === 'function') switchView('analyzer');
    if (typeof analyzePlayerProp === 'function') {
      await analyzePlayerProp({ preserveScroll: true });
    }
    return true;
  } catch (err) {
    console.warn('hydrateAnalyzerFromPropSelection failed:', err);
    return false;
  }
}

function renderMatchup(payload) {
  const nextGame = payload?.matchup?.next_game;
  const vsPosition = payload?.matchup?.vs_position;
  const availability = payload?.availability;
  const targets = getMatchupTargets();

  if (!nextGame && !vsPosition && !availability) {
    targets.forEach(({ badge, body }) => {
      badge.className = 'spotlight-pill neutral';
      badge.textContent = 'No matchup data';
      body.className = 'empty-state-panel compact matchup-empty';
      body.innerHTML = `
        <div class="empty-icon">🛡️</div>
        <strong>Matchup context unavailable.</strong>
        <span>We could not resolve the next opponent or position split for this player.</span>
      `;
    });
    return;
  }

  const leanTone = getLeanClass(vsPosition?.lean_tone);
  const leanText = vsPosition?.lean || (nextGame ? 'Upcoming game found' : 'Partial matchup');

  const nextGameLabel = nextGame
    ? `${nextGame.matchup_label} • ${nextGame.game_date ? formatNextGameDate(nextGame.game_date) : 'Date TBA'}${nextGame.game_time ? ` • ${nextGame.game_time}` : ''}`
    : 'Upcoming game unavailable';

  const isOverride = Boolean(nextGame?.is_override);
  const venueLabel = nextGame
    ? (isOverride ? 'Opponent override' : (nextGame.is_home ? 'Home game' : 'Away game'))
    : 'Venue unavailable';

  const overrideBannerHtml = isOverride
    ? `<div class="override-notice">📌 <strong>Manual opponent override:</strong> ${escapeHtml(nextGame?.opponent_name || nextGame?.opponent_abbreviation || 'Custom opponent')} — defense-vs-position and H2H stats reflect this selection, not the actual scheduled game.</div>`
    : '';

  let summaryText = 'Defense-vs-position data unavailable for this player and stat.';
  if (vsPosition) {
    summaryText = `${nextGame?.opponent_name || 'This opponent'} allows ${vsPosition.opponent_value.toFixed(2)} ${getStatLabel(vsPosition.stat).toLowerCase()} per player-game to ${vsPosition.position_label.toLowerCase()}, versus a league baseline of ${vsPosition.league_average.toFixed(2)} (${formatDelta(vsPosition.delta_pct)}%).`;
  }

  const bodyHtml = `
    ${overrideBannerHtml}
    <div class="matchup-grid">
      <article class="matchup-tile">
        <span class="small-label">Next game</span>
        <strong>${escapeHtml(nextGame?.opponent_name || 'Unavailable')}</strong>
        <small>${escapeHtml(nextGameLabel)}</small>
      </article>
      <article class="matchup-tile">
        <span class="small-label">Venue</span>
        <strong>${escapeHtml(venueLabel)}</strong>
        <small>${escapeHtml(nextGame?.player_team_abbreviation || 'NBA')}</small>
      </article>
      <article class="matchup-tile">
        <span class="small-label">Availability</span>
        <strong>${renderAvailabilityBadge(availability, true) || '—'}</strong>
        <small>${escapeHtml(availability?.reason || availability?.note || 'Official status unavailable')}</small>
      </article>
      <article class="matchup-tile">
        <span class="small-label">Vs position</span>
        <strong>${escapeHtml(vsPosition?.position_label || 'Unavailable')}</strong>
        <small>${escapeHtml(vsPosition ? getStatLabel(vsPosition.stat) : 'Need position data')}</small>
      </article>
      <article class="matchup-tile">
        <span class="small-label">Opponent allow rate</span>
        <strong>${vsPosition ? vsPosition.opponent_value.toFixed(2) : '—'}</strong>
        <small>${vsPosition ? `League avg ${vsPosition.league_average.toFixed(2)}` : 'No sample'}</small>
      </article>
      <article class="matchup-tile">
        <span class="small-label">Delta vs average</span>
        <strong class="${leanTone === 'good' ? 'match-good' : leanTone === 'bad' ? 'match-bad' : ''}">${vsPosition ? `${formatDelta(vsPosition.delta_pct)}%` : '—'}</strong>
        <small>${escapeHtml(vsPosition?.lean || 'Neutral')}</small>
      </article>
      <article class="matchup-tile">
        <span class="small-label">Sample</span>
        <strong>${vsPosition ? vsPosition.sample_gp.toFixed(0) : '—'}</strong>
        <small>${vsPosition ? `player-games vs ${escapeHtml(nextGame?.opponent_abbreviation || '')}` : 'No sample'}</small>
      </article>
    </div>
    <p class="matchup-summary">${escapeHtml(summaryText)}</p>
    ${availability ? `<p class="availability-footnote">${renderAvailabilityBadge(availability, true)} ${escapeHtml(availability.report_label || 'Latest report time unavailable')} • ${escapeHtml(availability.source || 'Official NBA injury report')}</p>` : ''}
  `;

  targets.forEach(({ badge, body }) => {
    badge.className = `spotlight-pill ${isOverride ? 'warning' : leanTone}`;
    badge.textContent = isOverride ? `📌 vs ${nextGame?.opponent_abbreviation || 'Override'}` : leanText;
    body.className = 'matchup-body';
    body.innerHTML = bodyHtml;
  });
}

function renderInterpretationPanels(payload) {
  const interpretation = payload?.interpretation || {};
  const opportunity = payload?.opportunity || {};
  const teamContext = payload?.team_context || {};
  const environment = payload?.environment || {};
  const toneMap = { good: 'good', warning: 'warning', bad: 'bad', neutral: 'neutral' };
  const interpretationToneClass = toneMap[interpretation.tone] || 'neutral';
  const opportunityToneClass = opportunity.minutes_trend === 'up' || opportunity.volume_trend === 'up'
    ? 'good'
    : (teamContext.impact_count ? 'warning' : 'neutral');
  const environmentToneClass = toneMap[environment.tone] || 'neutral';

  if (interpretationTone) {
    interpretationTone.className = `spotlight-pill ${interpretationToneClass}`;
    interpretationTone.textContent = interpretation.headline || 'Quick read';
  }
  if (opportunityTone) {
    opportunityTone.className = `spotlight-pill ${opportunityToneClass}`;
    opportunityTone.textContent = opportunity.minutes_label || 'Opportunity';
  }
  if (environmentTone) {
    environmentTone.className = `spotlight-pill ${environmentToneClass}`;
    environmentTone.textContent = environment.headline || 'Schedule spot';
  }

  if (interpretationBody) {
    const bullets = Array.isArray(interpretation.bullets) ? interpretation.bullets : [];
    interpretationBody.className = 'interpretation-body';
    interpretationBody.innerHTML = `
      <div class="insight-summary ${interpretationToneClass}">
        <span class="insight-summary-label">Quick read</span>
        <strong>${escapeHtml(interpretation.headline || 'Quick read unavailable')}</strong>
        <p>${escapeHtml(interpretation.summary || 'Analyze a player prop to generate a simple read.')}</p>
      </div>
      <ul class="insight-bullet-list compact-bullets">
        ${bullets.length ? bullets.map(item => `<li>${escapeHtml(item)}</li>`).join('') : '<li>Analyze a player prop to fill this section.</li>'}
      </ul>
    `;
  }

  if (opportunityBody) {
    const listedPlayers = (teamContext.players || []).map(item => `${formatPlayerName(item.name)} (${item.status})`);
    opportunityBody.className = 'opportunity-body';
    opportunityBody.innerHTML = `
      <div class="opportunity-chip-grid refined-opportunity-grid">
        <div class="opportunity-chip">
          <span class="small-label">Minutes</span>
          <strong>${Number(opportunity.minutes_last5 || 0).toFixed(1)}</strong>
          <small>${escapeHtml(opportunity.minutes_label || 'Minutes trend')}</small>
        </div>
        <div class="opportunity-chip">
          <span class="small-label">FGA</span>
          <strong>${Number(opportunity.fga_last5 || 0).toFixed(1)}</strong>
          <small>${escapeHtml(opportunity.volume_label || 'Shot volume trend')}</small>
        </div>
        <div class="opportunity-chip">
          <span class="small-label">3PA</span>
          <strong>${Number(opportunity.fg3a_last5 || 0).toFixed(1)}</strong>
          <small>Three-point volume</small>
        </div>
        <div class="opportunity-chip">
          <span class="small-label">FTA</span>
          <strong>${Number(opportunity.fta_last5 || 0).toFixed(1)}</strong>
          <small>Free-throw trips</small>
        </div>
      </div>
      <div class="opportunity-summary-wrap refined-opportunity-wrap">
        <div class="insight-summary neutral compact-summary">
          <span class="insight-summary-label">Opportunity read</span>
          <p class="opportunity-summary">${escapeHtml(opportunity.summary || 'Opportunity trends will appear after analysis.')}</p>
        </div>
        <div class="team-context-box ${teamContext.impact_count ? 'warning' : 'neutral'}">
          <strong>${escapeHtml(teamContext.headline || 'Team context')}</strong>
          <p>${escapeHtml(teamContext.summary || 'No team-availability context yet.')}</p>
          ${listedPlayers.length ? `<small>${escapeHtml(listedPlayers.join(' • '))}</small>` : '<small>No major same-team absences flagged on the latest report.</small>'}
        </div>
      </div>
    `;
  }

  if (environmentBody) {
    const restValue = Number.isInteger(environment.rest_days) ? `${environment.rest_days} day${environment.rest_days === 1 ? '' : 's'}` : 'Unknown';
    environmentBody.className = 'environment-body';
    environmentBody.innerHTML = `
      <div class="environment-chip-grid">
        <div class="environment-chip">
          <span class="small-label">Venue</span>
          <strong>${escapeHtml(environment.venue_label || 'TBD')}</strong>
          <small>${escapeHtml(environment.next_opponent ? `Next ${environment.next_opponent}` : 'Upcoming game')}</small>
        </div>
        <div class="environment-chip">
          <span class="small-label">Rest</span>
          <strong>${escapeHtml(restValue)}</strong>
          <small>${escapeHtml(environment.headline || 'Schedule spot')}</small>
        </div>
        <div class="environment-chip ${environment.is_back_to_back ? 'warning' : ''}">
          <span class="small-label">Back-to-back</span>
          <strong>${environment.is_back_to_back ? 'Yes' : 'No'}</strong>
          <small>${environment.is_back_to_back ? 'More fatigue risk' : 'No immediate fatigue flag'}</small>
        </div>
        <div class="environment-chip">
          <span class="small-label">Games in 7</span>
          <strong>${Number(environment.games_last7 || 0)}</strong>
          <small>${Number(environment.games_last7 || 0) >= 4 ? 'Busy recent schedule' : 'Normal recent load'}</small>
        </div>
      </div>
      <div class="insight-summary ${environmentToneClass} compact-summary">
        <span class="insight-summary-label">Schedule read</span>
        <strong>${escapeHtml(environment.headline || 'Schedule spot')}</strong>
        <p class="opportunity-summary">${escapeHtml(environment.summary || 'Schedule context will appear after analysis.')}</p>
      </div>
    `;
  }
}

function createLinePlugin(lineValue) {
  return {
    id: 'propLinePlugin',
    afterDatasetsDraw(chartInstance) {
      const { ctx, chartArea, scales: { y } } = chartInstance;
      const yValue = y.getPixelForValue(lineValue);
      if (!Number.isFinite(yValue)) return;

      ctx.save();
      ctx.beginPath();
      ctx.setLineDash([8, 6]);
      ctx.strokeStyle = getCssVar('--warning');
      ctx.lineWidth = 2;
      ctx.moveTo(chartArea.left, yValue);
      ctx.lineTo(chartArea.right, yValue);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = getCssVar('--warning');
      ctx.font = '700 12px Inter, Arial, sans-serif';
      ctx.fillText(`Line ${lineValue}`, chartArea.left + 8, yValue - 8);
      ctx.restore();
    }
  };
}

function renderChart(payload) {
  lastPayload = payload;
  const chartGames = payload.games || [];
  const labels = chartGames.map(game => game.game_date || game.matchup || '');
  const values = chartGames.map(game => game.value);
  const hits = chartGames.map(game => game.hit);
  const accent = getCssVar('--accent');
  const good = getCssVar('--good');
  const bad = getCssVar('--bad');
  const text = getChartTextColor();
  const muted = getMutedColor();
  const linePlugin = createLinePlugin(payload.line);

  if (chart) {
    chart.destroy();
  }

  chart = new Chart(document.getElementById('propsChart'), {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: getStatLabel(payload.stat),
        data: values,
        backgroundColor: hits.map(hit => hit ? `${good}CC` : `${bad}CC`),
        borderColor: hits.map(hit => hit ? good : bad),
        borderWidth: 1.4,
        borderRadius: 12,
        borderSkipped: false,
        hoverBackgroundColor: hits.map(hit => hit ? good : bad),
        maxBarThickness: 42
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 700 },
      layout: {
        padding: {
          bottom: 12
        }
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: document.body.classList.contains('light-theme')
            ? 'rgba(255,255,255,0.96)'
            : 'rgba(10,16,31,0.95)',
          titleColor: text,
          bodyColor: text,
          borderColor: `${accent}55`,
          borderWidth: 1,
          padding: 12,
          displayColors: false,
          callbacks: {
            title(items) {
              const item = items?.[0];
              const game = item ? chartGames[item.dataIndex] : null;
              return game?.game_date || item?.label || '';
            },
            label(context) {
              const value = context.raw;
              const game = chartGames[context.dataIndex] || null;
              const verdict = value >= payload.line ? 'Over ✓' : 'Under ✗';
              const lines = [`${getStatLabel(payload.stat)}: ${value} (${verdict})`];
              if (game?.matchup) lines.push(`vs ${game.matchup}`);
              if (game?.is_home === true) lines.push('Home game');
              else if (game?.is_home === false) lines.push('Away game');
              return lines;
            }
          }
        }
      },
      scales: {
        x: {
          ticks: {
            color: muted,
            autoSkip: false,
            minRotation: 35,
            maxRotation: 35,
            padding: 8,
            callback(value, index) {
              return labels[index] || '';
            }
          },
          grid: { display: false },
          border: { display: false }
        },
        y: {
          beginAtZero: true,
          ticks: { color: muted },
          grid: {
            color: document.body.classList.contains('light-theme')
              ? 'rgba(17,33,63,0.08)'
              : 'rgba(255,255,255,0.08)'
          },
          border: { display: false }
        }
      }
    },
    plugins: [linePlugin, valueLabelPlugin]
  });
}

function renderSummary(payload) {
  const streak = computeOverStreak(payload.games);
  const lastGame = payload.games[payload.games.length - 1];

  avgValue.textContent = payload.average.toFixed(1);
  hitRateValue.textContent = `${payload.hit_rate.toFixed(1)}%`;
  hitCountValue.textContent = `${payload.hit_count}/${payload.games_count}`;
  seasonValue.textContent = payload.season;
  streakValue.textContent = streak ? `${streak} straight` : '0';
  lastGameValue.textContent = lastGame ? `${lastGame.value}` : '—';

  const nextGame = payload.matchup?.next_game;
  const vsPosition = payload.matchup?.vs_position;
  const environment = payload.environment || {};
  const h2h = payload.h2h || {};
  chartTitle.textContent = `${payload.player.full_name} • ${getStatLabel(payload.stat)}`;
  chartSubtitle.textContent = nextGame
    ? `Line ${payload.line} across the last ${payload.last_n} games • Next ${nextGame.matchup_label}`
    : `Line ${payload.line} across the last ${payload.last_n} games.`;

  chartChips.innerHTML = `
    <span class="chart-chip">Avg ${payload.average.toFixed(1)}</span>
    <span class="chart-chip">${payload.hit_count}/${payload.games_count} overs</span>
    <span class="chart-chip">${payload.hit_rate.toFixed(1)}% hit rate</span>
    <span class="chart-chip">Season ${payload.season}</span>
    <span class="chart-chip">Current streak ${streak}</span>
    <span class="chart-chip">Stat ${payload.stat}</span>
    ${payload.availability ? `<span class="chart-chip">Status ${escapeHtml(payload.availability.status)}</span>` : ''}
    ${nextGame ? `<span class="chart-chip">Next ${escapeHtml(nextGame.matchup_label)}</span>` : ''}
    ${vsPosition ? `<span class="chart-chip">Vs ${escapeHtml(vsPosition.position_label)} ${formatDelta(vsPosition.delta_pct)}%</span>` : ''}
    ${h2h.games_count ? `<span class="chart-chip">H2H ${h2h.hit_count}/${h2h.games_count} vs ${escapeHtml(h2h.opponent_abbreviation || h2h.opponent_name || 'opponent')}</span>` : ''}
  `;

  renderMatchup(payload);
  renderInterpretationPanels(payload);
}

const gamesTableHeadRow = document.querySelector('#gameLogContent thead tr');

function formatGameLogNumber(value) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? num.toFixed(1) : '0.0';
}

function getGameLogColumns(stat) {
  const comboParts = getComboStatParts(stat);
  const columns = [
    { key: 'date', label: 'Date' },
    { key: 'matchup', label: 'Matchup' },
  ];

  if (comboParts.length > 1) {
    comboParts.forEach(part => {
      columns.push({ key: part, label: getStatLabel(part), tone: 'value' });
    });
    columns.push({ key: 'value', label: 'Total', tone: 'total' });
  } else {
    columns.push({ key: 'value', label: getStatLabel(stat), tone: 'total' });
  }

  columns.push(
    { key: 'minutes', label: 'MIN' },
    { key: 'fga', label: 'FGA' },
    { key: 'fg3a', label: '3PA' },
    { key: 'fta', label: 'FTA' },
    { key: 'result', label: 'Result' },
  );

  return columns;
}

function renderGameLogHeader(stat) {
  if (!gamesTableHeadRow) return;
  const columns = getGameLogColumns(stat);
  gamesTableHeadRow.innerHTML = columns.map(column => `<th>${escapeHtml(column.label)}</th>`).join('');
}

function getGameComponentValue(game, statKey) {
  if (!game) return 0;
  if (statKey === 'PTS') return Number(game?.components?.PTS ?? game.pts ?? 0);
  if (statKey === 'REB') return Number(game?.components?.REB ?? game.reb ?? 0);
  if (statKey === 'AST') return Number(game?.components?.AST ?? game.ast ?? 0);
  return 0;
}

function buildGameLogRowsMarkup(games, emptyTitle, emptySubtitle, stat = selectedStat) {
  const columns = getGameLogColumns(stat);
  if (!games || !games.length) {
    return `
      <tr>
        <td colspan="${columns.length}">
          <div class="empty-state-panel compact">
            <div class="empty-icon">📊</div>
            <strong>${escapeHtml(emptyTitle)}</strong>
            <span>${escapeHtml(emptySubtitle)}</span>
          </div>
        </td>
      </tr>
    `;
  }

  return games.slice().reverse().map(game => {
    const tds = columns.map(column => {
      if (column.key === 'date') return `<td>${escapeHtml(game.game_date || '—')}</td>`;
      if (column.key === 'matchup') return `<td>${escapeHtml(game.matchup || '—')}</td>`;
      if (column.key === 'value') return `<td class="${game.hit ? 'hit-value' : 'miss-value'}">${formatGameLogNumber(game.value)}</td>`;
      if (column.key === 'minutes') return `<td>${formatGameLogNumber(game.minutes)}</td>`;
      if (column.key === 'fga') return `<td>${formatGameLogNumber(game.fga)}</td>`;
      if (column.key === 'fg3a') return `<td>${formatGameLogNumber(game.fg3a)}</td>`;
      if (column.key === 'fta') return `<td>${formatGameLogNumber(game.fta)}</td>`;
      if (column.key === 'result') return `<td><span class="result-badge ${game.hit ? 'hit' : 'miss'}">${game.hit ? 'Hit' : 'Miss'}</span></td>`;
      return `<td>${formatGameLogNumber(getGameComponentValue(game, column.key))}</td>`;
    }).join('');

    return `<tr>${tds}</tr>`;
  }).join('');
}

function setActiveGameLogTab(view) {
  activeGameLogView = view;
  recentLogTab?.classList.toggle('active', view === 'recent');
  h2hLogTab?.classList.toggle('active', view === 'h2h');
}

function renderGameLogTab(view = activeGameLogView) {
  currentGameLogPayload = currentGameLogPayload || null;
  const h2h = currentGameLogPayload?.h2h || {};
  if (h2hLogTab) {
    const hasOpponent = Boolean(h2h.opponent_abbreviation || h2h.opponent_name);
    h2hLogTab.disabled = !hasOpponent;
    h2hLogTab.title = hasOpponent ? 'Show current-season results vs the next opponent' : 'Next opponent not available yet';
  }
  if (view === 'h2h' && !(h2h.opponent_abbreviation || h2h.opponent_name)) {
    view = 'recent';
  }
  setActiveGameLogTab(view);

  if (!currentGameLogPayload) {
    gameLogMeta.textContent = 'Hit / miss tags included';
    renderGameLogHeader(currentGameLogPayload?.stat || selectedStat);
    gamesTableBody.innerHTML = buildGameLogRowsMarkup([], 'No data yet.', 'Analyze a player prop to fill the game log.', currentGameLogPayload?.stat || selectedStat);
    return;
  }

  if (view === 'h2h') {
    const h2h = currentGameLogPayload.h2h || {};
    const oppLabel = h2h.opponent_abbreviation || h2h.opponent_name || 'opponent';
    const games = h2h.games || [];
    if (games.length) {
      gameLogMeta.textContent = `${h2h.hit_count}/${h2h.games_count} overs • Avg ${Number(h2h.average || 0).toFixed(1)} vs ${oppLabel} • Minutes and attempts included`;
      renderGameLogHeader(currentGameLogPayload?.stat || selectedStat);
      gamesTableBody.innerHTML = buildGameLogRowsMarkup(games, `No H2H games vs ${oppLabel} yet.`, 'No current-season meetings found for this next opponent.', currentGameLogPayload?.stat || selectedStat);
    } else {
      gameLogMeta.textContent = `H2H vs ${oppLabel}`;
      renderGameLogHeader(currentGameLogPayload?.stat || selectedStat);
      gamesTableBody.innerHTML = buildGameLogRowsMarkup([], `No H2H games vs ${oppLabel} yet.`, 'No current-season meetings found for this next opponent.', currentGameLogPayload?.stat || selectedStat);
    }
    return;
  }

  gameLogMeta.textContent = `Last ${currentGameLogPayload.last_n} games • Value, minutes, and attempts`;
  renderGameLogHeader(currentGameLogPayload?.stat || selectedStat);
  gamesTableBody.innerHTML = buildGameLogRowsMarkup(currentGameLogPayload.games || [], 'No data yet.', 'Analyze a player prop to fill the game log.', currentGameLogPayload?.stat || selectedStat);
}

function renderTable(payload) {
  currentGameLogPayload = payload;
  renderGameLogTab(activeGameLogView);
}

function getMarketTemplate() {
  return [
    'player_name,stat,line,over_odds,under_odds',
    'Nikola Jokic,REB,12.5,2.00,1.73',
    'Aaron Gordon,REB,5.5,1.90,1.80',
    'Jamal Murray,REB,4.5,1.95,1.75',
    'Christian Braun,REB,4.5,1.85,1.85',
    'Deni Avdija,REB,6.5,1.75,1.95',
    'Toumani Camara,REB,4.5,2.05,1.68',
    'Donovan Clingan,REB,11.5,1.85,1.85',
    '',
    'Correct the spelling of the names, you can use the internet to double check it.'
  ].join('\n');
}

function parseMarketText(rawText) {
  const lines = String(rawText || '')
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .filter(line => !line.startsWith('#'));

  if (!lines.length) {
    throw new Error('Paste at least one market row first.');
  }

  let dataLines = lines;
  const first = lines[0].toLowerCase();
  if (first.includes('player_name') || first.includes('player,')) {
    dataLines = lines.slice(1);
  }

  const rows = dataLines.map((line, index) => {
    const parts = line.split(',').map(part => part.trim());
    if (parts.length < 5) {
      throw new Error(`Row ${index + 1} is incomplete. Use: player_name,stat,line,over_odds,under_odds`);
    }

    const [player_name, stat, lineValue, overOdds, underOdds, team = '', opponent = ''] = parts;
    if (!player_name || !stat || !lineValue || !overOdds || !underOdds) {
      throw new Error(`Row ${index + 1} has missing required values.`);
    }

    return {
      player_name,
      stat: stat.toUpperCase(),
      line: Number(lineValue),
      over_odds: Number(overOdds),
      under_odds: Number(underOdds),
      team,
      opponent
    };
  });

  rows.forEach((row, index) => {
    if ([row.line, row.over_odds, row.under_odds].some(value => Number.isNaN(value))) {
      throw new Error(`Row ${index + 1} has a non-numeric line or odds value.`);
    }
  });

  return rows;
}


function setOddsApiStatus(message, tone = 'neutral') {
  if (!oddsApiStatus) return;
  oddsApiStatus.textContent = message;
  oddsApiStatus.dataset.tone = tone;
}

function setOddsQuotaMeta(quota) {
  if (!oddsQuotaMeta) return;
  if (!quota) {
    oddsQuotaMeta.textContent = 'Credits used will appear here after an Odds API call.';
    return;
  }
  oddsQuotaMeta.textContent = `Credits used: ${quota.used ?? '—'} • Remaining: ${quota.remaining ?? '—'} • Last call cost: ${quota.last ?? '—'}`;
}

function setOddsApiKeyMeta(apiKey) {
  if (!oddsApiKeyMeta) return;
  const raw = String(apiKey || '').trim();
  oddsApiKeyMeta.textContent = raw ? `API key used: ${raw}` : 'API key used: none';
}

function getOddsApiSettings() {
  const settings = {
    api_key: oddsApiKeyInput?.value?.trim() || '',
    sport: oddsSportSelect?.value || 'basketball_nba',
    regions: oddsRegionsInput?.value?.trim() || 'us',
    odds_format: oddsFormatSelect?.value || 'decimal',
    markets: oddsMarketsInput?.value?.trim() || '',
  };
  return settings;
}

function persistOddsApiSettings() {
  const settings = getOddsApiSettings();
  if (settings.api_key) localStorage.setItem(ODDS_API_KEY_STORAGE, settings.api_key);
  localStorage.setItem(ODDS_API_SETTINGS_STORAGE, JSON.stringify({
    sport: settings.sport,
    regions: settings.regions,
    odds_format: settings.odds_format,
    markets: settings.markets,
  }));
  setOddsApiKeyMeta(settings.api_key);
}

function loadStoredOddsApiSettings() {
  const storedKey = localStorage.getItem(ODDS_API_KEY_STORAGE) || '';
  let storedSettings = {};
  try {
    storedSettings = JSON.parse(localStorage.getItem(ODDS_API_SETTINGS_STORAGE) || '{}');
  } catch {
    storedSettings = {};
  }
  if (oddsApiKeyInput) oddsApiKeyInput.value = storedKey;
  if (oddsSportSelect && storedSettings.sport) oddsSportSelect.value = storedSettings.sport;
  if (oddsRegionsInput && storedSettings.regions) oddsRegionsInput.value = storedSettings.regions;
  if (oddsFormatSelect && storedSettings.odds_format) oddsFormatSelect.value = storedSettings.odds_format;
  if (oddsMarketsInput && storedSettings.markets) oddsMarketsInput.value = storedSettings.markets;
  setOddsApiKeyMeta(storedKey);
}

function renderOddsEvents(events) {
  if (!oddsEventSelect) return;
  oddsEventSelect.innerHTML = '';
  if (!Array.isArray(events) || !events.length) {
    oddsEventSelect.innerHTML = '<option value="">No events found</option>';
    return;
  }
  oddsEventSelect.innerHTML = '<option value="">Select an event</option>' + events.map(event => {
    const timeText = event?.commence_time ? formatPHT(event.commence_time) : 'Time TBA';
    return `<option value="${escapeHtml(event.id)}">${escapeHtml(event.away_team || '')} @ ${escapeHtml(event.home_team || '')} • ${escapeHtml(timeText)}</option>`;
  }).join('');
}

async function loadOddsEvents() {
  const settings = getOddsApiSettings();
  if (!settings.api_key) {
    alert('Please enter an Odds API key first.');
    return;
  }
  persistOddsApiSettings();
  setOddsApiStatus('Loading events...', 'working');
  setOddsQuotaMeta(null);
  if (oddsLoadEventsBtn) oddsLoadEventsBtn.disabled = true;
  try {
    const response = await fetch('/api/odds/events', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: settings.api_key, sport: settings.sport })
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || payload.error || 'Failed to load events.');
    renderOddsEvents(payload.events || []);
    setOddsQuotaMeta(payload.quota);
    setOddsApiKeyMeta(payload.api_key_used || settings.api_key);
    setOddsApiStatus(`Loaded ${(payload.events || []).length} event(s)`, 'good');
  } catch (error) {
    console.error(error);
    setOddsApiStatus('Load failed', 'bad');
    alert(error.message || 'Failed to load Odds API events.');
  } finally {
    if (oddsLoadEventsBtn) oddsLoadEventsBtn.disabled = false;
  }
}

function buildImportedRowsText(importRows) {
  return (importRows || []).map(row => row.csv_row).join('\n');
}

async function importOddsPropsAndScan() {
  const settings = getOddsApiSettings();
  const eventId = oddsEventSelect?.value || '';
  if (!settings.api_key) {
    alert('Please enter an Odds API key first.');
    return;
  }
  if (!eventId) {
    alert('Please load events and choose one event first.');
    return;
  }
  persistOddsApiSettings();
  setOddsApiStatus('Importing props...', 'working');
  if (oddsImportScanBtn) oddsImportScanBtn.disabled = true;
  try {
    const response = await fetch('/api/odds/player-props-import', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        api_key: settings.api_key,
        sport: settings.sport,
        event_id: eventId,
        regions: settings.regions,
        odds_format: settings.odds_format,
        markets: settings.markets,
      })
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || payload.error || 'Failed to import player props.');
    setOddsQuotaMeta(payload.quota);
    setOddsApiKeyMeta(payload.api_key_used || settings.api_key);
    const csvRows = Array.isArray(payload.csv_rows) ? payload.csv_rows : [];
    if (!csvRows.length) {
      setOddsApiStatus('No props found', 'bad');
      alert('No complete Over/Under player prop rows were found for this event and market selection.');
      return;
    }
    marketTextarea.value = csvRows.join('\n');
    marketMeta.textContent = `${csvRows.length} imported row(s) • ${payload.event?.away_team || ''} @ ${payload.event?.home_team || ''} • Credits used ${payload.quota?.used ?? '—'} • Remaining ${payload.quota?.remaining ?? '—'}`;
    setOddsApiStatus(`Imported ${csvRows.length} prop row(s)`, 'good');
    await runMarketScan();
  } catch (error) {
    console.error(error);
    setOddsApiStatus('Import failed', 'bad');
    alert(error.message || 'Failed to import Odds API props.');
  } finally {
    if (oddsImportScanBtn) oddsImportScanBtn.disabled = false;
  }
}

function getConfidenceTone(confidence) {
  if (confidence === 'A') return 'elite';
  if (confidence === 'B') return 'good';
  if (confidence === 'C') return 'warm';
  if (confidence === 'X') return 'out';
  return '';
}

function renderMarketEmpty(message = 'Paste your board using the template, then click Analyze Board.') {
  marketMeta.textContent = 'Paste rows using the template below. Team and opponent are detected automatically from the player.';
  marketResults.className = 'bet-finder-state empty-state-panel compact';
  marketResults.innerHTML = `
    <div class="empty-icon">🧾</div>
    <strong>No market scan yet.</strong>
    <span>${escapeHtml(message)}</span>
  `;
}

async function focusMarketPlayer(item) {
  if (!item?.player?.id) return;

  const teamId = item.player.team_id;
  if (teamId && String(teamSelect.value) !== String(teamId)) {
    teamSelect.value = String(teamId);
    try {
      await loadRoster(teamId);
    } catch (rosterErr) {
      // Roster load failed (NBA API throttle/timeout) — continue anyway.
      // The player is already known from the market scan result so analysis
      // can still run; the roster panel will just be empty/stale.
      console.warn('Roster load failed, continuing with player from market scan:', rosterErr.message || rosterErr);
    }
  }

  setSelectedPlayer({
    id: item.player.id,
    full_name: item.player.full_name,
    is_active: true,
    team_abbreviation: item.player.team_abbreviation || '',
    team_name: item.player.team_name || '',
    team_id: item.player.team_id || null,
    position: item.player.position || '',
    jersey: item.player.jersey || '',
    availability: item.analysis?.availability || item.availability || null,
    matchup: item.analysis?.matchup || item.matchup || null,
    environment: item.analysis?.environment || item.environment || null
  });

  setActiveProp(item.market.stat);
  lineInput.value = item.market.line;
  switchView('analyzer');
  await analyzePlayerProp();
}

function renderMarketResults(payload) {
  currentMarketResultsPayload = payload;
  const sortKey = currentMarketSort || 'best_ev';
  const filterKey = currentMarketFilter || 'all';
  const results = [...filterMarketRows(payload.results || [], filterKey)]
    .filter(item => passesExpertFilters(item))
    .sort((a, b) => compareMarketRows(a, b, sortKey, currentMarketSortDirection));

  if (!results.length) {
    renderMarketEmpty('No rows produced a usable result. Check names and try again.');
    return;
  }

  if (marketSortSelect) marketSortSelect.value = sortKey;
  renderMarketFilterChips();
  renderMarketExpertFilterChips();
  marketMeta.textContent = `${results.length} props scanned • ${getMarketSortLabel(sortKey)} • ${getMarketFilterLabel(filterKey)} • ${getExpertFilterSummary()}`;

  const featured = results.slice(0, 6);
  marketResults.className = 'market-results-shell';
  marketResults.innerHTML = `
    <div class="market-slips-header">
      <div>
        <h3>Ranked prop slips</h3>
        <p>The highest-value board looks are surfaced as sportsbook-style tiles before the full table.</p>
      </div>
      <span class="spotlight-pill neutral">Top ${featured.length} featured</span>
    </div>
    <div class="market-slips-grid">
      ${featured.map((item, index) => {
        const tone = getConfidenceTone(item.best_bet.confidence);
        const availability = item.availability || item.analysis?.availability || { status: 'Unknown', tone: 'neutral', reason: 'No report found', note: '' };
        const matchupData = item.analysis?.matchup || item.matchup || {};
        const matchupLean = matchupData?.vs_position?.lean || 'No matchup';
        const matchupDetail = matchupData?.next_game?.matchup_label || 'No next opponent';
        const expertAngles = getMarketExpertAngles(item).slice(0, 3);
        const side = (item.best_bet.display_side || item.best_bet.side || 'Lean').toUpperCase();
        const sideClass = side.toLowerCase().includes('under') ? 'under' : 'over';
        return `
          <article class="market-slip-card" data-index="${index}">
            <div class="market-slip-head">
              <div class="market-slip-player">
                <img src="${getPlayerImage(item.player.id)}" alt="${escapeHtml(item.player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
                <div>
                  <strong>${escapeHtml(item.player.full_name)}</strong>
                  <span>${escapeHtml(item.player.team)} vs ${escapeHtml(item.player.opponent || '')}</span>
                  <small>${renderAvailabilityBadge(availability, true)} ${escapeHtml(availability.reason || availability.note || 'No official note')}</small>
                </div>
              </div>
              <div class="market-slip-rank">#${index + 1}</div>
            </div>
            <div class="market-slip-body">
              <div class="market-slip-line">
                <strong>${escapeHtml(item.market.stat)} ${item.market.line}</strong>
                <span class="market-slip-side ${sideClass}">${escapeHtml(side)}</span>
              </div>
              <span class="market-slip-confidence finder-badge ${tone}">${escapeHtml(item.best_bet.confidence || 'Neutral')} ${item.best_bet.confidence_score ? escapeHtml(item.best_bet.confidence_score) : ''}</span>
              <p class="market-slip-summary">${escapeHtml(item.best_bet.user_read || item.best_bet.confidence_summary || 'Confidence summary unavailable.')}</p>
              <div class="market-slip-stats">
                <div class="market-slip-stat">
                  <span>Edge</span>
                  <strong class="${(item.best_bet.edge || 0) >= 0 ? 'hit-value' : 'miss-value'}">${item.best_bet.edge ?? '—'}%</strong>
                  <small>Model advantage</small>
                </div>
                <div class="market-slip-stat">
                  <span>EV</span>
                  <strong class="${(item.best_bet.ev || 0) >= 0 ? 'hit-value' : 'miss-value'}">${item.best_bet.ev ?? '—'}%</strong>
                  <small>Expected value</small>
                </div>
                <div class="market-slip-stat">
                  <span>Hit rate</span>
                  <strong>${item.analysis.hit_rate}%</strong>
                  <small>${item.analysis.hit_count}/${item.analysis.games_count}</small>
                </div>
                <div class="market-slip-stat">
                  <span>Average</span>
                  <strong>${item.analysis.average}</strong>
                  <small>Recent sample</small>
                </div>
              </div>
            </div>
            <div class="market-slip-footer">
              <div class="market-slip-angle-row">
                ${expertAngles.length ? expertAngles.map(angle => `<span class="market-angle-badge ${angle.tone ? `tone-${angle.tone}` : ''} ${angle.kind ? `kind-${angle.kind}` : ''}">${escapeHtml(angle.label)}</span>`).join('') : '<span class="market-angle-empty">No expert angle</span>'}
              </div>
              <div class="market-matchup-cell">
                <strong>${escapeHtml(matchupLean)}</strong>
                <small>${escapeHtml(matchupDetail)}</small>
              </div>
            </div>
          </article>
        `;
      }).join('')}
    </div>
    <div class="market-results-table-wrap">
      <table class="market-results-table">
        <thead>
          <tr>
            <th>Player</th>
            <th>Availability</th>
            <th>Prop</th>
            <th>Best side</th>
            <th aria-sort="${getMarketAriaSort('best_edge')}"><button class="market-sort-header" data-sort-key="best_edge" type="button" title="Sort by best edge"><span>Edge</span><span class="market-sort-arrow">${getMarketSortArrow('best_edge')}</span></button></th>
            <th aria-sort="${getMarketAriaSort('best_ev')}"><button class="market-sort-header" data-sort-key="best_ev" type="button" title="Sort by best EV"><span>EV</span><span class="market-sort-arrow">${getMarketSortArrow('best_ev')}</span></button></th>
            <th>Model %</th>
            <th>Implied %</th>
            <th aria-sort="${getMarketAriaSort('highest_hit_rate')}"><button class="market-sort-header" data-sort-key="highest_hit_rate" type="button" title="Sort by highest win rate"><span>Last ${payload.last_n}</span><span class="market-sort-arrow">${getMarketSortArrow('highest_hit_rate')}</span></button></th>
            <th>Average</th>
            <th>Expert angles</th>
            <th>Matchup</th>
          </tr>
        </thead>
        <tbody>
          ${results.map((item, index) => {
            const tone = getConfidenceTone(item.best_bet.confidence);
            const availability = item.availability || item.analysis?.availability || { status: 'Unknown', tone: 'neutral', reason: 'No report found', note: '' };
            const matchupData = item.analysis?.matchup || item.matchup || {};
            const matchupLean = matchupData?.vs_position?.lean || 'No matchup';
            const matchupDetail = matchupData?.next_game?.matchup_label || 'No next opponent';
            const expertAngles = getMarketExpertAngles(item);
            return `
              <tr class="market-row" data-index="${index}">
                <td>
                  <div class="market-player-cell">
                    <img src="${getPlayerImage(item.player.id)}" alt="${escapeHtml(item.player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
                    <div>
                      <strong>${escapeHtml(item.player.full_name)}</strong>
                      <small>${escapeHtml(item.player.team)} vs ${escapeHtml(item.player.opponent || '')}</small>
                    </div>
                  </div>
                </td>
                <td><div class="market-availability-cell">${renderAvailabilityBadge(availability, true)}<small>${escapeHtml(availability.reason || availability.note || 'No official note')}</small></div></td>
                <td>${escapeHtml(item.market.stat)} ${item.market.line}</td>
                <td>
                  <div class="market-confidence-cell">
                    <span class="finder-badge ${tone}">${item.best_bet.display_side || item.best_bet.side} • ${item.best_bet.confidence}${item.best_bet.confidence_score ? ` ${item.best_bet.confidence_score}` : ''}</span>
                    <small>${escapeHtml(item.best_bet.user_read || item.best_bet.confidence_summary || 'Confidence summary unavailable.')}</small>
                  </div>
                </td>
                <td class="${(item.best_bet.edge || 0) >= 0 ? 'hit-value' : 'miss-value'}">${item.best_bet.edge ?? '—'}%</td>
                <td class="${(item.best_bet.ev || 0) >= 0 ? 'hit-value' : 'miss-value'}">${item.best_bet.ev ?? '—'}%</td>
                <td>${item.best_bet.model_probability ?? '—'}%</td>
                <td>${item.best_bet.implied_probability ?? '—'}%</td>
                <td>${item.analysis.hit_count}/${item.analysis.games_count} (${item.analysis.hit_rate}%)</td>
                <td>${item.analysis.average}</td>
                <td>
                  <div class="market-angle-cell">
                    ${expertAngles.length ? expertAngles.map(angle => `<span class="market-angle-badge ${angle.tone ? `tone-${angle.tone}` : ''} ${angle.kind ? `kind-${angle.kind}` : ''}">${escapeHtml(angle.label)}</span>`).join('') : '<span class="market-angle-empty">No expert angle</span>'}
                  </div>
                </td>
                <td>
                  <div class="market-matchup-cell">
                    <strong>${escapeHtml(matchupLean)}</strong>
                    <small>${escapeHtml(matchupDetail)}</small>
                  </div>
                </td>
              </tr>
            `;
          }).join('')}
        </tbody>
      </table>
    </div>
  `;

  marketResults.querySelectorAll('.market-row, .market-slip-card').forEach(row => {
    row.addEventListener('click', async () => {
      const item = results[Number(row.dataset.index)];
      if (!item) return;
      try {
        setStatus('Loading market pick');
        await focusMarketPlayer(item);
        setStatus('Ready');
      } catch (error) {
        console.error(error);
        const msg = error.message || '';
        if (!msg.toLowerCase().includes('roster')) {
          alert(msg || 'Failed to open that market pick.');
        }
        setStatus('Error');
      }
    });
  });

  marketResults.querySelectorAll('.market-sort-header').forEach(header => {
    header.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      toggleMarketSort(header.dataset.sortKey || 'best_ev');
      if (marketSortSelect) marketSortSelect.value = currentMarketSort;
      renderMarketResults(currentMarketResultsPayload || payload);
    });
  });
}


async function runMarketScan() {
  switchView('market');
  let rows;
  try {
    rows = parseMarketText(marketTextarea.value);
  } catch (error) {
    alert(error.message);
    return;
  }

  setStatus('Scanning market');
  marketScanBtn.disabled = true;
  marketResults.className = 'bet-finder-state empty-state-panel compact';
  marketResults.innerHTML = `
    <div class="empty-icon">⏳</div>
    <strong>Scanning your board...</strong>
    <span>Comparing hit rate, implied odds, EV, and matchup context.</span>
  `;

  try {
    const response = await fetch('/api/market-scan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        rows,
        last_n: Number(gamesSelect.value),
        season: seasonInput.value.trim() || undefined
      })
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || 'Market scan failed.');
    }
    renderMarketResults(payload);
    saveLatestMarketResults(payload);
    renderOverviewBestBets();
    setStatus('Ready');
  } catch (error) {
    console.error(error);
    renderMarketEmpty(error.message || 'Market scan failed.');
    setStatus('Error');
  } finally {
    marketScanBtn.disabled = false;
  }
}

function resetDashboardForNoSelection() {
  avgValue.textContent = '—';
  hitRateValue.textContent = '—';
  hitCountValue.textContent = '—';
  seasonValue.textContent = '—';
  streakValue.textContent = '—';
  lastGameValue.textContent = '—';
  chartTitle.textContent = 'Waiting for a player selection';
  chartSubtitle.textContent = 'Choose a player and analyze a prop to populate the chart.';
  chartChips.innerHTML = '<span class="chart-chip">Waiting for data</span>';
  currentGameLogPayload = null;
  activeGameLogView = 'recent';
  if (gameLogMeta) gameLogMeta.textContent = 'Hit / miss tags included';
  renderGameLogTab('recent');
  getMatchupTargets().forEach(({ badge, body }) => {
    badge.className = 'spotlight-pill neutral';
    badge.textContent = 'Waiting for analysis';
    body.className = 'empty-state-panel compact matchup-empty';
    body.innerHTML = `
      <div class="empty-icon">🛡️</div>
      <strong>No matchup loaded yet.</strong>
      <span>Analyze a player prop to load the next opponent and defense-vs-position read.</span>
    `;
  });

  if (environmentTone) {
    environmentTone.className = 'spotlight-pill neutral';
    environmentTone.textContent = 'Waiting for analysis';
  }
  if (environmentBody) {
    environmentBody.className = 'environment-body';
    environmentBody.innerHTML = `
      <div class="environment-chip-grid">
        <div class="environment-chip"><span class="small-label">Venue</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="environment-chip"><span class="small-label">Rest</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="environment-chip"><span class="small-label">Back-to-back</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="environment-chip"><span class="small-label">Games in 7</span><strong>—</strong><small>Waiting for analysis</small></div>
      </div>
      <div class="insight-summary neutral compact-summary">
        <span class="insight-summary-label">Schedule read</span>
        <p class="opportunity-summary">Analyze a player prop to see rest, back-to-back risk, and the upcoming spot.</p>
      </div>
    `;
  }

  if (chart) {
    chart.destroy();
    chart = null;
  }
  lastPayload = null;
}

async function analyzePlayerProp(options = {}) {
  switchView('analyzer', { scroll: !options.preserveScroll });
  if (!selectedPlayer) {
    alert('Please select a player first.');
    return;
  }

  setStatus('Loading');
  analyzeBtn.disabled = true;

  try {
    const params = new URLSearchParams({
      player_id: selectedPlayer.id,
      stat: selectedStat,
      line: lineInput.value,
      last_n: gamesSelect.value
    });

    if (selectedPlayer.team_id) params.set('team_id', selectedPlayer.team_id);
    if (selectedPlayer.position) params.set('player_position', selectedPlayer.position);

    const season = seasonInput.value.trim();
    if (season) params.set('season', season);

    const overrideOppId = oppSelect?.value;
    if (overrideOppId) params.set('override_opponent_id', overrideOppId);

    const response = await fetch(`/api/player-prop?${params.toString()}`);
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.detail || 'Failed to analyze player prop.');
    }

    selectedPlayer = {
      ...selectedPlayer,
      team_id: payload.player.team_id || selectedPlayer.team_id,
      position: payload.player.position || selectedPlayer.position,
      availability: payload.availability || null,
      matchup: payload.matchup || null,
      environment: payload.environment || null
    };
    renderSelectedPlayer();
    renderSummary(payload);
    renderChart(payload);
    renderTable(payload);
    setStatus('Ready');
  } catch (error) {
    console.error(error);
    alert(error.message);
    setStatus('Error');
  } finally {
    analyzeBtn.disabled = false;
  }
}

teamSelect.addEventListener('change', async () => {
  const teamId = teamSelect.value;
  const selectedOption = teamSelect.options[teamSelect.selectedIndex];

  if (!teamId) {
    rosterPlayers = [];
    selectedTeam = null;
    selectedPlayer = null;
    playerSearchInput.value = '';
    applyTeamAccent();
    renderSelectedPlayer();
    updateSelectedCardStyles();
    clearAnalysisForNewSelection();
    rosterTitle.textContent = 'Team roster';
    rosterMeta.textContent = 'Choose a team to load players.';
    playerGrid.classList.add('empty-grid');
    playerGrid.innerHTML = `
      <div class="empty-roster-state empty-state-panel compact">
        <div class="empty-icon">🏀</div>
        <strong>Choose a team to load player cards.</strong>
        <span>You’ll get clickable headshots, jersey numbers, and positions.</span>
      </div>
    `;
    betFinderMeta.textContent = 'Uses your current team, prop, line, and recent-game sample.';
    renderBetFinderEmpty();
    renderMarketEmpty();
    setStatus('Ready');
    return;
  }

  const teamChanged = String(selectedPlayer?.team_id || '') !== String(teamId);
  applyTeamAccent(selectedOption.dataset.abbreviation);
  setStatus('Loading roster');
  renderRosterSkeleton();

  if (teamChanged && selectedPlayer) {
    selectedPlayer = null;
    playerSearchInput.value = '';
    renderSelectedPlayer();
    updateSelectedCardStyles();
    clearAnalysisForNewSelection();
  }

  try {
    await loadRoster(teamId);
    betFinderMeta.textContent = `${selectedOption.dataset.name} • ${getStatLabel(selectedStat)} ${lineInput.value} • Last ${gamesSelect.value}`;
    renderBetFinderEmpty('Click Bet Finder to rank this roster by recent hit rate.');
    setStatus('Roster ready');
  } catch (error) {
    console.error(error);
    alert(error.message);
    setStatus('Error');
  }
});


['focus', 'mousedown', 'touchstart'].forEach(evt => {
  teamSelect?.addEventListener(evt, () => {
    ensureTeamsLoaded();
  }, { passive: true });
});

playerSearchInput.addEventListener('input', () => {
  const query = playerSearchInput.value.trim();
  if (searchTimeout) clearTimeout(searchTimeout);

  if (query.length < 2) {
    searchResults.classList.add('hidden');
    return;
  }

  searchTimeout = setTimeout(async () => {
    try {
      const data = await searchPlayers(query);
      renderSearchResults(data.results || []);
    } catch (error) {
      console.error(error);
      searchResults.innerHTML = '<div class="search-item"><div class="search-copy"><div>Search failed.</div><small>Please try again.</small></div></div>';
      searchResults.classList.remove('hidden');
    }
  }, 250);
});

propButtonsWrap.querySelectorAll('.prop-chip').forEach(chip => {
  chip.addEventListener('click', () => setActiveProp(chip.dataset.stat));
});

navItems.forEach(item => {
  item.addEventListener('click', () => switchView(item.dataset.view));
});

quickViewButtons.forEach(button => {
  button.addEventListener('click', () => switchView(button.dataset.goView));
});

if (betFinderViewRunBtn) {
  betFinderViewRunBtn.addEventListener('click', runBetFinder);
}

analyzeBtn.addEventListener('click', analyzePlayerProp);
betFinderBtn.addEventListener('click', runBetFinder);

// Opponent override selector — highlight when a manual team is chosen
if (oppSelect) {
  oppSelect.addEventListener('change', () => {
    if (oppSelect.value) {
      oppSelect.classList.add('override-active');
    } else {
      oppSelect.classList.remove('override-active');
    }
  });
}
clearRecentBtn.addEventListener('click', () => clearRecentPlayersState({ resetCurrent: true }));
marketTemplateBtn.addEventListener('click', () => { marketTextarea.value = getMarketTemplate(); });
marketClearBtn.addEventListener('click', () => { marketTextarea.value = ''; renderMarketEmpty(); });
marketScanBtn.addEventListener('click', runMarketScan);
oddsLoadEventsBtn?.addEventListener('click', loadOddsEvents);
oddsImportScanBtn?.addEventListener('click', importOddsPropsAndScan);
[oddsApiKeyInput, oddsSportSelect, oddsRegionsInput, oddsFormatSelect, oddsMarketsInput].forEach(el => {
  el?.addEventListener('change', persistOddsApiSettings);
  el?.addEventListener('input', persistOddsApiSettings);
});
if (marketSortSelect) {
  marketSortSelect.value = currentMarketSort;
  marketSortSelect.addEventListener('change', () => {
    currentMarketSort = marketSortSelect.value || 'best_ev';
    currentMarketSortDirection = 'desc';
    localStorage.setItem('nba-props-market-sort', currentMarketSort);
    localStorage.setItem('nba-props-market-sort-direction', currentMarketSortDirection);
    if (currentMarketResultsPayload) renderMarketResults(currentMarketResultsPayload);
  });
}

if (marketFilterChips) {
  marketFilterChips.addEventListener('click', (event) => {
    const chip = event.target.closest('[data-filter-key]');
    if (!chip) return;
    event.preventDefault();
    event.stopPropagation();
    const nextFilter = chip.dataset.filterKey || 'all';
    if (currentMarketFilter === nextFilter) return;
    currentMarketFilter = nextFilter;
    localStorage.setItem('nba-props-market-filter', currentMarketFilter);
    renderMarketFilterChips();
    if (currentMarketResultsPayload) renderMarketResults(currentMarketResultsPayload);
  });
}

if (marketExpertFilterChips) {
  marketExpertFilterChips.addEventListener('click', (event) => {
    const chip = event.target.closest('[data-expert-filter-key]');
    if (!chip) return;
    event.preventDefault();
    event.stopPropagation();
    const key = chip.dataset.expertFilterKey || '';
    if (!key || !(key in currentExpertFilters)) return;
    currentExpertFilters[key] = !currentExpertFilters[key];
    saveExpertFilters();
    renderMarketExpertFilterChips();
    if (currentMarketResultsPayload) renderMarketResults(currentMarketResultsPayload);
  });
}
recentLogTab?.addEventListener('click', () => renderGameLogTab('recent'));


h2hLogTab?.addEventListener('click', () => renderGameLogTab('h2h'));

themeToggle.addEventListener('click', () => {
  const nextTheme = document.body.classList.contains('light-theme') ? 'dark' : 'light';
  setTheme(nextTheme);
});

if (sidebarToggle) {
  sidebarToggle.addEventListener('click', () => {
    const collapsed = !document.body.classList.contains('sidebar-collapsed');
    setSidebarCollapsed(collapsed);
  });
}

if (mobileSidebarToggle) {
  mobileSidebarToggle.addEventListener('click', () => {
    if (window.innerWidth < 1280) {
      document.body.classList.toggle('sidebar-mobile-open');
      return;
    }
    if (sidebarToggle) {
      sidebarToggle.click();
      return;
    }
    const collapsed = !document.body.classList.contains('sidebar-collapsed');
    setSidebarCollapsed(collapsed);
  });
}

document.addEventListener('click', (event) => {
  if (!playerSearchInput.contains(event.target) && !searchResults.contains(event.target)) {
    searchResults.classList.add('hidden');
  }

  if (
    window.innerWidth < 1280 &&
    document.body.classList.contains('sidebar-mobile-open') &&
    !event.target.closest('.dashboard-sidebar') &&
    !event.target.closest('#mobileSidebarToggle')
  ) {
    document.body.classList.remove('sidebar-mobile-open');
  }
});

window.addEventListener('keydown', (event) => {
  if (event.key === 'Enter' && document.activeElement !== playerSearchInput) {
    analyzePlayerProp();
  }
});

(async function init() {
  applySavedTheme();
  renderRecentPlayers();
  renderSelectedPlayer();
  renderOverviewSelection();
  resetDashboardForNoSelection();
  renderBetFinderEmpty();
  renderMarketEmpty();
  renderMarketFilterChips();
  renderMarketExpertFilterChips();
  loadStoredOddsApiSettings();
  setActiveProp(selectedStat);
  switchView(activeView);
  setStatus('Loading teams');

  try {
    await loadTeams();
    setStatus('Ready');
  } catch (error) {
    console.error(error);
    alert(error.message);
    setStatus('Error');
  }
})();

/* === Premium UX / prediction upgrade overrides === */
const recommendationToneEl = document.getElementById('recommendationTone');
const recommendationBodyEl = document.getElementById('recommendationBody');
const overviewCautionBoardEl = document.getElementById('overviewCautionBoard');
const overviewBoostBoardEl = document.getElementById('overviewBoostBoard');
const favoritesListEl = document.getElementById('favoritesList');
const simpleViewBtnEl = document.getElementById('simpleViewBtn');
const advancedViewBtnEl = document.getElementById('advancedViewBtn');
const stickyPlayerNameEl = document.getElementById('stickyPlayerName');
const stickyPropLabelEl = document.getElementById('stickyPropLabel');
const favoritePlayerBtnEl = document.getElementById('favoritePlayerBtn');
const savePropBtnEl = document.getElementById('savePropBtn');
const chartModeBarsBtnEl = document.getElementById('chartModeBarsBtn');
const chartModeComboBtnEl = document.getElementById('chartModeComboBtn');
const chartHighlightBtnEl = document.getElementById('chartHighlightBtn');
const chartSampleBtnsEl = document.querySelectorAll('.chart-sample-btn');
const collapseButtonsEl = document.querySelectorAll('.collapse-btn');
const FAVORITES_KEY = 'nba-props-favorites';
const DENSITY_MODE_KEY = 'nba-props-density-mode';
const CHART_PREFS_KEY = 'nba-props-chart-prefs';
let analyzerDensityMode = localStorage.getItem(DENSITY_MODE_KEY) || 'simple';
let upgradedChartPrefs = (() => {
  try {
    const parsed = JSON.parse(localStorage.getItem(CHART_PREFS_KEY) || '{}');
    return { mode: parsed.mode === 'combo' ? 'combo' : 'bars', highlight: parsed.highlight !== false };
  } catch {
    return { mode: 'bars', highlight: true };
  }
})();

function getTeamLogo(teamId) {
  return teamId ? `https://cdn.nba.com/logos/nba/${teamId}/global/L/logo.svg` : '';
}

function loadFavoritesUpgrade() {
  try {
    return JSON.parse(localStorage.getItem(FAVORITES_KEY) || '[]');
  } catch {
    return [];
  }
}

function saveFavoritesUpgrade(items) {
  localStorage.setItem(FAVORITES_KEY, JSON.stringify(items.slice(0, 12)));
}

function updateStickyAnalyzerSummary() {
  if (stickyPlayerNameEl) stickyPlayerNameEl.textContent = selectedPlayer?.full_name || 'No player selected';
  if (stickyPropLabelEl) stickyPropLabelEl.textContent = `${selectedStat} ${lineInput?.value || '—'}`;
  if (favoritePlayerBtnEl) {
    const favorites = loadFavoritesUpgrade();
    const isFavorite = !!selectedPlayer && favorites.some(item => item.type === 'player' && item.key === String(selectedPlayer.id));
    favoritePlayerBtnEl.classList.toggle('active', isFavorite);
  }
}

function toggleFavoriteUpgrade(item) {
  const favorites = loadFavoritesUpgrade();
  const key = `${item.type}:${item.key}`;
  const existingIndex = favorites.findIndex(entry => `${entry.type}:${entry.key}` === key);
  if (existingIndex >= 0) favorites.splice(existingIndex, 1);
  else favorites.unshift(item);
  saveFavoritesUpgrade(favorites);
  renderFavoritesUpgrade();
  updateStickyAnalyzerSummary();
}

function renderFavoritesUpgrade() {
  if (!favoritesListEl) return;
  const favorites = loadFavoritesUpgrade();
  if (!favorites.length) {
    favoritesListEl.className = 'favorites-list empty-state-panel compact';
    favoritesListEl.innerHTML = `
      <div class="empty-icon">💾</div>
      <strong>No favorites yet.</strong>
      <span>Star a player or save a prop from the analyzer to keep it here.</span>
    `;
    return;
  }

  favoritesListEl.className = 'favorites-list favorites-grid';
  favoritesListEl.innerHTML = favorites.map((item, index) => {
    const player = item.player || null;
    const teamId = item.team_id || player?.team_id || null;
    const teamAbbr = item.team_abbreviation || player?.team_abbreviation || '';
    const playerName = player?.full_name || item.title || 'Saved favorite';
    const isProp = item.type === 'prop';
    const eyebrow = isProp ? 'Saved prop' : 'Saved player';
    const subtitle = item.subtitle || (isProp ? 'Reusable prop setup' : (player?.position || 'Saved player'));
    const kicker = isProp
      ? `${item.stat || ''} ${item.line ?? ''}`.trim()
      : [teamAbbr, player?.position].filter(Boolean).join(' • ');
    const logo = teamId ? getTeamLogo(teamId) : '';
    const headshot = player?.id ? getPlayerImage(player.id) : getFallbackHeadshot();
    return `
      <button class="favorite-card favorite-card-rich" type="button" data-index="${index}">
        <div class="favorite-card-top">
          <span class="favorite-type ${item.type}">${eyebrow}</span>
          ${kicker ? `<span class="favorite-chip">${escapeHtml(kicker)}</span>` : ''}
        </div>
        <div class="favorite-card-body">
          <div class="favorite-visual-shell">
            <img class="favorite-player-shot" src="${headshot}" alt="${escapeHtml(playerName)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
            ${logo ? `<img class="favorite-team-logo" src="${logo}" alt="${escapeHtml(teamAbbr || 'Team')}" onerror="this.hidden=true">` : ''}
          </div>
          <div class="favorite-copy">
            <strong>${escapeHtml(item.title)}</strong>
            <small>${escapeHtml(subtitle)}</small>
          </div>
        </div>
      </button>
    `;
  }).join('');

  favoritesListEl.querySelectorAll('.favorite-card').forEach(card => {
    card.addEventListener('click', async () => {
      const item = favorites[Number(card.dataset.index)];
      if (!item) return;
      if (item.type === 'player') {
        if (item.team_id && String(teamSelect.value) !== String(item.team_id)) {
          teamSelect.value = String(item.team_id);
          await loadRoster(Number(item.team_id));
        }
        setSelectedPlayer(item.player);
        switchView('analyzer');
        return;
      }
      if (item.type === 'prop') {
        if (item.player?.team_id && String(teamSelect.value) !== String(item.player.team_id)) {
          teamSelect.value = String(item.player.team_id);
          await loadRoster(Number(item.player.team_id));
        }
        setSelectedPlayer(item.player);
        setActiveProp(item.stat);
        lineInput.value = item.line;
        // Reset opponent to auto when loading from favorites (no specific game context)
        if (oppSelect) { oppSelect.value = ''; oppSelect.classList.remove('override-active'); }
        switchView('analyzer');
        await analyzePlayerProp();
      }
    });
  });
}

function applyDensityModeUpgrade(mode) {
  analyzerDensityMode = mode === 'advanced' ? 'advanced' : 'simple';
  document.body.classList.toggle('density-simple', analyzerDensityMode === 'simple');
  document.body.classList.toggle('density-advanced', analyzerDensityMode === 'advanced');
  localStorage.setItem(DENSITY_MODE_KEY, analyzerDensityMode);
  simpleViewBtnEl?.classList.toggle('active', analyzerDensityMode === 'simple');
  advancedViewBtnEl?.classList.toggle('active', analyzerDensityMode === 'advanced');
}

function saveChartPrefsUpgrade() {
  localStorage.setItem(CHART_PREFS_KEY, JSON.stringify(upgradedChartPrefs));
  chartModeBarsBtnEl?.classList.toggle('active', upgradedChartPrefs.mode === 'bars');
  chartModeComboBtnEl?.classList.toggle('active', upgradedChartPrefs.mode === 'combo');
  chartHighlightBtnEl?.classList.toggle('active', !!upgradedChartPrefs.highlight);
  if (chartHighlightBtnEl) chartHighlightBtnEl.textContent = upgradedChartPrefs.highlight ? 'Highlight O/U' : 'Neutral bars';
}

function updateChartSampleButtonsUpgrade() {
  chartSampleBtnsEl.forEach(btn => btn.classList.toggle('active', btn.dataset.sample === String(gamesSelect?.value || '10')));
}

function renderPanelSkeletonUpgrade(body, icon = '⏳', title = 'Loading...', subtitle = 'Pulling current data...') {
  if (!body) return;
  body.className = 'empty-state-panel compact skeleton-panel';
  body.innerHTML = `
    <div class="empty-icon">${icon}</div>
    <strong>${title}</strong>
    <span>${subtitle}</span>
    <div class="skeleton-line"></div>
    <div class="skeleton-line short"></div>
  `;
}

function toggleSectionByIdUpgrade(id) {
  const body = document.getElementById(id);
  if (!body) return;
  const collapsed = !body.classList.contains('collapsed');
  body.classList.toggle('collapsed', collapsed);
  body.closest('.card')?.classList.toggle('section-collapsed', collapsed);
  const btn = document.querySelector(`.collapse-btn[data-collapse-target="${id}"]`);
  if (btn) btn.textContent = collapsed ? '+' : '−';
}

function renderOverviewBestBets() {
  if (!overviewBestBets || !overviewBestBetsMeta) return;
  const stored = loadStoredMarketResults();
  const results = stored?.results || [];
  overviewBestBetsMeta.textContent = results.length ? formatStoredTime(stored.updated_at) : 'Populated by your latest Market Scanner run.';

  const renderBoard = (node, items, icon, title, subtitle) => {
    if (!node) return;
    if (!items.length) {
      node.className = 'overview-best-bets-list empty-state-panel compact board-empty-state';
      node.innerHTML = `<div class="board-empty-head"><div class="empty-icon">${icon}</div><strong>${title}</strong></div><span>${subtitle}</span>`;
      return;
    }
    node.className = 'overview-best-bets-list';
    node.innerHTML = items.map((item, index) => `
      <button class="overview-best-bet-card ${item.best_bet.confidence_tone || ''}" data-index="${index}" type="button">
        <div class="overview-best-bet-head">
          <span class="overview-rank">#${index + 1}</span>
          <span class="finder-badge ${(item.best_bet.traffic_light?.tone || item.best_bet.confidence_tone || '')}">${escapeHtml(item.best_bet.display_side || item.best_bet.side)} • ${escapeHtml(item.best_bet.traffic_light?.label || item.best_bet.confidence || '')}</span>
        </div>
        <strong>${escapeHtml(item.player.full_name)}</strong>
        <small>${escapeHtml(item.market.stat)} ${item.market.line} • ${escapeHtml(item.player.team || '')}${item.player.opponent ? ` vs ${escapeHtml(item.player.opponent)}` : ''}</small>
        <div class="overview-best-bet-metrics"><span>Edge ${item.best_bet.edge ?? '—'}%</span><span>EV ${item.best_bet.ev ?? '—'}%</span></div>
        <p>${escapeHtml(item.best_bet.explanation || item.best_bet.user_read || 'Latest board note.')}</p>
      </button>
    `).join('');
    node.querySelectorAll('.overview-best-bet-card').forEach((card, idx) => card.addEventListener('click', async () => focusMarketPlayer(items[idx])));
  };

  const strongest = results.slice(0, 5);
  const caution = results.filter(item => ['yellow', 'red'].includes(item.best_bet.traffic_light?.tone || '') || item.analysis?.availability?.is_risky || item.availability?.is_risky || (item.analysis?.matchup?.vs_position?.lean_tone === 'bad')).slice(0, 3);
  const boosts = results.filter(item => Number(item.analysis?.team_context?.impact_count || item.team_context?.impact_count || 0) > 0 || /boost|expanded|thin|rise/i.test(String(item.analysis?.team_context?.headline || item.team_context?.headline || ''))).slice(0, 3);
  renderBoard(overviewBestBets, strongest, '⭐', 'No best bets saved yet.', 'Run Market Scanner to pin the strongest current board edges here.');
  renderBoard(overviewCautionBoardEl, caution, '⚠️', 'No caution spots yet.', 'Risky plays will appear here after a board scan.');
  renderBoard(overviewBoostBoardEl, boosts, '📈', 'No injury boosts yet.', 'Plays with teammate-absence boosts will appear here.');
}

// buildTodayGameCard defined above (with injury panel)

function renderTodayGames(payload) {
  latestTodayGamesPayload = payload;
  const rawGames = payload.games || [];
  const seenGameIds = new Set();
  const games = rawGames.filter(game => {
    const key = game.game_id || `${game.away?.team_id}-${game.home?.team_id}-${game.game_label}`;
    if (seenGameIds.has(key)) return false;
    seenGameIds.add(key);
    return true;
  });
  if (todayGamesMeta) {
    todayGamesMeta.textContent = payload.fallback_used
      ? `No games on ${payload.requested_date}. Showing next slate on ${payload.resolved_date}. ${payload.report_label ? `• Report ${payload.report_label}` : ''}`
      : `${games.length} game${games.length === 1 ? '' : 's'} on ${payload.resolved_date}${payload.report_label ? ` • Report ${payload.report_label}` : ''}`;
  }
  if (overviewTodayMeta) {
    overviewTodayMeta.textContent = payload.fallback_used ? `Next slate: ${payload.resolved_date}` : `${games.length} game${games.length === 1 ? '' : 's'} on ${payload.resolved_date}`;
  }
  if (!games.length) {
    const emptyHtml = `<div class="empty-state-panel compact today-game-empty"><div class="empty-icon">🗓️</div><strong>No games on the active slate.</strong><span>When the NBA schedule posts games, they will appear here with report context.</span></div>`;
    if (todayGamesBoard) todayGamesBoard.innerHTML = emptyHtml;
    if (overviewTodayGames) overviewTodayGames.innerHTML = emptyHtml;
    return;
  }
  if (todayGamesBoard) {
    todayGamesBoard.innerHTML = games.map(game => buildTodayGameCard(game, false)).join('');
    bindSlateTeamButtons(todayGamesBoard);
  }
  if (overviewTodayGames) {
    overviewTodayGames.className = 'today-overview-list';
    overviewTodayGames.innerHTML = games.slice(0, 4).map(game => buildTodayGameCard(game, true)).join('');
    bindSlateTeamButtons(overviewTodayGames);
  }
}

function renderInterpretationPanels(payload) {
  const interpretation = payload?.interpretation || {};
  const opportunity = payload?.opportunity || {};
  const teamContext = payload?.team_context || {};
  const environment = payload?.environment || {};
  const recommendation = payload?.traffic_light || { label: 'Caution', tone: 'yellow', summary: 'Analyze a prop to generate a clearer read.' };
  const confidence = payload?.confidence || {};
  const toneMap = { good: 'good', warning: 'warning', bad: 'bad', neutral: 'neutral' };
  const interpretationToneClass = toneMap[interpretation.tone] || 'neutral';
  const opportunityToneClass = opportunity.minutes_trend === 'up' || opportunity.volume_trend === 'up' ? 'good' : (teamContext.impact_count ? 'warning' : 'neutral');
  const environmentToneClass = toneMap[environment.tone] || 'neutral';
  const trafficToneClass = recommendation.tone === 'green' ? 'good' : recommendation.tone === 'red' ? 'bad' : 'warning';

  if (recommendationToneEl) {
    recommendationToneEl.className = `spotlight-pill ${trafficToneClass}`;
    recommendationToneEl.textContent = recommendation.label || 'Recommendation';
  }
  if (recommendationBodyEl) {
    recommendationBodyEl.className = 'interpretation-body';
    recommendationBodyEl.innerHTML = `
      <div class="insight-summary ${trafficToneClass}">
        <span class="insight-summary-label">Traffic light</span>
        <strong>${escapeHtml(recommendation.label || 'Caution')}</strong>
        <p>${escapeHtml(recommendation.summary || 'Analyze a player prop to generate a clearer read.')}</p>
      </div>
      <div class="traffic-light-row">
        <span class="traffic-pill ${trafficToneClass}">${escapeHtml(payload?.recommended_side || 'LEAN')}</span>
        <span class="traffic-meta">${escapeHtml(confidence.grade || '—')} ${escapeHtml(String(confidence.score || ''))} • ${escapeHtml(confidence.summary || '')}</span>
      </div>`;
  }
  if (interpretationTone) {
    interpretationTone.className = `spotlight-pill ${interpretationToneClass}`;
    interpretationTone.textContent = interpretation.headline || 'Quick read';
  }
  if (opportunityTone) {
    opportunityTone.className = `spotlight-pill ${opportunityToneClass}`;
    opportunityTone.textContent = opportunity.minutes_label || 'Opportunity';
  }
  if (environmentTone) {
    environmentTone.className = `spotlight-pill ${environmentToneClass}`;
    environmentTone.textContent = environment.headline || 'Schedule spot';
  }
  if (interpretationBody) {
    const bullets = Array.isArray(interpretation.bullets) ? interpretation.bullets : [];
    interpretationBody.className = 'interpretation-body';
    interpretationBody.innerHTML = `
      <div class="insight-summary ${interpretationToneClass}">
        <span class="insight-summary-label">Quick read</span>
        <strong>${escapeHtml(interpretation.headline || 'Quick read unavailable')}</strong>
        <p>${escapeHtml(interpretation.summary || 'Analyze a player prop to generate a simple read.')}</p>
      </div>
      <ul class="insight-bullet-list compact-bullets">${bullets.length ? bullets.map(item => `<li>${escapeHtml(item)}</li>`).join('') : '<li>Analyze a player prop to fill this section.</li>'}</ul>`;
  }
  if (opportunityBody) {
    const listedPlayers = (teamContext.players || []).map(item => `${formatPlayerName(item.name)} (${item.status})`);
    const focusMetrics = Array.isArray(opportunity.focus_metrics) ? opportunity.focus_metrics : [];
    opportunityBody.className = 'opportunity-body';
    opportunityBody.innerHTML = `
      <div class="opportunity-chip-grid refined-opportunity-grid">
        <div class="opportunity-chip"><span class="small-label">Minutes</span><strong>${Number(opportunity.minutes_last5 || 0).toFixed(1)}</strong><small>${escapeHtml(opportunity.minutes_label || 'Minutes trend')}</small></div>
        <div class="opportunity-chip"><span class="small-label">FGA</span><strong>${Number(opportunity.fga_last5 || 0).toFixed(1)}</strong><small>${escapeHtml(opportunity.volume_label || 'Shot volume trend')}</small></div>
        <div class="opportunity-chip"><span class="small-label">3PA</span><strong>${Number(opportunity.fg3a_last5 || 0).toFixed(1)}</strong><small>Three-point volume</small></div>
        <div class="opportunity-chip"><span class="small-label">FTA</span><strong>${Number(opportunity.fta_last5 || 0).toFixed(1)}</strong><small>Free-throw trips</small></div>
      </div>
      <div class="opportunity-focus-card ${opportunityToneClass}">
        <span class="insight-summary-label">${escapeHtml(opportunity.focus_title || 'Prop focus')}</span>
        <strong>${escapeHtml(opportunity.focus_summary || 'Prop-specific context appears here after analysis.')}</strong>
        <div class="opportunity-focus-grid">${focusMetrics.length ? focusMetrics.map(metric => `<div class="focus-metric"><span>${escapeHtml(metric.label)}</span><strong>${escapeHtml(String(metric.value))}</strong><small>${escapeHtml(metric.note || '')}</small></div>`).join('') : '<div class="focus-metric"><span>Stat</span><strong>—</strong><small>Analyze a prop first</small></div>'}</div>
      </div>
      <div class="opportunity-summary-wrap refined-opportunity-wrap">
        <div class="insight-summary neutral compact-summary"><span class="insight-summary-label">Opportunity read</span><p class="opportunity-summary">${escapeHtml(opportunity.summary || 'Opportunity trends will appear after analysis.')}</p></div>
        <div class="team-context-box ${teamContext.impact_count ? 'warning' : 'neutral'}"><strong>${escapeHtml(teamContext.headline || 'Team context')}</strong><p>${escapeHtml(teamContext.impact_summary || teamContext.summary || 'No team-availability context yet.')}</p>${listedPlayers.length ? `<small>${escapeHtml(listedPlayers.join(' • '))}</small>` : '<small>No major same-team absences flagged on the latest report.</small>'}</div>
      </div>`;
  }
  if (environmentBody) {
    const restValue = Number.isInteger(environment.rest_days) ? `${environment.rest_days} day${environment.rest_days === 1 ? '' : 's'}` : 'Unknown';
    environmentBody.className = 'environment-body';
    environmentBody.innerHTML = `
      <div class="environment-chip-grid">
        <div class="environment-chip"><span class="small-label">Venue</span><strong>${escapeHtml(environment.venue_label || 'TBD')}</strong><small>${escapeHtml(environment.next_opponent ? `Next ${environment.next_opponent}` : 'Upcoming game')}</small></div>
        <div class="environment-chip"><span class="small-label">Rest</span><strong>${escapeHtml(restValue)}</strong><small>${escapeHtml(environment.headline || 'Schedule spot')}</small></div>
        <div class="environment-chip ${environment.is_back_to_back ? 'warning' : ''}"><span class="small-label">Back-to-back</span><strong>${environment.is_back_to_back ? 'Yes' : 'No'}</strong><small>${environment.is_back_to_back ? 'More fatigue risk' : 'No immediate fatigue flag'}</small></div>
        <div class="environment-chip"><span class="small-label">Games in 7</span><strong>${Number(environment.games_last7 || 0)}</strong><small>${Number(environment.games_last7 || 0) >= 4 ? 'Busy recent schedule' : 'Normal recent load'}</small></div>
      </div>
      <div class="insight-summary ${environmentToneClass} compact-summary"><span class="insight-summary-label">Schedule read</span><strong>${escapeHtml(environment.headline || 'Schedule spot')}</strong><p class="opportunity-summary">${escapeHtml(environment.summary || 'Schedule context will appear after analysis.')}</p></div>`;
  }
}



function createTrendValueLabelPlugin(chartGames, payload) {
  return {
    id: 'trendValueLabelPlugin',
    afterDatasetsDraw(chartInstance) {
      const { ctx } = chartInstance;
      ctx.save();
      ctx.font = '700 11px Inter, Arial, sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';

      chartInstance.data.datasets.forEach((dataset, datasetIndex) => {
        const meta = chartInstance.getDatasetMeta(datasetIndex);
        if (!meta || meta.hidden || dataset.type === 'line') return;

        meta.data.forEach((element, index) => {
          const rawValue = Number(Array.isArray(dataset.data) ? dataset.data[index] : 0);
          if (!Number.isFinite(rawValue) || rawValue <= 0) return;

          const props = element.getProps(['x', 'y', 'base', 'width'], true);
          const height = Math.abs((props.base ?? 0) - (props.y ?? 0));
          if (!Number.isFinite(height) || height < 16) return;

          const text = Number.isInteger(rawValue) ? String(rawValue) : rawValue.toFixed(1);
          ctx.fillStyle = '#ffffff';
          ctx.fillText(text, props.x, props.y + height / 2);
        });
      });

      ctx.restore();
    }
  };
}
function formatTrendAxisDate(value) {
  if (!value) return '';
  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  }
  const cleaned = String(value).trim();
  const parsedText = new Date(cleaned.replace(/\s+/g, ' '));
  if (!Number.isNaN(parsedText.getTime())) {
    return parsedText.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  }
  return cleaned;
}

/**
 * Format any date/datetime string in Philippines Time (Asia/Manila, UTC+8).
 * Returns e.g. "Apr 5 • 8:00 AM PHT" or "Apr 5 PHT" if no time component.
 */
function formatPHT(value) {
  if (!value) return '';
  const PHT = 'Asia/Manila';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value).trim();
  const datePart = d.toLocaleDateString('en-PH', { timeZone: PHT, month: 'short', day: 'numeric' });
  // Only show time if there is a non-midnight time component or an explicit T in the string
  const hasTime = String(value).includes('T') || String(value).includes(' ') && String(value).match(/\d{1,2}:\d{2}/);
  if (hasTime) {
    const timePart = d.toLocaleTimeString('en-PH', { timeZone: PHT, hour: 'numeric', minute: '2-digit' });
    return `${datePart} • ${timePart} PHT`;
  }
  return `${datePart} PHT`;
}

/**
 * Format a next-game date string for display in the matchup panels.
 * Uses PHT if value looks like an ISO datetime, otherwise falls back to formatTrendAxisDate.
 */
function formatNextGameDate(value) {
  if (!value) return 'Date TBD';
  // ISO datetime (contains T or is YYYY-MM-DD format)
  if (/^\d{4}-\d{2}-\d{2}/.test(String(value).trim())) {
    return formatPHT(value);
  }
  return formatTrendAxisDate(value);
}

function renderChart(payload) {
  lastPayload = payload;
  const chartGames = payload.games || [];
  const labels = chartGames.map(game => formatTrendAxisDate(game.game_date || game.gameDate || game.date || game.matchup || ''));
  const values = chartGames.map(game => game.value);
  const hits = chartGames.map(game => game.hit);
  const accent = getCssVar('--accent');
  const good = getCssVar('--good');
  const bad = getCssVar('--bad');
  const textColor = getChartTextColor();
  const muted = getMutedColor();
  const linePlugin = createLinePlugin(payload.line);
  const valueLabelPlugin = createTrendValueLabelPlugin(chartGames, payload);
  const comboParts = getComboStatParts(payload.stat);
  const isComboMarket = comboParts.length > 1;

  if (chart) chart.destroy();
  let datasets = [];
  if (isComboMarket) {
    datasets = comboParts.map((part, index) => ({
      type: 'bar',
      label: getStatLabel(part),
      data: chartGames.map(game => Number(game?.components?.[part] ?? game?.[part.toLowerCase()] ?? 0)),
      backgroundColor: chartGames.map(() => getComboStatColor(part, index)),
      borderColor: chartGames.map(() => getComboStatColor(part, index).replace('0.82', '1')),
      borderWidth: 1.2,
      borderRadius: { topLeft: 10, topRight: 10, bottomLeft: 0, bottomRight: 0 },
      borderSkipped: false,
      maxBarThickness: 34,
      categoryPercentage: 0.72,
      barPercentage: 0.88,
      stack: 'combo-total',
    }));
    if (upgradedChartPrefs.mode === 'combo') {
      datasets.push({ type: 'line', label: 'Total', data: values, borderColor: accent, backgroundColor: accent, pointBackgroundColor: accent, pointRadius: 3, pointHoverRadius: 4, borderWidth: 2, tension: 0.28, fill: false, yAxisID: 'y' });
    }
  } else {
    const baseColors = upgradedChartPrefs.highlight ? hits.map(hit => hit ? `${good}CC` : `${bad}CC`) : values.map(() => `${accent}CC`);
    const baseBorders = upgradedChartPrefs.highlight ? hits.map(hit => hit ? good : bad) : values.map(() => accent);
    datasets = [{
      type: 'bar', label: getStatLabel(payload.stat), data: values, backgroundColor: baseColors, borderColor: baseBorders, borderWidth: 1.4, borderRadius: 12, borderSkipped: false, maxBarThickness: 42,
    }];
    if (upgradedChartPrefs.mode === 'combo') {
      datasets.push({ type: 'line', label: 'Trend', data: values, borderColor: accent, pointBackgroundColor: accent, pointRadius: 3, pointHoverRadius: 4, borderWidth: 2, tension: 0.28, fill: false });
    }
  }
  chart = new Chart(document.getElementById('propsChart'), {
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 520 },
      layout: { padding: { bottom: 12 } },
      plugins: {
        legend: { display: isComboMarket, labels: { color: muted, usePointStyle: true, pointStyle: 'rectRounded', padding: 14 } },
        tooltip: {
          backgroundColor: document.body.classList.contains('light-theme') ? 'rgba(255,255,255,0.96)' : 'rgba(10,16,31,0.95)',
          titleColor: textColor,
          bodyColor: textColor,
          borderColor: `${accent}55`,
          borderWidth: 1,
          padding: 12,
          displayColors: true,
          callbacks: {
            label(context) {
              const game = chartGames[context.dataIndex] || {};
              if (isComboMarket && context.dataset?.label !== 'Total') {
                return `${context.dataset.label}: ${context.raw}`;
              }
              const value = Number(context.raw || 0);
              const verdict = value >= payload.line ? 'Over ✓' : 'Under ✗';
              const extra = isComboMarket ? ` • Total ${Number(game.value || 0).toFixed(1)}` : '';
              return `${context.dataset?.label || getStatLabel(payload.stat)}: ${value}${extra} (${verdict})`;
            },
            afterBody(items) {
              if (!items?.length) return [];
              const game = chartGames[items[0].dataIndex] || {};
              const lines = [];
              if (isComboMarket) lines.push(`Line: ${payload.line}`, `Total: ${Number(game.value || 0).toFixed(1)}`);
              if (game.matchup) lines.push(`vs ${game.matchup}`);
              if (game.is_home === true) lines.push('Home game');
              else if (game.is_home === false) lines.push('Away game');
              return lines;
            }
          }
        }
      },
      scales: {
        x: { stacked: isComboMarket, ticks: { color: muted, autoSkip: false, minRotation: 35, maxRotation: 35, padding: 8, callback(value, index) { return labels[index] || ''; } }, grid: { display: false }, border: { display: false } },
        y: { stacked: isComboMarket, beginAtZero: true, ticks: { color: muted }, grid: { color: document.body.classList.contains('light-theme') ? 'rgba(17,33,63,0.08)' : 'rgba(255,255,255,0.08)' }, border: { display: false } }
      }
    },
    plugins: [linePlugin, valueLabelPlugin]
  });
}

if (lineInput) lineInput.addEventListener('input', updateStickyAnalyzerSummary);
if (gamesSelect) gamesSelect.addEventListener('change', updateChartSampleButtonsUpgrade);
simpleViewBtnEl?.addEventListener('click', () => applyDensityModeUpgrade('simple'));
advancedViewBtnEl?.addEventListener('click', () => applyDensityModeUpgrade('advanced'));
chartModeBarsBtnEl?.addEventListener('click', () => { upgradedChartPrefs.mode = 'bars'; saveChartPrefsUpgrade(); if (lastPayload) renderChart(lastPayload); });
chartModeComboBtnEl?.addEventListener('click', () => { upgradedChartPrefs.mode = 'combo'; saveChartPrefsUpgrade(); if (lastPayload) renderChart(lastPayload); });
chartHighlightBtnEl?.addEventListener('click', () => { upgradedChartPrefs.highlight = !upgradedChartPrefs.highlight; saveChartPrefsUpgrade(); if (lastPayload) renderChart(lastPayload); });
chartSampleBtnsEl.forEach(btn => btn.addEventListener('click', async () => {
  gamesSelect.value = btn.dataset.sample;
  updateChartSampleButtonsUpgrade();
  if (selectedPlayer) await analyzePlayerProp({ preserveScroll: true });
}));
collapseButtonsEl.forEach(btn => btn.addEventListener('click', () => toggleSectionByIdUpgrade(btn.dataset.collapseTarget)));
favoritePlayerBtnEl?.addEventListener('click', () => {
  if (!selectedPlayer) return;
  toggleFavoriteUpgrade({ type: 'player', key: String(selectedPlayer.id), title: selectedPlayer.full_name, subtitle: `${selectedPlayer.team_abbreviation || ''} ${selectedPlayer.position || ''}`.trim(), team_id: selectedPlayer.team_id || null, player: selectedPlayer });
});
savePropBtnEl?.addEventListener('click', () => {
  if (!selectedPlayer) return;
  toggleFavoriteUpgrade({ type: 'prop', key: `${selectedPlayer.id}:${selectedStat}:${lineInput.value}`, title: `${selectedPlayer.full_name} • ${selectedStat} ${lineInput.value}`, subtitle: lastPayload?.traffic_light?.summary || lastPayload?.interpretation?.summary || 'Saved from analyzer', stat: selectedStat, line: Number(lineInput.value || 0), player: selectedPlayer });
});
analyzeBtn?.addEventListener('click', () => {
  getMatchupTargets().forEach(target => renderPanelSkeletonUpgrade(target.body, '🛡️', 'Loading matchup...', 'Fetching the next opponent context.'));
  renderPanelSkeletonUpgrade(recommendationBodyEl, '🚦', 'Loading recommendation...', 'Building the traffic-light read.');
  renderPanelSkeletonUpgrade(interpretationBody, '🧠', 'Loading quick read...', 'Summarizing the latest signals.');
  renderPanelSkeletonUpgrade(opportunityBody, '⏱️', 'Loading opportunity...', 'Pulling minutes, attempts, and team context.');
  renderPanelSkeletonUpgrade(environmentBody, '📅', 'Loading schedule...', 'Checking the current rest and schedule spot.');
}, true);
marketScanBtn?.addEventListener('click', () => {
  if (marketResults) marketResults.innerHTML = `<div class="empty-state-panel compact skeleton-panel"><div class="empty-icon">⏳</div><strong>Scanning your board...</strong><span>Comparing hit rate, implied odds, EV, and matchup context.</span><div class="skeleton-line"></div><div class="skeleton-line short"></div></div>`;
}, true);
applyDensityModeUpgrade(analyzerDensityMode);
saveChartPrefsUpgrade();
renderFavoritesUpgrade();
renderOverviewBestBets();
updateStickyAnalyzerSummary();
updateChartSampleButtonsUpgrade();


function setActiveProp(stat) {
  selectedStat = stat;
  propButtonsWrap.querySelectorAll('.prop-chip').forEach(chip => {
    chip.classList.toggle('active', chip.dataset.stat === stat);
  });
  updateStickyAnalyzerSummary();
}

function getSelectedPlayerContextData() {
  const payload = lastPayload && String(lastPayload?.player?.id || '') === String(selectedPlayer?.id || '') ? lastPayload : null;
  return {
    matchup: selectedPlayer?.matchup || payload?.matchup || null,
    environment: selectedPlayer?.environment || payload?.environment || null,
    availability: selectedPlayer?.availability || payload?.availability || null
  };
}

function ensureSelectedPlayerMatchupTile() {
  if (selectedPlayerMatchupTile) return selectedPlayerMatchupTile;
  const matchupSection = document.getElementById('selectedPlayerMatchupSection');
  if (!matchupSection) return null;
  selectedPlayerMatchupTile = document.createElement('article');
  selectedPlayerMatchupTile.id = 'selectedPlayerMatchupTile';
  selectedPlayerMatchupTile.className = 'card glass selected-player-matchup-tile empty-state-panel compact matchup-empty hidden';
  matchupSection.appendChild(selectedPlayerMatchupTile);
  return selectedPlayerMatchupTile;
}

function renderSelectedPlayerContext() {
  const tile = ensureSelectedPlayerMatchupTile();
  const matchupSection = document.getElementById('selectedPlayerMatchupSection');
  if (!tile || !matchupSection) return '';
  if (!selectedPlayer) {
    tile.className = 'card glass selected-player-matchup-tile empty-state-panel compact matchup-empty hidden';
    tile.innerHTML = '';
    matchupSection.classList.add('hidden');
    return '';
  }

  const { matchup, availability } = getSelectedPlayerContextData();
  const nextGame = matchup?.next_game || null;
  const vsPosition = matchup?.vs_position || null;

  if (!nextGame && !vsPosition && !availability) {
    tile.className = 'card glass selected-player-matchup-tile empty-state-panel compact matchup-empty hidden';
    tile.innerHTML = '';
    matchupSection.classList.add('hidden');
    return '';
  }

  const toneMap = { good: 'good', bad: 'bad', warning: 'neutral', neutral: 'neutral' };
  const isOverrideTile = Boolean(nextGame?.is_override);
  const leanTone = isOverrideTile ? 'warning' : (toneMap[getLeanClass(vsPosition?.lean_tone)] || 'neutral');
  const leanText = isOverrideTile ? `📌 vs ${nextGame?.opponent_abbreviation || 'Override'}` : (vsPosition?.lean || (nextGame ? 'Upcoming game found' : 'Partial matchup'));
  const nextGameLabel = nextGame
    ? `${nextGame.matchup_label || nextGame.opponent_name || 'Upcoming game'} • ${nextGame.game_date ? formatNextGameDate(nextGame.game_date) : 'Date TBA'}${nextGame.game_time ? ` • ${nextGame.game_time}` : ''}`
    : 'Upcoming game unavailable';
  const venueLabel = nextGame ? (isOverrideTile ? 'Opponent override' : (nextGame.is_home ? 'Home game' : 'Away game')) : 'Venue unavailable';
  const oppVal = typeof vsPosition?.opponent_value === 'number' ? vsPosition.opponent_value.toFixed(2) : (vsPosition?.opponent_value ?? '—');
  const lgVal = typeof vsPosition?.league_average === 'number' ? vsPosition.league_average.toFixed(2) : (vsPosition?.league_average ?? '—');
  const delta = typeof vsPosition?.delta_pct === 'number' ? `${formatDelta(vsPosition.delta_pct)}%` : '—';
  const sample = typeof vsPosition?.sample_gp === 'number' ? `${vsPosition.sample_gp.toFixed(0)}` : '—';
  const defRankValue = vsPosition?.rank_label || (vsPosition?.def_rank ? `#${vsPosition.def_rank}` : (vsPosition?.rank ? `#${vsPosition.rank}` : '—'));
  const defRankSub = vsPosition?.position_label ? `vs ${vsPosition.position_label}` : 'Position ranking';

  let summaryText = 'Defense-vs-position data unavailable for this player and stat.';
  if (vsPosition) {
    summaryText = `${nextGame?.opponent_name || 'This opponent'} allows ${oppVal} ${getStatLabel(vsPosition.stat).toLowerCase()} per player-game to ${String(vsPosition.position_label || 'this position').toLowerCase()}, versus a league baseline of ${lgVal} (${delta}).`;
  }

  matchupSection.classList.remove('hidden');
  tile.className = `card glass selected-player-matchup-tile matchup-panel-inline matchup-tone-${leanTone} fade-in-up`;
  tile.innerHTML = `
    <div class="section-head compact-inline-head">
      <div class="matchup-head-copy">
        <p class="section-kicker">Next Matchup</p>
        <h3>Opponent context</h3>
        <small>${escapeHtml(nextGameLabel)}</small>
      </div>
      <span class="spotlight-pill ${leanTone}">${escapeHtml(leanText)}</span>
    </div>
    <div class="selected-matchup-grid">
      <article class="matchup-tile matchup-stat-tile">
        <span class="small-label">Next game</span>
        <strong>${escapeHtml(nextGame?.opponent_name || 'Unavailable')}</strong>
        <small>${escapeHtml(nextGameLabel)}</small>
      </article>
      <article class="matchup-tile matchup-stat-tile">
        <span class="small-label">Venue</span>
        <strong>${escapeHtml(venueLabel)}</strong>
        <small>${escapeHtml(nextGame?.player_team_abbreviation || 'NBA')}</small>
      </article>
      <article class="matchup-tile matchup-stat-tile">
        <span class="small-label">Availability</span>
        <strong>${renderAvailabilityBadge(availability, true) || '—'}</strong>
        <small>${escapeHtml(availability?.reason || availability?.note || 'Official status unavailable')}</small>
      </article>
      <article class="matchup-tile matchup-stat-tile">
        <span class="small-label">Vs position</span>
        <strong>${escapeHtml(vsPosition?.position_label || 'Unavailable')}</strong>
        <small>${escapeHtml(vsPosition ? getStatLabel(vsPosition.stat) : 'Defense-vs-position unavailable')}</small>
      </article>
      <article class="matchup-tile matchup-stat-tile">
        <span class="small-label">Opponent allow rate</span>
        <strong>${escapeHtml(String(oppVal))}</strong>
        <small>League avg ${escapeHtml(String(lgVal))}</small>
      </article>
      <article class="matchup-tile matchup-stat-tile">
        <span class="small-label">Delta vs average</span>
        <strong class="${leanTone === 'good' ? 'match-good' : leanTone === 'bad' ? 'match-bad' : 'match-neutral'}">${escapeHtml(delta)}</strong>
        <small>${escapeHtml(vsPosition?.lean || 'Neutral')}</small>
      </article>
      <article class="matchup-tile matchup-stat-tile">
        <span class="small-label">Sample</span>
        <strong>${escapeHtml(String(sample))}</strong>
        <small>${vsPosition ? `player-games vs ${escapeHtml(nextGame?.opponent_abbreviation || '')}` : 'No sample'}</small>
      </article>
      <article class="matchup-tile matchup-stat-tile matchup-rank-tile">
        <span class="small-label">DEF RANK</span>
        <strong>${escapeHtml(String(defRankValue))}</strong>
        <small>${escapeHtml(defRankSub)}</small>
      </article>
    </div>
    <div class="matchup-summary matchup-summary-inline">
      <span class="insight-summary-label">Matchup read</span>
      <strong>${escapeHtml(leanText)}</strong>
      <p>${escapeHtml(summaryText)}</p>
    </div>
  `;
  return '';
}

function renderSelectedPlayer() {
  if (!selectedPlayer) {
    selectedPlayerBadge.className = 'selected-player selected-player-empty';
    selectedPlayerBadge.innerHTML = `
      <div class="selected-player-avatar placeholder-avatar">NBA</div>
      <div class="selected-player-copy">
        <span class="selected-player-label">Waiting for a selection</span>
        <strong>No player selected</strong>
        <small>Choose a team, then click a player card to start.</small>
      </div>
    `;
    renderSelectedPlayerContext();
    renderOverviewSelection();
    updateStickyAnalyzerSummary();
    return;
  }

  const subLine = [
    selectedPlayer.team_name || selectedPlayer.team_abbreviation || '',
    selectedPlayer.position || '',
    selectedPlayer.jersey ? `#${selectedPlayer.jersey}` : ''
  ].filter(Boolean).join(' • ');
  const contextData = getSelectedPlayerContextData();
  const availabilitySource = contextData.availability || selectedPlayer.availability;
  const availabilityHtml = availabilitySource
    ? `<div class="selected-player-meta-row">${renderAvailabilityBadge(availabilitySource)}<small>${escapeHtml(availabilitySource.reason || availabilitySource.note || '')}</small></div>`
    : '';

  selectedPlayerBadge.className = 'selected-player';
  selectedPlayerBadge.innerHTML = `
    <img src="${getPlayerImage(selectedPlayer.id)}" alt="${escapeHtml(selectedPlayer.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
    <div class="selected-player-copy">
      <span class="selected-player-label">Selected player</span>
      <strong>${escapeHtml(selectedPlayer.full_name)}</strong>
      <small>${escapeHtml(subLine || (selectedPlayer.is_active ? 'Active player' : 'Player'))}</small>
      ${availabilityHtml}
    </div>
  `;
  renderSelectedPlayerContext();
  renderOverviewSelection();
  updateStickyAnalyzerSummary();
}

function clearAnalysisForNewSelection() {
  resetDashboardForNoSelection();

  if (recommendationToneEl) {
    recommendationToneEl.className = 'spotlight-pill neutral';
    recommendationToneEl.textContent = 'Waiting for analysis';
  }
  if (recommendationBodyEl) {
    recommendationBodyEl.className = 'empty-state-panel compact matchup-empty';
    recommendationBodyEl.innerHTML = `
      <div class="empty-icon">🚦</div>
      <strong>No recommendation yet.</strong>
      <span>Analyze a player prop to generate a green / yellow / red read.</span>
    `;
  }
  if (interpretationTone) {
    interpretationTone.className = 'spotlight-pill neutral';
    interpretationTone.textContent = 'Waiting for analysis';
  }
  if (opportunityTone) {
    opportunityTone.className = 'spotlight-pill neutral';
    opportunityTone.textContent = 'Waiting for analysis';
  }
  if (environmentTone) {
    environmentTone.className = 'spotlight-pill neutral';
    environmentTone.textContent = 'Waiting for analysis';
  }
  if (interpretationBody) {
    interpretationBody.className = 'interpretation-body';
    interpretationBody.innerHTML = `
      <div class="insight-summary neutral">
        <span class="insight-summary-label">Quick read</span>
        <strong>Waiting for analysis</strong>
        <p>Select a player and run the analyzer to generate a simple interpretation.</p>
      </div>
      <ul class="insight-bullet-list compact-bullets"><li>Recent trend, matchup, and opportunity notes will appear here.</li></ul>
    `;
  }
  if (opportunityBody) {
    opportunityBody.className = 'opportunity-body';
    opportunityBody.innerHTML = `
      <div class="opportunity-chip-grid refined-opportunity-grid">
        <div class="opportunity-chip"><span class="small-label">Minutes</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="opportunity-chip"><span class="small-label">FGA</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="opportunity-chip"><span class="small-label">3PA</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="opportunity-chip"><span class="small-label">FTA</span><strong>—</strong><small>Waiting for analysis</small></div>
      </div>
      <div class="opportunity-focus-card neutral"><span class="insight-summary-label">Prop focus</span><strong>Prop-specific context will appear here after analysis.</strong></div>
      <div class="opportunity-summary-wrap refined-opportunity-wrap">
        <div class="insight-summary neutral compact-summary"><span class="insight-summary-label">Opportunity read</span><p class="opportunity-summary">Analyze a player prop to load minutes, attempts, and team context.</p></div>
        <div class="team-context-box neutral"><strong>Team context</strong><p>No team-availability context yet.</p><small>Latest same-team absences will appear here after analysis.</small></div>
      </div>
    `;
  }
  if (environmentBody) {
    environmentBody.className = 'environment-body';
    environmentBody.innerHTML = `
      <div class="environment-chip-grid">
        <div class="environment-chip"><span class="small-label">Venue</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="environment-chip"><span class="small-label">Rest</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="environment-chip"><span class="small-label">Back-to-back</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="environment-chip"><span class="small-label">Games in 7</span><strong>—</strong><small>Waiting for analysis</small></div>
      </div>
      <div class="insight-summary neutral compact-summary"><span class="insight-summary-label">Schedule read</span><p class="opportunity-summary">Analyze a player prop to see rest, back-to-back risk, and the upcoming spot.</p></div>
    `;
  }
  updateStickyAnalyzerSummary();
}


function saveLatestMarketResults(payload) {
  const snapshot = {
    updated_at: new Date().toISOString(),
    results: (payload.results || []).slice(0, 12)
  };
  localStorage.setItem(MARKET_RESULTS_KEY, JSON.stringify(snapshot));
}



function getMarketFilterLabel(filterKey) {
  if (filterKey === 'positive_ev') return 'Filter: +EV';
  if (filterKey === 'edge_5') return 'Filter: Edge ≥ 5%';
  if (filterKey === 'win_rate_60') return 'Filter: Win Rate ≥ 60%';
  if (filterKey === 'available_only') return 'Filter: Available';
  if (filterKey === 'good_matchup') return 'Filter: Good Matchup';
  return 'Filter: All';
}

function filterMarketRows(rows, filterKey) {
  const list = Array.isArray(rows) ? rows : [];
  if (filterKey === 'positive_ev') {
    return list.filter(item => Number(item?.best_bet?.ev ?? Number.NEGATIVE_INFINITY) > 0);
  }
  if (filterKey === 'edge_5') {
    return list.filter(item => Number(item?.best_bet?.edge ?? Number.NEGATIVE_INFINITY) >= 5);
  }
  if (filterKey === 'win_rate_60') {
    return list.filter(item => Number(item?.analysis?.hit_rate ?? Number.NEGATIVE_INFINITY) >= 60);
  }
  if (filterKey === 'available_only') {
    return list.filter(item => {
      const availability = item?.availability || item?.analysis?.availability;
      if (!availability) return true;
      const status = String(availability?.status || '').toLowerCase();
      return !status.includes('out') && !status.includes('doubtful');
    });
  }
  if (filterKey === 'good_matchup') {
    return list.filter(item => {
      const tone = String(item?.analysis?.matchup?.vs_position?.lean_tone || item?.matchup?.vs_position?.lean_tone || '').toLowerCase();
      return tone === 'good';
    });
  }
  return list;
}

function renderMarketFilterChips() {
  if (!marketFilterChips) return;
  const filters = [
    ['all', 'All'],
    ['positive_ev', '+EV'],
    ['edge_5', 'Edge ≥ 5%'],
    ['win_rate_60', 'Win Rate ≥ 60%'],
    ['available_only', 'Available'],
    ['good_matchup', 'Good Matchup'],
  ];
  marketFilterChips.innerHTML = filters.map(([key, label]) => `
    <button class="market-filter-chip ${currentMarketFilter === key ? 'active' : ''}" type="button" data-filter-key="${key}">${label}</button>
  `).join('');
}

function getMarketSortLabel(sortKey) {
  if (sortKey === 'best_edge') return 'Sorted by best edge';
  if (sortKey === 'highest_hit_rate') return 'Sorted by highest win rate';
  if (sortKey === 'best_combo') return 'Sorted by best combo';
  return 'Sorted by best EV';
}

function getMarketSortValue(item, sortKey) {
  const ev = Number(item?.best_bet?.ev ?? Number.NEGATIVE_INFINITY);
  const edge = Number(item?.best_bet?.edge ?? Number.NEGATIVE_INFINITY);
  const hitRate = Number(item?.analysis?.hit_rate ?? Number.NEGATIVE_INFINITY);
  const confidence = Number(item?.best_bet?.confidence_score ?? Number.NEGATIVE_INFINITY);
  const availabilityRank = Number(item?.availability?.sort_rank ?? 3);

  if (sortKey === 'best_edge') return [edge, ev, hitRate, confidence, -availabilityRank];
  if (sortKey === 'highest_hit_rate') return [hitRate, ev, edge, confidence, -availabilityRank];
  if (sortKey === 'best_combo') return [confidence, ev, edge, hitRate, -availabilityRank];
  return [ev, edge, hitRate, confidence, -availabilityRank];
}

function compareMarketRows(a, b, sortKey, direction = currentMarketSortDirection || 'desc') {
  const av = getMarketSortValue(a, sortKey);
  const bv = getMarketSortValue(b, sortKey);
  const multiplier = direction === 'asc' ? 1 : -1;
  for (let i = 0; i < av.length; i += 1) {
    if (av[i] === bv[i]) continue;
    return (av[i] - bv[i]) * multiplier;
  }
  return 0;
}

function getMarketSortArrow(sortKey) {
  if (currentMarketSort !== sortKey) return '↕';
  return currentMarketSortDirection === 'asc' ? '↑' : '↓';
}

function getMarketAriaSort(sortKey) {
  if (currentMarketSort !== sortKey) return 'none';
  return currentMarketSortDirection === 'asc' ? 'ascending' : 'descending';
}

function toggleMarketSort(sortKey) {
  if (!sortKey) return;
  if (currentMarketSort === sortKey) {
    currentMarketSortDirection = currentMarketSortDirection === 'desc' ? 'asc' : 'desc';
  } else {
    currentMarketSort = sortKey;
    currentMarketSortDirection = 'desc';
  }
  localStorage.setItem('nba-props-market-sort', currentMarketSort);
  localStorage.setItem('nba-props-market-sort-direction', currentMarketSortDirection);
}

// duplicate renderMarketResults removed; using primary implementation above


async function loadTodayGames(force = false) {
  if (latestTodayGamesPayload && !force) {
    renderTodayGames(latestTodayGamesPayload);
    return;
  }

  if (todayGamesMeta) todayGamesMeta.textContent = "Loading today's NBA slate...";
  if (overviewTodayMeta) overviewTodayMeta.textContent = "Fetching today's slate...";
  if (todayGamesBoard) todayGamesBoard.innerHTML = `<div class="empty-state-panel compact skeleton-panel today-game-empty"><div class="empty-icon">🗓️</div><strong>Loading games...</strong><span>Pulling the current slate and report context.</span><div class="skeleton-line"></div><div class="skeleton-line short"></div></div>`;
  if (overviewTodayGames) overviewTodayGames.innerHTML = `<div class="empty-state-panel compact skeleton-panel today-game-empty"><div class="empty-icon">🗓️</div><strong>Loading games...</strong><span>Pulling the current slate and report context.</span></div>`;
  try {
    const response = await fetch('/api/todays-games');
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || "Failed to load today's games.");
    }
    renderTodayGames(payload);
  } catch (error) {
    console.error(error);
    const fallbackMessage = error.message || "Failed to load today's slate.";
    if (todayGamesBoard) {
      todayGamesBoard.innerHTML = `<div class="empty-state-panel compact today-game-empty"><div class="empty-icon">⚠️</div><strong>Could not load today&#39;s games.</strong><span>${escapeHtml(fallbackMessage)}</span></div>`;
    }
    if (overviewTodayGames) {
      overviewTodayGames.innerHTML = `<div class="empty-state-panel compact today-game-empty"><div class="empty-icon">⚠️</div><strong>Could not load the slate.</strong><span>${escapeHtml(fallbackMessage)}</span></div>`;
    }
  }
}


/* === Analyzer filter upgrade === */
const filtersBadgeEl = document.getElementById('filtersBadge');
const activeFiltersSummaryEl = document.getElementById('activeFiltersSummary');
const toggleAdvancedFiltersBtnEl = document.getElementById('toggleAdvancedFiltersBtn');
const advancedFiltersPanelEl = document.getElementById('advancedFiltersPanel');
const quickFilterChipButtonsEl = document.querySelectorAll('#quickFilterChips .filter-chip');
const filterLocationSelectEl = document.getElementById('filterLocationSelect');
const filterResultSelectEl = document.getElementById('filterResultSelect');
const filterMarginMinEl = document.getElementById('filterMarginMin');
const filterMarginMaxEl = document.getElementById('filterMarginMax');
const filterMinMinutesEl = document.getElementById('filterMinMinutes');
const filterMaxMinutesEl = document.getElementById('filterMaxMinutes');
const filterMinFgaEl = document.getElementById('filterMinFga');
const filterMaxFgaEl = document.getElementById('filterMaxFga');
const filterH2HOnlyEl = document.getElementById('filterH2HOnly');
const applyFiltersBtnEl = document.getElementById('applyFiltersBtn');
const resetFiltersBtnEl = document.getElementById('resetFiltersBtn');

function parseNullableNumber(value) {
  if (value === null || value === undefined) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const number = Number(raw);
  return Number.isFinite(number) ? number : null;
}

function getAnalyzerFiltersState() {
  return {
    location: filterLocationSelectEl?.value || 'all',
    result: filterResultSelectEl?.value || 'all',
    margin_min: parseNullableNumber(filterMarginMinEl?.value),
    margin_max: parseNullableNumber(filterMarginMaxEl?.value),
    min_minutes: parseNullableNumber(filterMinMinutesEl?.value),
    max_minutes: parseNullableNumber(filterMaxMinutesEl?.value),
    min_fga: parseNullableNumber(filterMinFgaEl?.value),
    max_fga: parseNullableNumber(filterMaxFgaEl?.value),
    h2h_only: !!filterH2HOnlyEl?.checked,
  };
}

function setAnalyzerFiltersState(state = {}) {
  if (filterLocationSelectEl) filterLocationSelectEl.value = state.location || 'all';
  if (filterResultSelectEl) filterResultSelectEl.value = state.result || 'all';
  if (filterMarginMinEl) filterMarginMinEl.value = state.margin_min ?? '';
  if (filterMarginMaxEl) filterMarginMaxEl.value = state.margin_max ?? '';
  if (filterMinMinutesEl) filterMinMinutesEl.value = state.min_minutes ?? '';
  if (filterMaxMinutesEl) filterMaxMinutesEl.value = state.max_minutes ?? '';
  if (filterMinFgaEl) filterMinFgaEl.value = state.min_fga ?? '';
  if (filterMaxFgaEl) filterMaxFgaEl.value = state.max_fga ?? '';
  if (filterH2HOnlyEl) filterH2HOnlyEl.checked = !!state.h2h_only;
  refreshFilterChipStates();
  updateFilterSummaryUpgrade();
}

function resetAnalyzerFiltersState() {
  [filterLocationSelectEl, filterResultSelectEl, filterMarginMinEl, filterMarginMaxEl, filterMinMinutesEl, filterMaxMinutesEl, filterMinFgaEl, filterMaxFgaEl, filterH2HOnlyEl].forEach(el => {
    el?.addEventListener('input', () => { refreshFilterChipStates(); updateFilterSummaryUpgrade(); });
    el?.addEventListener('change', () => { refreshFilterChipStates(); updateFilterSummaryUpgrade(); });
  });

  setAnalyzerFiltersState({ location: 'all', result: 'all', h2h_only: false });
}

function buildFilterSummaryCopy(filters) {
  const chips = [];
  if (filters.location === 'home') chips.push('Home');
  else if (filters.location === 'away') chips.push('Away');
  if (filters.result === 'win') chips.push('Wins');
  else if (filters.result === 'loss') chips.push('Losses');
  if (filters.margin_min !== null || filters.margin_max !== null) {
    if (filters.margin_min !== null && filters.margin_max !== null) chips.push(`Margin ${filters.margin_min}-${filters.margin_max}`);
    else if (filters.margin_min !== null) chips.push(`Margin ≥ ${filters.margin_min}`);
    else chips.push(`Margin ≤ ${filters.margin_max}`);
  }
  if (filters.min_minutes !== null || filters.max_minutes !== null) {
    if (filters.min_minutes !== null && filters.max_minutes !== null) chips.push(`MIN ${filters.min_minutes}-${filters.max_minutes}`);
    else if (filters.min_minutes !== null) chips.push(`MIN ≥ ${filters.min_minutes}`);
    else chips.push(`MIN ≤ ${filters.max_minutes}`);
  }
  if (filters.min_fga !== null || filters.max_fga !== null) {
    if (filters.min_fga !== null && filters.max_fga !== null) chips.push(`FGA ${filters.min_fga}-${filters.max_fga}`);
    else if (filters.min_fga !== null) chips.push(`FGA ≥ ${filters.min_fga}`);
    else chips.push(`FGA ≤ ${filters.max_fga}`);
  }
  if (filters.h2h_only) chips.push('H2H only');
  return { label: chips.length ? chips.join(' • ') : 'All games', hasFilters: chips.length > 0, chips };
}

function updateFilterSummaryUpgrade(payload = null) {
  const filterSummary = payload?.active_filters || buildFilterSummaryCopy(getAnalyzerFiltersState());
  if (filtersBadgeEl) {
    filtersBadgeEl.textContent = filterSummary.label || 'All games';
    filtersBadgeEl.className = `spotlight-pill ${filterSummary.has_filters ? 'good' : 'neutral'}`;
  }
  if (activeFiltersSummaryEl) {
    if (payload?.filtered_pool_count !== undefined && payload?.season_pool_count !== undefined) {
      activeFiltersSummaryEl.textContent = `${filterSummary.label || 'All games'} • Using ${payload.filtered_pool_count} of ${payload.season_pool_count} season games.`;
    } else {
      activeFiltersSummaryEl.textContent = filterSummary.hasFilters
        ? `${filterSummary.label} • Applied to the analyzer, chart, and game log.`
        : 'No active filters. Using all games in the selected season.';
    }
  }
}

function detectPresetMatch(filters) {
  const checks = {
    all: !buildFilterSummaryCopy(filters).hasFilters,
    home: filters.location === 'home' && !filters.result && false,
  };
  if (filters.location === 'home' && filters.result === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && !filters.h2h_only) return 'home';
  if (filters.location === 'away' && filters.result === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && !filters.h2h_only) return 'away';
  if (filters.result === 'win' && filters.location === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && !filters.h2h_only) return 'wins';
  if (filters.result === 'loss' && filters.location === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && !filters.h2h_only) return 'losses';
  if (filters.h2h_only && filters.location === 'all' && filters.result === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null) return 'h2h';
  if (filters.min_minutes === 30 && filters.max_minutes === null && filters.location === 'all' && filters.result === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_fga === null && filters.max_fga === null && !filters.h2h_only) return 'min30';
  if (filters.margin_min === 0 && filters.margin_max === 5 && filters.location === 'all' && filters.result === 'all' && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && !filters.h2h_only) return 'close';
  if (filters.margin_min === 11 && filters.margin_max === null && filters.location === 'all' && filters.result === 'all' && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && !filters.h2h_only) return 'blowout';
  return buildFilterSummaryCopy(filters).hasFilters ? '' : 'all';
}

function refreshFilterChipStates() {
  const preset = detectPresetMatch(getAnalyzerFiltersState());
  quickFilterChipButtonsEl.forEach(btn => btn.classList.toggle('active', btn.dataset.filterPreset === preset));
}

function applyFilterPresetUpgrade(preset) {
  const state = { location: 'all', result: 'all', h2h_only: false };
  if (preset === 'home') state.location = 'home';
  else if (preset === 'away') state.location = 'away';
  else if (preset === 'wins') state.result = 'win';
  else if (preset === 'losses') state.result = 'loss';
  else if (preset === 'h2h') state.h2h_only = true;
  else if (preset === 'min30') state.min_minutes = 30;
  else if (preset === 'close') { state.margin_min = 0; state.margin_max = 5; }
  else if (preset === 'blowout') { state.margin_min = 11; }
  setAnalyzerFiltersState(state);
  if (selectedPlayer) analyzePlayerProp({ preserveScroll: true, preserveSection: true, forceRefresh: true });
}

function getAnalyzerFocusScrollTarget() {
  return document.querySelector('.chart-card') || document.querySelector('.analyzer-filter-card') || document.querySelector('[data-view="analyzer"]');
}

function scrollAnalyzerFocusIntoView() {
  const target = getAnalyzerFocusScrollTarget();
  if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function renderEmptyFilterStatesUpgrade() {
  if (!gameLogMeta) return;
  gameLogMeta.textContent = 'No games matched the active filters';
  gamesTableBody.innerHTML = `
    <tr>
      <td colspan="8">
        <div class="empty-state-panel compact">
          <div class="empty-icon">🧪</div>
          <strong>No games matched the filters.</strong>
          <span>Relax the current split or reset the filters to repopulate the trend view and game log.</span>
        </div>
      </td>
    </tr>
  `;
  if (chartTitle) chartTitle.textContent = 'No games matched the active filters';
  if (chartSubtitle) chartSubtitle.textContent = 'Adjust or reset the analyzer filters to rebuild the trend view.';
  if (chartChips) chartChips.innerHTML = '<span class="chart-chip">No filtered sample</span>';
  if (chart) { chart.destroy(); chart = null; }
}

function computeConfidenceGrade(hitRate, edge, gamesCount) {
  const safeEdge = Number.isFinite(edge) ? edge : 0;
  const score = (hitRate * 0.5) + (Math.max(0, safeEdge) * 1.5) + Math.min(gamesCount, 15);
  if (score >= 62) return 'A';
  if (score >= 50) return 'B';
  if (score >= 38) return 'C';
  return 'D';
}
function gradeLabel(grade) {
  return { A: 'Elite', B: 'Strong', C: 'Playable', D: 'Thin' }[grade] || '—';
}
function buildSplitChips(games, line) {
  if (!games || !games.length) return '';
  const home = games.filter(g => g.is_home === true);
  const away = games.filter(g => g.is_home === false);
  function hitPct(list) {
    if (!list.length) return null;
    return ((list.filter(g => g.hit).length / list.length) * 100).toFixed(0) + '%';
  }
  let html = '';
  const h = hitPct(home), a = hitPct(away);
  if (h) html += `<span class="chart-chip">Home ${h}</span>`;
  if (a) html += `<span class="chart-chip">Away ${a}</span>`;
  return html;
}
function renderSummary(payload) {
  const streak = computeOverStreak(payload.games || []);
  const lastGame = payload.games?.[payload.games.length - 1];
  avgValue.textContent = Number(payload.average || 0).toFixed(1);
  hitRateValue.textContent = `${Number(payload.hit_rate || 0).toFixed(1)}%`;
  hitCountValue.textContent = `${payload.hit_count || 0}/${payload.games_count || 0}`;
  seasonValue.textContent = payload.season;
  streakValue.textContent = streak ? `${streak} straight` : '0';
  lastGameValue.textContent = lastGame ? `${lastGame.value}` : '—';

  const nextGame = payload.matchup?.next_game;
  const vsPosition = payload.matchup?.vs_position;
  const h2h = payload.h2h || {};
  const grade = computeConfidenceGrade(
    Number(payload.hit_rate || 0),
    Number(payload.average || 0) - Number(payload.line || 0),
    payload.games_count || 0
  );

  chartTitle.textContent = `${payload.player.full_name} • ${getStatLabel(payload.stat)}`;

  // Running hit-rate subtitle with grade + streak
  const hitRatePct = Number(payload.hit_rate || 0).toFixed(1);
  let subtitleText = `${hitRatePct}% hit rate (${payload.hit_count || 0}/${payload.games_count || 0}) • Grade ${grade}: ${gradeLabel(grade)}`;
  if (streak > 1) subtitleText += ` • 🔥 ${streak} straight`;
  if (nextGame) subtitleText += ` • vs ${nextGame.matchup_label}`;
  chartSubtitle.textContent = subtitleText;

  const splitHtml = buildSplitChips(payload.games || [], payload.line);
  chartChips.innerHTML = `
    <span class="chart-chip">Avg ${Number(payload.average || 0).toFixed(1)}</span>
    <span class="chart-chip">${payload.hit_count || 0}/${payload.games_count || 0} overs</span>
    <span class="chart-chip grade-chip grade-${grade.toLowerCase()}">${grade} ${gradeLabel(grade)}</span>
    <span class="chart-chip">Season ${escapeHtml(payload.season || '')}</span>
    <span class="chart-chip">Streak ${streak}</span>
    ${splitHtml}
    ${payload.active_filters?.label ? `<span class="chart-chip">${escapeHtml(payload.active_filters.label)}</span>` : ''}
    ${payload.filtered_pool_count !== undefined ? `<span class="chart-chip">Pool ${payload.filtered_pool_count}/${payload.season_pool_count}</span>` : ''}
    ${payload.availability ? `<span class="chart-chip">Status ${escapeHtml(payload.availability.status)}</span>` : ''}
    ${nextGame ? `<span class="chart-chip">Next ${escapeHtml(nextGame.matchup_label)}</span>` : ''}
    ${vsPosition ? `<span class="chart-chip">Vs ${escapeHtml(vsPosition.position_label)} ${formatDelta(vsPosition.delta_pct)}%</span>` : ''}
    ${h2h.games_count ? `<span class="chart-chip">H2H ${h2h.hit_count}/${h2h.games_count} vs ${escapeHtml(h2h.opponent_abbreviation || h2h.opponent_name || 'opponent')}</span>` : ''}
  `;

  renderMatchup(payload);
  renderInterpretationPanels(payload);
  updateFilterSummaryUpgrade(payload);
}

async function analyzePlayerProp(options = {}) {
  switchView('analyzer', { scroll: !options.preserveScroll });
  if (!selectedPlayer) {
    alert('Please select a player first.');
    return;
  }

  setStatus('Loading');
  analyzeBtn.disabled = true;

  try {
    const params = new URLSearchParams({
      player_id: selectedPlayer.id,
      stat: selectedStat,
      line: lineInput.value,
      last_n: gamesSelect.value
    });
    if (selectedPlayer.team_id) params.set('team_id', selectedPlayer.team_id);
    if (selectedPlayer.position) params.set('player_position', selectedPlayer.position);
    const season = seasonInput.value.trim();
    if (season) params.set('season', season);

    const filters = getAnalyzerFiltersState();
    if (filters.location && filters.location !== 'all') params.set('location', filters.location);
    if (filters.result && filters.result !== 'all') params.set('result', filters.result);
    if (filters.margin_min !== null) params.set('margin_min', String(filters.margin_min));
    if (filters.margin_max !== null) params.set('margin_max', String(filters.margin_max));
    if (filters.min_minutes !== null) params.set('min_minutes', String(filters.min_minutes));
    if (filters.max_minutes !== null) params.set('max_minutes', String(filters.max_minutes));
    if (filters.min_fga !== null) params.set('min_fga', String(filters.min_fga));
    if (filters.max_fga !== null) params.set('max_fga', String(filters.max_fga));
    if (filters.h2h_only) params.set('h2h_only', 'true');

    const overrideOppId = oppSelect?.value;
    if (overrideOppId) params.set('override_opponent_id', overrideOppId);

    const response = await fetch(`/api/player-prop?${params.toString()}`);
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || 'Failed to analyze player prop.');

    selectedPlayer = {
      ...selectedPlayer,
      team_id: payload.player.team_id || selectedPlayer.team_id,
      position: payload.player.position || selectedPlayer.position,
      availability: payload.availability || null,
      matchup: payload.matchup || null,
      environment: payload.environment || null
    };
    renderSelectedPlayer();
    renderSummary(payload);
    if (payload.games && payload.games.length) {
      renderChart(payload);
      renderTable(payload);
    } else {
      renderEmptyFilterStatesUpgrade();
      renderInterpretationPanels(payload);
      updateFilterSummaryUpgrade(payload);
    }
    setStatus('Ready');
    if (options.preserveSection) {
      requestAnimationFrame(() => scrollAnalyzerFocusIntoView());
    }
  } catch (error) {
    console.error(error);
    // Show visual error in chart area instead of alert
    if (typeof renderChartError === 'function') {
      renderChartError(error.message || 'Analysis failed. Please try again.');
    } else {
      alert(error.message);
    }
    setStatus('Error');
  } finally {
    analyzeBtn.disabled = false;
  }
}

toggleAdvancedFiltersBtnEl?.addEventListener('click', () => {
  advancedFiltersPanelEl?.classList.toggle('hidden');
  if (toggleAdvancedFiltersBtnEl) toggleAdvancedFiltersBtnEl.textContent = advancedFiltersPanelEl?.classList.contains('hidden') ? 'Advanced' : 'Hide';
});

quickFilterChipButtonsEl.forEach(btn => btn.addEventListener('click', () => applyFilterPresetUpgrade(btn.dataset.filterPreset || 'all')));
applyFiltersBtnEl?.addEventListener('click', async () => {
  updateFilterSummaryUpgrade();
  if (selectedPlayer) await analyzePlayerProp({ preserveScroll: true, preserveSection: true, forceRefresh: true });
});
resetFiltersBtnEl?.addEventListener('click', async () => {
  resetAnalyzerFiltersState();
  if (selectedPlayer) await analyzePlayerProp({ preserveScroll: true, preserveSection: true, forceRefresh: true });
});

[filterLocationSelectEl, filterResultSelectEl, filterMarginMinEl, filterMarginMaxEl, filterMinMinutesEl, filterMaxMinutesEl, filterMinFgaEl, filterMaxFgaEl, filterH2HOnlyEl].forEach(el => {
  el?.addEventListener('input', () => { refreshFilterChipStates(); updateFilterSummaryUpgrade(); });
  el?.addEventListener('change', () => { refreshFilterChipStates(); updateFilterSummaryUpgrade(); });
});

setAnalyzerFiltersState({ location: 'all', result: 'all', h2h_only: false });


/* ═══════════════════════════════════════════════════════════════════════
   IMPROVEMENT PATCH — Features 7-13
   ═══════════════════════════════════════════════════════════════════════ */

/* ── Session cache helpers ─────────────────────────────────────── */
function getPropCacheKey(playerId, stat, line, lastN, filters) {
  const state = filters || (typeof getAnalyzerFiltersState === 'function' ? getAnalyzerFiltersState() : {});
  const filterKey = [
    state.location || 'all',
    state.result || 'all',
    state.margin_min ?? '',
    state.margin_max ?? '',
    state.min_minutes ?? '',
    state.max_minutes ?? '',
    state.min_fga ?? '',
    state.max_fga ?? '',
    state.h2h_only ? '1' : '0'
  ].join(':');
  return `prop:${playerId}:${stat}:${line}:${lastN}:${filterKey}`;
}
function readPropCache(key) {
  try {
    const raw = sessionStorage.getItem(key);
    if (!raw) return null;
    const { ts, payload } = JSON.parse(raw);
    if (Date.now() - ts > 5 * 60 * 1000) return null; // 5-min TTL
    return payload;
  } catch { return null; }
}
function writePropCache(key, payload) {
  try {
    sessionStorage.setItem(key, JSON.stringify({ ts: Date.now(), payload }));
  } catch { /* quota */ }
}

/* ── Chart error overlay ───────────────────────────────────────── */
function renderChartError(message) {
  const canvas = document.getElementById('propsChart');
  if (!canvas) return;
  if (chart) { chart.destroy(); chart = null; }
  const wrap = canvas.parentElement;
  wrap.querySelectorAll('.chart-error-overlay').forEach(el => el.remove());
  const overlay = document.createElement('div');
  overlay.className = 'chart-error-overlay';
  overlay.innerHTML = `
    <div class="chart-error-inner">
      <span class="chart-error-icon">⚠️</span>
      <strong>Chart unavailable</strong>
      <small>${escapeHtml(message || 'An error occurred loading the trend data.')}</small>
      <button class="secondary-btn compact-btn" type="button" onclick="this.closest('.chart-error-overlay').remove();if(selectedPlayer)analyzePlayerProp({preserveScroll:true,forceRefresh:true})">Retry</button>
    </div>`;
  wrap.appendChild(overlay);
}

/* ── Mini sparkline SVG for Bet Finder cards ──────────────────── */
function buildSparklineSvg(values, line, width, height) {
  width = width || 72; height = height || 26;
  if (!values || values.length < 2) return '';
  const allVals = values.concat([line]);
  const min = Math.min.apply(null, allVals) * 0.88;
  const max = (Math.max.apply(null, allVals) * 1.08) || 1;
  const range = max - min || 1;
  function toX(i) { return (i / (values.length - 1)) * width; }
  function toY(v) { return height - ((v - min) / range) * height; }
  const pts = values.map(function(v, i) { return toX(i).toFixed(1) + ',' + toY(v).toFixed(1); }).join(' ');
  const lineY = toY(line).toFixed(1);
  const dots = values.map(function(v, i) {
    const hit = v >= line;
    return '<circle cx="' + toX(i).toFixed(1) + '" cy="' + toY(v).toFixed(1) + '" r="2.2" fill="' + (hit ? 'var(--good)' : 'var(--bad)') + '" opacity="0.9"/>';
  }).join('');
  return '<svg viewBox="0 0 ' + width + ' ' + height + '" width="' + width + '" height="' + height + '" xmlns="http://www.w3.org/2000/svg" style="display:block;overflow:visible">'
    + '<line x1="0" y1="' + lineY + '" x2="' + width + '" y2="' + lineY + '" stroke="var(--warning)" stroke-width="1" stroke-dasharray="3,2" opacity="0.75"/>'
    + '<polyline points="' + pts + '" fill="none" stroke="var(--accent)" stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"/>'
    + dots
    + '</svg>';
}

/* ── Inject sparklines + grade into Bet Finder results ─────────── */
(function patchBetFinderSparklines() {
  const _orig = renderBetFinderResults;
  renderBetFinderResults = function(payload) {
    _orig.call(this, payload);
    const results = payload.results || [];
    document.querySelectorAll('.finder-card').forEach(function(card, idx) {
      const item = results[idx];
      if (!item) return;
      const values = (item.games || []).map(function(g) { return Number(g.value || 0); });
      const grade = computeConfidenceGrade(item.hit_rate, item.avg_edge || 0, item.games_count || 0);
      const footer = card.querySelector('.finder-footer');
      // Sparkline
      if (values.length >= 2 && footer) {
        const svg = buildSparklineSvg(values, payload.line);
        const sparkWrap = document.createElement('div');
        sparkWrap.className = 'finder-sparkline';
        sparkWrap.innerHTML = svg;
        card.insertBefore(sparkWrap, footer);
      }
      // Grade chip
      const chipRow = card.querySelector('.finder-chip-row');
      if (chipRow && !chipRow.querySelector('.grade-chip')) {
        const gradeEl = document.createElement('span');
        gradeEl.className = 'finder-chip grade-chip grade-' + grade.toLowerCase();
        gradeEl.textContent = grade + ' • ' + gradeLabel(grade);
        chipRow.appendChild(gradeEl);
      }
    });
  };
})();

/* ── Session cache: intercept analyzePlayerProp ─────────────────── */
(function patchAnalyzeCache() {
  const _orig = analyzePlayerProp;
  analyzePlayerProp = async function(options) {
    options = options || {};
    if (!selectedPlayer) { alert('Please select a player first.'); return; }

    // Remove stale error overlay
    document.querySelectorAll('.chart-error-overlay').forEach(function(el) { el.remove(); });

    if (!options.forceRefresh) {
      const cacheKey = getPropCacheKey(selectedPlayer.id, selectedStat, lineInput ? lineInput.value : '', gamesSelect ? gamesSelect.value : '', typeof getAnalyzerFiltersState === 'function' ? getAnalyzerFiltersState() : null);
      const cached = readPropCache(cacheKey);
      if (cached) {
        switchView('analyzer', { scroll: !options.preserveScroll });
        // Restore player meta from cache
        if (cached._playerMeta) {
          selectedPlayer = Object.assign({}, selectedPlayer, cached._playerMeta);
        }
        renderSelectedPlayer();
        renderSummary(cached);
        if (cached.games && cached.games.length) {
          renderChart(cached);
          renderTable(cached);
        }
        setStatus('Ready');
        return;
      }
    }

    return _orig.apply(this, arguments);
  };
})();

/* ── Write cache after successful analysis ──────────────────────── */
(function patchPostAnalysis() {
  // Hook into renderChart to write cache after real fetch
  const _origRenderChart = renderChart;
  renderChart = function(payload) {
    _origRenderChart.apply(this, arguments);
    if (!selectedPlayer || !payload) return;
    const cacheKey = getPropCacheKey(selectedPlayer.id, selectedStat, lineInput ? lineInput.value : '', gamesSelect ? gamesSelect.value : '', typeof getAnalyzerFiltersState === 'function' ? getAnalyzerFiltersState() : null);
    const toCache = Object.assign({}, payload);
    toCache._playerMeta = {
      team_id: selectedPlayer.team_id,
      position: selectedPlayer.position,
      availability: selectedPlayer.availability,
      matchup: selectedPlayer.matchup,
      environment: selectedPlayer.environment
    };
    writePropCache(cacheKey, toCache);
  };
})();

/* ── Debounced line input re-analyze ────────────────────────────── */
(function patchLineInputDebounce() {
  if (!lineInput) return;
  lineInput.addEventListener('input', function() {
    if (typeof updateStickyAnalyzerSummary === 'function') updateStickyAnalyzerSummary();
    clearTimeout(window._propLineDebounce);
    window._propLineDebounce = setTimeout(function() {
      if (selectedPlayer && lastPayload) analyzePlayerProp({ preserveScroll: true });
    }, 800);
  });
})();

/* ── Keyboard shortcuts: J/K cycle players, ←/→ cycle stats ──── */
(function bindKeyboardShortcuts() {
  document.addEventListener('keydown', function(e) {
    const tag = (document.activeElement || {}).tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
    if (e.metaKey || e.ctrlKey || e.altKey) return;

    if (e.key === 'j' || e.key === 'k') {
      e.preventDefault();
      const cards = Array.from(document.querySelectorAll('.player-card'));
      if (!cards.length) return;
      const curIdx = selectedPlayer ? cards.findIndex(function(c) { return Number(c.dataset.id) === selectedPlayer.id; }) : -1;
      const nextIdx = e.key === 'j' ? Math.min(curIdx + 1, cards.length - 1) : Math.max(curIdx - 1, 0);
      if (cards[nextIdx]) {
        cards[nextIdx].click();
        cards[nextIdx].scrollIntoView({ block: 'nearest', behavior: 'smooth' });
      }
    }

    if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
      if (!propButtonsWrap) return;
      e.preventDefault();
      const chips = Array.from(propButtonsWrap.querySelectorAll('.prop-chip'));
      if (!chips.length) return;
      const curIdx = chips.findIndex(function(c) { return c.dataset.stat === selectedStat; });
      const nextIdx = e.key === 'ArrowRight' ? Math.min(curIdx + 1, chips.length - 1) : Math.max(curIdx - 1, 0);
      if (chips[nextIdx]) chips[nextIdx].click();
    }
  });
})();

/* ── Restore last player + stat on page load ─────────────────── */
(function restoreLastSession() {
  const lastStat = localStorage.getItem(LAST_STAT_KEY);
  if (lastStat && propButtonsWrap) {
    try { setActiveProp(lastStat); } catch(e) { /* ignore */ }
  }
  const raw = localStorage.getItem(LAST_PLAYER_KEY);
  if (raw) {
    try {
      const player = JSON.parse(raw);
      if (player && player.id) {
        setSelectedPlayer(player);
      }
    } catch(e) { /* ignore */ }
  }
})();

/* ── Patch setActiveProp to persist stat ────────────────────────── */
(function patchSetActivePropPersist() {
  const _orig = setActiveProp;
  setActiveProp = function(stat) {
    _orig.call(this, stat);
    try { localStorage.setItem(LAST_STAT_KEY, stat); } catch(e) { /* ignore */ }
  };
})();

/* ── Runtime CSS for all new UI elements ─────────────────────── */
(function injectPatchStyles() {
  const style = document.createElement('style');
  style.textContent = `
    .chart-wrap { position: relative; }
    .chart-error-overlay {
      position: absolute; inset: 0;
      display: flex; align-items: center; justify-content: center;
      background: rgba(10,16,31,0.88);
      backdrop-filter: blur(6px);
      border-radius: 12px;
      z-index: 10;
    }
    body.light-theme .chart-error-overlay {
      background: rgba(240,244,255,0.92);
    }
    .chart-error-inner {
      display: flex; flex-direction: column; align-items: center; gap: 10px;
      text-align: center; padding: 28px 24px;
    }
    .chart-error-icon { font-size: 2rem; line-height: 1; }
    .chart-error-inner strong { color: var(--text); font-size: 1rem; font-weight: 700; }
    .chart-error-inner small { color: var(--muted); max-width: 280px; display: block; line-height: 1.5; }

    .grade-chip { font-weight: 700; letter-spacing: 0.02em; }
    .grade-a { background: rgba(16,185,129,0.16) !important; color: var(--good) !important; border-color: var(--good) !important; }
    .grade-b { background: rgba(59,130,246,0.16) !important; color: #60a5fa !important; border-color: #3b82f6 !important; }
    .grade-c { background: rgba(251,191,36,0.16) !important; color: var(--warning) !important; border-color: var(--warning) !important; }
    .grade-d { background: rgba(239,68,68,0.13) !important; color: var(--bad) !important; border-color: var(--bad) !important; }

    .finder-sparkline {
      padding: 4px 14px 6px;
      opacity: 0.95;
    }

    .kbd-hint {
      display: inline-block;
      font-size: 9px;
      color: var(--muted);
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 4px;
      padding: 1px 5px;
      margin-left: 4px;
      vertical-align: middle;
      font-family: monospace;
    }
    body.light-theme .kbd-hint {
      background: rgba(0,0,0,0.04);
      border-color: rgba(0,0,0,0.12);
    }
  `;
  document.head.appendChild(style);
})();

/* ── Keyboard hint badges on roster meta ────────────────────────── */
(function addKbdHints() {
  const rosterMetaEl = document.getElementById('rosterMeta');
  if (rosterMetaEl && !rosterMetaEl.querySelector('.kbd-hint')) {
    rosterMetaEl.insertAdjacentHTML('beforeend',
      ' <span class="kbd-hint" title="Press J or K to cycle players">J / K</span>' +
      ' <span class="kbd-hint" title="Press \u2190 or \u2192 to change stat">\u2190 \u2192</span>'
    );
  }
})();

/* ═══════════════════════════════════════════════════════════════════════
   PARLAY BUILDER
═══════════════════════════════════════════════════════════════════════ */
(function initParlayBuilder() {
  const PARLAY_KEYS_STORAGE     = 'nba-props-parlay-keys';
  const PARLAY_SETTINGS_STORAGE = 'nba-props-parlay-settings';

  // ── DOM refs ──────────────────────────────────────────────────────────
  const parlayApiKeys         = document.getElementById('parlayApiKeys');
  const parlaySportSelect     = document.getElementById('parlaySportSelect');
  const parlayOddsFormatSel   = document.getElementById('parlayOddsFormatSelect');
  const parlayLegsSelect      = document.getElementById('parlayLegsSelect');
  const parlayLastNSelect     = document.getElementById('parlayLastNSelect');
  const parlayBookmakerSelect = document.getElementById('parlayBookmakerSelect');
  const parlayLoadEventsBtn   = document.getElementById('parlayLoadEventsBtn');

  // ── Progress bar helper ───────────────────────────────────────────────
  function setParlayProgress(pct, label) {
    var bar  = document.getElementById('parlayProgressBar');
    var lbl  = document.getElementById('parlayProgressLabel');
    var wrap = document.getElementById('parlayProgressWrap');
    if (!wrap) return;
    if (pct <= 0) { wrap.style.display = 'none'; return; }
    wrap.style.display = '';
    if (bar)  { bar.style.width = pct + '%'; bar.setAttribute('aria-valuenow', pct); }
    if (lbl && label) lbl.textContent = label;
  }
  const parlayEventPickerWrap = document.getElementById('parlayEventPickerWrap');
  const parlayEventList       = document.getElementById('parlayEventList');
  const parlaySelectAllBtn    = document.getElementById('parlaySelectAllBtn');
  const parlaySelectNoneBtn   = document.getElementById('parlaySelectNoneBtn');
  const parlayEventSelMeta    = document.getElementById('parlayEventSelectionMeta');
  const parlayBuildWrap       = document.getElementById('parlayBuildWrap');
  const parlayBuildBtn        = document.getElementById('parlayBuildBtn');
  const parlayRebuildBtn      = document.getElementById('parlayRebuildBtn');
  const parlayRescrapeBtn     = document.getElementById('parlayRescrapeBtn');
  const parlayStatusMeta      = document.getElementById('parlayStatusMeta');
  const parlayQuotaBar        = document.getElementById('parlayQuotaBar');
  const parlayQuotaList       = document.getElementById('parlayQuotaList');
  const parlayTicket          = document.getElementById('parlayTicket');
  const parlayTicketOdds      = document.getElementById('parlayTicketOdds');
  const parlayAllPropsWrap    = document.getElementById('parlayAllPropsWrap');
  const parlayAllPropsTitle   = document.getElementById('parlayAllPropsTitle');
  const parlayPropCount       = document.getElementById('parlayPropCount');
  const parlayAllPropsBody    = document.getElementById('parlayAllPropsBody');
  const parlayEmptyState      = document.getElementById('parlayEmptyState');

  if (!parlayLoadEventsBtn) return;

  // ── State ─────────────────────────────────────────────────────────────
  let allEvents        = [];       // events returned by /api/odds/events
  let selectedEventIds = new Set();
  let cachedScoredProps  = null;
  let cachedQuotaLog     = null;
  let cachedScrapeMeta   = null;

  // ── Persist / restore ─────────────────────────────────────────────────
  try {
    const k = localStorage.getItem(PARLAY_KEYS_STORAGE);
    if (k && parlayApiKeys) parlayApiKeys.value = k;
    const s = JSON.parse(localStorage.getItem(PARLAY_SETTINGS_STORAGE) || '{}');
    if (s.sport       && parlaySportSelect)   parlaySportSelect.value   = s.sport;
    if (s.odds_format && parlayOddsFormatSel) parlayOddsFormatSel.value = s.odds_format;
    if (s.legs        && parlayLegsSelect)    parlayLegsSelect.value    = String(s.legs);
    if (s.last_n      && parlayLastNSelect)   parlayLastNSelect.value   = String(s.last_n);
    if (s.bookmaker   && parlayBookmakerSelect) parlayBookmakerSelect.value = s.bookmaker;
  } catch(e) {}

  function saveSettings() {
    try {
      localStorage.setItem(PARLAY_KEYS_STORAGE, (parlayApiKeys.value || '').trim());
      localStorage.setItem(PARLAY_SETTINGS_STORAGE, JSON.stringify({
        sport: parlaySportSelect.value, odds_format: parlayOddsFormatSel.value,
        legs: parseInt(parlayLegsSelect.value), last_n: parseInt(parlayLastNSelect.value),
        bookmaker: parlayBookmakerSelect ? parlayBookmakerSelect.value : 'draftkings',
      }));
    } catch(e) {}
  }

  // ── Helpers ───────────────────────────────────────────────────────────
  function escHtml(s) { return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
  function fmtOdds(v) { return v != null ? Number(v).toFixed(2) : '—'; }
  function show(el, v) { if (el) el.style.display = v ? '' : 'none'; }
  function setStatus(msg, err) {
    if (!parlayStatusMeta) return;
    parlayStatusMeta.textContent = msg;
    parlayStatusMeta.style.color = err ? 'var(--bad)' : '';
  }
  function hitClass(pct) { return pct >= 70 ? 'hit-pct-high' : pct >= 55 ? 'hit-pct-mid' : 'hit-pct-low'; }

  function formatEventTime(iso) {
    if (!iso) return '';
    try {
      const d = new Date(iso);
      // Always show Philippines Time (Asia/Manila, UTC+8)
      const datePart = d.toLocaleDateString('en-PH', { timeZone: 'Asia/Manila', month: 'short', day: 'numeric' });
      const timePart = d.toLocaleTimeString('en-PH', { timeZone: 'Asia/Manila', hour: '2-digit', minute: '2-digit' });
      return datePart + ' ' + timePart + ' PHT';
    } catch(e) { return ''; }
  }

  // ── Update legs dropdown to match selected event count ─────────────────
  function syncLegsToEventCount() {
    const count = selectedEventIds.size;
    if (!parlayLegsSelect) return;
    const current = parseInt(parlayLegsSelect.value) || 3;
    // Remove options beyond event count, add up to max(event count, 6)
    parlayLegsSelect.innerHTML = '';
    const max = Math.min(count, 6);
    for (let i = 2; i <= max; i++) {
      const opt = document.createElement('option');
      opt.value = String(i);
      opt.textContent = i + '-Leg';
      if (i === Math.min(current, max)) opt.selected = true;
      parlayLegsSelect.appendChild(opt);
    }
    // Show/hide build wrap based on whether enough events selected
    show(parlayBuildWrap, count >= 2);
    if (parlayEventSelMeta) {
      parlayEventSelMeta.textContent = count === 0
        ? 'Select at least 2 games'
        : count === 1
          ? '1 game selected — select at least 1 more'
          : count + ' games selected · up to ' + max + '-leg parlay available';
    }
  }

  // ── Render event picker chips ──────────────────────────────────────────
  function renderEventPicker(events) {
    parlayEventList.innerHTML = events.map(function(ev) {
      const time = formatEventTime(ev.commence_time);
      return '<div class="parlay-event-chip" data-event-id="' + escHtml(ev.id) + '">' +
        '<span class="parlay-chip-check">✓</span>' +
        '<div>' +
          '<div class="parlay-chip-teams">' + escHtml(ev.away_team) + ' @ ' + escHtml(ev.home_team) + '</div>' +
          (time ? '<div class="parlay-chip-time">' + time + '</div>' : '') +
        '</div>' +
      '</div>';
    }).join('');

    parlayEventList.querySelectorAll('.parlay-event-chip').forEach(function(chip) {
      chip.addEventListener('click', function() {
        const id = chip.dataset.eventId;
        if (selectedEventIds.has(id)) {
          selectedEventIds.delete(id);
          chip.classList.remove('selected');
        } else {
          selectedEventIds.add(id);
          chip.classList.add('selected');
        }
        syncLegsToEventCount();
        // Clear cache when selection changes — different events = different scrape
        if (cachedScoredProps) {
          cachedScoredProps = null; cachedQuotaLog = null; cachedScrapeMeta = null;
          showCacheButtons(false);
          show(parlayTicket, false);
          show(parlayAllPropsWrap, false);
          setStatus('Event selection changed — click Scrape & Build to refresh.', false);
        }
      });
    });
  }

  function showCacheButtons(hasCache) {
    show(parlayRebuildBtn,  hasCache);
    show(parlayRescrapeBtn, hasCache);
    if (parlayBuildBtn) parlayBuildBtn.style.display = hasCache ? 'none' : '';
  }

  // ── Render quota pills ─────────────────────────────────────────────────
  function renderQuota(log) {
    if (!log || !log.length) return;
    show(parlayQuotaBar, true);
    parlayQuotaList.innerHTML = log.map(function(e) {
      const q = e.quota || {};
      return '<span class="parlay-quota-pill"><strong>' + escHtml(e.call||'?') + '</strong>' +
        (q.remaining != null ? ' · rem: ' + q.remaining : '') +
        (q.last != null ? ' · cost: ' + q.last : '') + '</span>';
    }).join('');
  }

  // ── Pick top N legs from cached props ──────────────────────────────────
  function pickLegs(props, n) {
    const legs = [], seenPlayers = new Set(), seenEvents = new Set();
    for (const p of props) {
      if (legs.length >= n) break;
      const pid = Number(p.player_id);
      const eid = p.event_id || '';
      if (seenPlayers.has(pid)) continue;
      if (eid && seenEvents.has(eid)) continue; // no same-game parlay
      seenPlayers.add(pid);
      if (eid) seenEvents.add(eid);
      legs.push(p);
    }
    return legs;
  }
  function calcOdds(legs) {
    if (!legs.length) return null;
    return Math.round(legs.reduce(function(a,l){ return a * l.odds; }, 1) * 100) / 100;
  }

  // ── Render ticket ──────────────────────────────────────────────────────
  function renderTicket(parlay, legs, parlayOdds) {
    if (!parlay || !parlay.length) return;
    show(parlayTicket, true);
    const legsSpan = parlayTicket.querySelector('#parlayTicketLegs');
    if (legsSpan && legsSpan.tagName === 'SPAN') legsSpan.textContent = legs;
    if (parlayTicketOdds) parlayTicketOdds.textContent = parlayOdds != null ? fmtOdds(parlayOdds) + 'x' : '—';
    const grid = parlayTicket.querySelector('.parlay-legs-grid');
    if (!grid) return;
    grid.innerHTML = parlay.map(function(leg, i) {
      const sc = leg.side === 'OVER' ? 'over' : 'under';
      const fp = Math.min(100, leg.hit_rate || 0);
      return '<div class="parlay-leg-card" data-leg-idx="' + i + '" title="Analyze ' + escHtml(leg.player_name) + '" style="cursor:pointer">' +
        '<span class="parlay-leg-rank">#' + (i+1) + '</span>' +
        '<div class="parlay-leg-player">' + escHtml(leg.player_name) + '</div>' +
        '<div class="parlay-leg-market">' +
          '<span class="parlay-leg-stat">' + escHtml(leg.stat) + '</span>' +
          '<span class="parlay-leg-line">' + leg.line + '</span>' +
          '<span class="parlay-leg-side ' + sc + '">' + leg.side + '</span>' +
        '</div>' +
        '<div class="parlay-leg-stats-row">' +
          '<div class="parlay-leg-stat-item"><span class="slabel">Hit %</span><span class="sval">' + leg.hit_rate + '%</span></div>' +
          '<div class="parlay-leg-stat-item"><span class="slabel">Avg</span><span class="sval">'   + (leg.average||'—')     + '</span></div>' +
          '<div class="parlay-leg-stat-item"><span class="slabel">Games</span><span class="sval">' + (leg.games_count||'—') + '</span></div>' +
          '<div class="parlay-leg-stat-item"><span class="slabel">Odds</span><span class="sval">'  + fmtOdds(leg.odds)      + '</span></div>' +
        '</div>' +
        '<div class="parlay-leg-hit-bar"><div class="parlay-leg-hit-fill" style="width:' + fp + '%"></div></div>' +
        '<div class="parlay-leg-analyze-hint" style="font-size:0.72rem;color:var(--muted);margin-top:6px;text-align:center">Click to analyze →</div>' +
      '</div>';
    }).join('');

    // Wire up click → analyzer with full auto-populate
    grid.querySelectorAll('.parlay-leg-card[data-leg-idx]').forEach(function(card) {
      card.addEventListener('click', async function() {
        const leg = parlay[parseInt(card.dataset.legIdx)];
        if (!leg || !leg.player_id) return;
        try {
          await hydrateAnalyzerFromPropSelection(leg);
        } catch(err) { console.warn('Parlay ticket leg nav failed:', err); }
      });
    });
  }

  // ── Render all props table (clickable → analyzer) ─────────────────────
  function renderAllProps(all, selectedIds) {
    if (!all || !all.length) return;
    show(parlayAllPropsWrap, true);
    if (parlayPropCount)     parlayPropCount.textContent     = all.length + ' props — click any row to analyze';
    if (parlayAllPropsTitle) parlayAllPropsTitle.textContent = all.length + ' props ranked by hit rate';
    const sel = new Set((selectedIds||[]).map(Number));
    parlayAllPropsBody.innerHTML = all.map(function(p, idx) {
      const isSel = sel.has(Number(p.player_id));
      const imgSrc = 'https://cdn.nba.com/headshots/nba/latest/1040x760/' + (p.player_id||'') + '.png';
      const gameTag = p.game_label ? '<span class="parlay-game-label">' + escHtml(p.game_label) + '</span>' : '';
      return '<tr class="' + (isSel ? 'parlay-selected-row' : '') + '" data-prop-idx="' + idx + '" title="Analyze ' + escHtml(p.player_name) + '">' +
        '<td><span style="display:flex;align-items:center;gap:8px"><img src="' + imgSrc + '" style="width:26px;height:26px;border-radius:50%;object-fit:cover;object-position:top" onerror="this.hidden=true">' +
          '<span style="display:flex;flex-direction:column;gap:2px">' +
            '<span>' + escHtml(p.player_name) + (isSel ? ' ⭐' : '') + '</span>' + gameTag +
          '</span></span></td>' +
        '<td>' + escHtml(p.stat) + '</td><td>' + p.line + '</td>' +
        '<td class="' + (p.side==='OVER'?'side-over':'side-under') + '">' + p.side + '</td>' +
        '<td class="hit-pct-cell ' + hitClass(p.hit_rate) + '">' + p.hit_rate + '%</td>' +
        '<td>' + (p.average||'—') + '</td><td>' + (p.games_count||'—') + '</td>' +
        '<td>' + fmtOdds(p.odds) + '</td>' +
        '<td><button class="parlay-track-btn" data-track-idx="' + idx + '">+ Track</button></td>' +
        '</tr>';
    }).join('');
    parlayAllPropsBody.querySelectorAll('tr[data-prop-idx]').forEach(function(row) {
      row.addEventListener('click', async function(e) {
        if (e.target.closest('.parlay-track-btn')) return;
        const prop = all[parseInt(row.dataset.propIdx)];
        if (!prop || !prop.player_id) return;
        try {
          await hydrateAnalyzerFromPropSelection(prop);
        } catch(e) { console.warn('Parlay row nav failed:', e); }
      });
    });
    parlayAllPropsBody.querySelectorAll('.parlay-track-btn').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var prop = all[parseInt(btn.dataset.trackIdx)];
        if (typeof window._trackerAddProp === 'function') {
          window._trackerAddProp(prop);
          btn.textContent = '\u2713 Added';
          btn.disabled = true;
          btn.style.background = 'rgba(57,217,138,0.15)';
          btn.style.color = 'var(--good)';
          btn.style.borderColor = 'var(--good)';
          if (typeof switchView === 'function') switchView('tracker');
        }
      });
    });
  }

  // ── Rebuild from cache (zero API calls) ───────────────────────────────
  function rebuildFromCache() {
    if (!cachedScoredProps || !cachedScoredProps.length) return;
    const n    = parseInt(parlayLegsSelect.value) || 3;
    const legs = pickLegs(cachedScoredProps, n);
    const odds = calcOdds(legs);
    renderTicket(legs, n, odds);
    renderAllProps(cachedScoredProps, legs.map(function(l){ return l.player_id; }));
    const m = cachedScrapeMeta || {};
    const ago = m.scrapedAt ? ' · cached ' + Math.round((Date.now()-m.scrapedAt)/60000) + 'm ago' : '';
    setStatus((m.evCount||0) + ' events · ' + (m.analyzed||0) + ' props analyzed' + ago + ' — no credits used', false);
  }

  // ── Full scrape selected events → analyze → render ───────────────────
  async function runScrape() {
    const rawKeys = (parlayApiKeys.value || '').trim();
    if (!rawKeys) { setStatus('Enter at least one API key.', true); return; }
    const keys = rawKeys.split(',').map(function(k){ return k.trim(); }).filter(Boolean);
    if (selectedEventIds.size === 0) { setStatus('Select at least one event first.', true); return; }
    const legs       = parseInt(parlayLegsSelect.value) || 2;
    const sport      = parlaySportSelect.value;
    const oddsFormat = parlayOddsFormatSel.value;
    const lastN      = parseInt(parlayLastNSelect.value) || 10;

    saveSettings();
    show(parlayTicket, false); show(parlayAllPropsWrap, false);
    show(parlayQuotaBar, false); show(parlayEmptyState, false);
    parlayQuotaList.innerHTML = '';
    const totalEvents = selectedEventIds.size;
    setStatus('⏳ Queueing parlay build for ' + totalEvents + ' game(s)…', false);
    setParlayProgress(3, 'Sending background job to server…');
    if (parlayBuildBtn)    parlayBuildBtn.disabled    = true;
    if (parlayRebuildBtn)  parlayRebuildBtn.disabled  = true;
    if (parlayRescrapeBtn) parlayRescrapeBtn.disabled = true;

    try {
      const createResp = await fetch('/api/parlay-builder/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          api_keys: keys,
          legs: legs,
          sport: sport,
          odds_format: oddsFormat,
          last_n: lastN,
          event_ids: Array.from(selectedEventIds),
          bookmaker: parlayBookmakerSelect ? parlayBookmakerSelect.value : 'draftkings',
        }),
      });
      if (!createResp.ok) {
        const err = await createResp.json().catch(function(){ return {}; });
        throw new Error(err.detail || 'Unable to start parlay builder');
      }
      const created = await createResp.json();
      if (!created || !created.job_id) throw new Error('Missing parlay job id from server.');

      const jobId = created.job_id;
      const pollDelayMs = 1200;
      const maxPollMs = 9 * 60 * 1000;
      const startedAt = Date.now();
      let lastMessage = '';

      while (true) {
        const pollResp = await fetch('/api/parlay-builder/jobs/' + encodeURIComponent(jobId), { cache: 'no-store' });
        if (!pollResp.ok) {
          const err = await pollResp.json().catch(function(){ return {}; });
          throw new Error(err.detail || 'Failed to read parlay job status');
        }
        const job = await pollResp.json();
        const status = String(job.status || 'queued').toLowerCase();
        const progress = Math.max(0, Math.min(100, parseInt(job.progress || 0, 10) || 0));
        const stage = job.stage || 'Processing';
        const message = job.message || (stage + '…');
        setParlayProgress(progress > 0 ? progress : 4, stage + ' — ' + message);
        if (message !== lastMessage) {
          setStatus('⏳ ' + message, false);
          lastMessage = message;
        }

        if (status === 'completed') {
          const data = job.result || {};
          cachedScoredProps = data.all_props_scored || [];
          cachedQuotaLog    = data.quota_log || [];
          cachedScrapeMeta  = { evCount: data.events_scraped||0, propCount: data.props_found||0, analyzed: data.props_analyzed||0, scrapedAt: Date.now() };

          renderQuota(cachedQuotaLog);
          const m = cachedScrapeMeta;
          setStatus('✓ Done — ' + m.evCount + ' event(s) scraped · ' + m.propCount + ' props found · ' + m.analyzed + ' analyzed', false);
          setParlayProgress(100, '✓ Complete!');
          setTimeout(function() { setParlayProgress(0, ''); }, 1800);

          if (!cachedScoredProps.length) {
            show(parlayEmptyState, true);
            const s = parlayEmptyState.querySelector('strong'), sp = parlayEmptyState.querySelector('span');
            if (s) s.textContent = data.message || 'No props found for selected events.';
            if (sp) sp.textContent = 'Try different events or check your API keys.';
            if (typeof hideBanner === 'function') hideBanner();
            break;
          }
          showCacheButtons(true);
          rebuildFromCache();
          if (typeof hideBanner === 'function') hideBanner();
          break;
        }

        if (status === 'failed') {
          const errorDetail = (job.error && (job.error.detail || job.error.message)) || message || 'Parlay build failed';
          throw new Error(errorDetail);
        }

        if ((Date.now() - startedAt) > maxPollMs) {
          throw new Error('Parlay build took too long on the host. Please try fewer games or retry.');
        }
        await new Promise(function(resolve) { setTimeout(resolve, pollDelayMs); });
      }
    } catch(err) {
      setParlayProgress(0, '');
      setStatus('❌ Error: ' + (err.message || 'Unknown'), true);
      show(parlayEmptyState, true);
    } finally {
      if (parlayBuildBtn)    parlayBuildBtn.disabled    = false;
      if (parlayRebuildBtn)  parlayRebuildBtn.disabled  = false;
      if (parlayRescrapeBtn) parlayRescrapeBtn.disabled = false;
    }
  }

  // ── Step 1: Load today's events ───────────────────────────────────────
  parlayLoadEventsBtn.addEventListener('click', async function() {
    const rawKeys = (parlayApiKeys.value || '').trim();
    if (!rawKeys) { setStatus('Enter at least one API key first.', true); return; }
    const keys = rawKeys.split(',').map(function(k){ return k.trim(); }).filter(Boolean);
    saveSettings();
    parlayLoadEventsBtn.disabled = true;
    parlayLoadEventsBtn.textContent = 'Loading…';
    show(parlayEventPickerWrap, false);
    show(parlayBuildWrap, false);

    try {
      const resp = await fetch('/api/odds/events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ api_key: keys[0], sport: parlaySportSelect.value }),
      });
      if (!resp.ok) {
        const err = await resp.json().catch(function(){ return {}; });
        throw new Error(err.detail || 'Failed to load events');
      }
      const data = await resp.json();
      allEvents = (data.events || []).filter(function(e){ return e.id && e.home_team; });

      if (!allEvents.length) {
        setStatus('No events found for today. Try again later.', true);
        return;
      }

      // Reset state
      selectedEventIds.clear();
      cachedScoredProps = null; cachedQuotaLog = null; cachedScrapeMeta = null;
      showCacheButtons(false);
      show(parlayTicket, false); show(parlayAllPropsWrap, false);

      renderEventPicker(allEvents);
      show(parlayEventPickerWrap, true);
      syncLegsToEventCount();
      setStatus(allEvents.length + ' events loaded. Pick your games.', false);
    } catch(err) {
      setStatus('Error loading events: ' + err.message, true);
    } finally {
      parlayLoadEventsBtn.disabled = false;
      parlayLoadEventsBtn.textContent = "Load Today's Events";
    }
  });

  // Select all / none
  if (parlaySelectAllBtn) {
    parlaySelectAllBtn.addEventListener('click', function() {
      parlayEventList.querySelectorAll('.parlay-event-chip').forEach(function(c) {
        selectedEventIds.add(c.dataset.eventId);
        c.classList.add('selected');
      });
      syncLegsToEventCount();
    });
  }
  if (parlaySelectNoneBtn) {
    parlaySelectNoneBtn.addEventListener('click', function() {
      selectedEventIds.clear();
      parlayEventList.querySelectorAll('.parlay-event-chip').forEach(function(c) { c.classList.remove('selected'); });
      syncLegsToEventCount();
    });
  }

  // Scrape & Build
  if (parlayBuildBtn) parlayBuildBtn.addEventListener('click', function() { runScrape(); });

  // Reshuffle (re-pick from cache, zero credits)
  if (parlayRebuildBtn) {
    parlayRebuildBtn.addEventListener('click', function() {
      if (!cachedScoredProps || !cachedScoredProps.length) { setStatus('No cached data. Run Scrape & Build first.', true); return; }
      // Rotate the cached props so a different combination surfaces
      if (cachedScoredProps.length > 1) cachedScoredProps.push(cachedScoredProps.shift());
      rebuildFromCache();
    });
  }

  // Re-scrape (clear cache, burn credits)
  if (parlayRescrapeBtn) {
    parlayRescrapeBtn.addEventListener('click', function() {
      cachedScoredProps = null; cachedQuotaLog = null; cachedScrapeMeta = null;
      showCacheButtons(false);
      runScrape();
    });
  }

  // Leg change → instant local rebuild
  if (parlayLegsSelect) {
    parlayLegsSelect.addEventListener('change', function() {
      if (cachedScoredProps && cachedScoredProps.length) rebuildFromCache();
    });
  }

})();

/* ═══════════════════════════════════════════════════════════════════════
   PROP TRACKER
═══════════════════════════════════════════════════════════════════════ */
(function initPropTracker() {
  const TRACKER_STORAGE_KEY = 'nba-props-tracker-bets';

  const trackerPlayerInput = document.getElementById('trackerPlayerInput');
  const trackerStatSelect  = document.getElementById('trackerStatSelect');
  const trackerLineInput   = document.getElementById('trackerLineInput');
  const trackerSideSelect  = document.getElementById('trackerSideSelect');
  const trackerOddsInput   = document.getElementById('trackerOddsInput');
  const trackerBookInput   = document.getElementById('trackerBookInput');
  const trackerAddBtn      = document.getElementById('trackerAddBtn');
  const trackerAddError    = document.getElementById('trackerAddError');
  const trackerRefreshBtn  = document.getElementById('trackerRefreshBtn');
  const trackerCards       = document.getElementById('trackerCards');
  const trackerEmpty       = document.getElementById('trackerEmpty');

  if (!trackerAddBtn) return;

  // ── Autocomplete dropdown for player input ────────────────────────────
  const trackerPlayerDropdown = document.getElementById('trackerPlayerDropdown');
  let _autocompleteDebounce = null;
  let _selectedPlayerName = '';
  let _selectedPlayerId = null;

  function closeDropdown() {
    if (trackerPlayerDropdown) {
      trackerPlayerDropdown.innerHTML = '';
      trackerPlayerDropdown.style.display = 'none';
    }
    if (trackerPlayerInput) {
      trackerPlayerInput.setAttribute('aria-expanded', 'false');
    }
  }

  function openDropdown(results) {
    if (!trackerPlayerDropdown) return;
    if (!results.length) { closeDropdown(); return; }
    trackerPlayerDropdown.innerHTML = results.map(function(p) {
      return '<li role="option" class="tracker-player-option" data-id="' + p.id + '" data-name="' + esc(p.full_name) + '" style="padding:8px 12px;cursor:pointer;list-style:none;border-bottom:1px solid var(--border,#333)">'
        + esc(p.full_name) + (p.is_active ? '' : ' <small style="opacity:.5">(inactive)</small>') + '</li>';
    }).join('');
    trackerPlayerDropdown.style.display = 'block';
    if (trackerPlayerInput) trackerPlayerInput.setAttribute('aria-expanded', 'true');

    trackerPlayerDropdown.querySelectorAll('.tracker-player-option').forEach(function(li) {
      li.addEventListener('mousedown', function(e) {
        e.preventDefault(); // prevent blur before click
        _selectedPlayerId   = li.dataset.id ? Number(li.dataset.id) : null;
        _selectedPlayerName = li.dataset.name || '';
        trackerPlayerInput.value = _selectedPlayerName;
        closeDropdown();
      });
    });
  }

  if (trackerPlayerInput) {
    trackerPlayerInput.addEventListener('input', function() {
      const q = trackerPlayerInput.value.trim();
      _selectedPlayerId = null; // reset cached selection on new typing
      clearTimeout(_autocompleteDebounce);
      if (q.length < 2) { closeDropdown(); return; }
      _autocompleteDebounce = setTimeout(async function() {
        try {
          const r = await fetch('/api/players/search?q=' + encodeURIComponent(q));
          if (!r.ok) return;
          const data = await r.json();
          openDropdown((data.results || []).slice(0, 8));
        } catch(e) { closeDropdown(); }
      }, 220);
    });

    trackerPlayerInput.addEventListener('blur', function() {
      // Slight delay so mousedown on option fires first
      setTimeout(closeDropdown, 200);
    });

    trackerPlayerInput.addEventListener('keydown', function(e) {
      if (e.key === 'Escape') closeDropdown();
    });
  }

  // ── State ─────────────────────────────────────────────────────────────
  // bet = { id, player_name, player_id, stat, line, side, odds, book,
  //         current_val, games_count, last_updated, status }
  let bets = [];

  // ── Persist ───────────────────────────────────────────────────────────
  function saveBets() {
    try { localStorage.setItem(TRACKER_STORAGE_KEY, JSON.stringify(bets)); } catch(e) {}
  }
  function loadBets() {
    try { return JSON.parse(localStorage.getItem(TRACKER_STORAGE_KEY) || '[]'); } catch(e) { return []; }
  }

  // ── Helpers ───────────────────────────────────────────────────────────
  function esc(s) { return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
  function escapeHtmlT(s) { return esc(s); }
  function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2,6); }
  function showErr(msg) {
    if (!trackerAddError) return;
    trackerAddError.textContent = msg;
    trackerAddError.style.display = msg ? '' : 'none';
  }
  function showEmpty(v) {
    if (trackerEmpty) trackerEmpty.style.display = v ? '' : 'none';
    if (trackerCards) trackerCards.style.display = v ? 'none' : '';
  }

  // ── Compute status from current val vs line ────────────────────────────
  function computeStatus(bet) {
    const val  = bet.current_val;
    const line = parseFloat(bet.line);
    if (val === null || val === undefined) return 'pending';
    const v = parseFloat(val);
    if (bet.side === 'OVER')  return v >= line ? 'hit'    : v >= line * 0.85 ? 'close' : 'progress';
    if (bet.side === 'UNDER') return v <= line ? 'hit'    : v <= line * 1.15 ? 'close' : 'busted';
    return 'progress';
  }

  // For UNDER, the bar fills inversely: more value = worse
  function computeBarPct(bet) {
    const val  = bet.current_val;
    const line = parseFloat(bet.line);
    if (val === null || val === undefined) return 0;
    const v = parseFloat(val);
    if (bet.side === 'OVER') {
      return Math.min(100, Math.round((v / line) * 100));
    } else {
      // UNDER: bar fills as remaining room shrinks; hits 100% when val === 0
      const pct = Math.max(0, Math.round(((line - v) / line) * 100));
      return Math.min(100, pct);
    }
  }

  // ── Render a single card ───────────────────────────────────────────────
  function renderCard(bet) {
    const status  = computeStatus(bet);
    const barPct  = computeBarPct(bet);
    const val     = bet.current_val !== null && bet.current_val !== undefined
                    ? parseFloat(bet.current_val).toFixed(1) : '—';
    const line    = parseFloat(bet.line).toFixed(1);
    const imgSrc  = bet.player_id
                    ? 'https://cdn.nba.com/headshots/nba/latest/1040x760/' + bet.player_id + '.png'
                    : '';

    // Card gets extra class when injured — must be declared before cardClass uses it
    const extraCardCls = bet.is_injured ? ' injured' : '';

    // Card-level class
    const cardClass = (status === 'hit' ? 'tracker-card hit'
                    : status === 'busted' ? 'tracker-card busted'
                    : 'tracker-card') + extraCardCls;

    // Bar track class
    const barClass  = status === 'hit'     ? 'tracker-bar-track hit'
                    : status === 'close'   ? 'tracker-bar-track close'
                    : status === 'busted'  ? 'tracker-bar-track busted'
                    : status === 'pending' ? 'tracker-bar-track pending'
                    :                        'tracker-bar-track progress';

    // Status badge
    const badgeLabel = status === 'hit'     ? '✓ HIT'
                     : status === 'busted'  ? '✗ BUST'
                     : status === 'close'   ? 'CLOSE'
                     : status === 'pending' ? 'PENDING'
                     :                        'IN PROGRESS';
    const badgeClass = status === 'hit' || status === 'close' ? 'hit'
                     : status === 'busted' ? 'busted'
                     : status === 'pending' ? 'pending' : 'active';

    // Injury pill
    const injPill = bet.is_injured
      ? '<span class="tracker-injury-pill">' + escapeHtmlT(bet.injury_status || 'Injured') + '</span>'
      : '';

    // Source pill
    const src = bet.source || '';
    const srcPill = src === 'LIVE'
      ? '<span class="tracker-source-pill live">LIVE</span>'
      : src === 'FINAL'    ? '<span class="tracker-source-pill final">FINAL</span>'
      : src === 'UPCOMING' ? '<span class="tracker-source-pill upcoming">UPCOMING</span>'
      : src === 'NO GAME TODAY' ? '<span class="tracker-source-pill nogame">NO GAME</span>'
      : '';

    // Period/clock
    const gameCtx = bet.game_label
      ? esc(bet.game_label) + (bet.game_status === 'live' && bet.period ? ' · ' + esc(bet.period) + (bet.clock ? ' ' + esc(bet.clock) : '') : '')
      : '';

    const sideClass = bet.side === 'OVER' ? 'over' : 'under';
    const lastUpd   = bet.last_updated
                    ? new Date(bet.last_updated).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})
                    : '—';

    // Line tick position — where the line marker sits on the bar
    const tickPct = bet.side === 'OVER' ? 100 : Math.round(((parseFloat(bet.line)) / (parseFloat(bet.line) * 1.5)) * 100);

    return `
<div class="${cardClass}" data-bet-id="${esc(bet.id)}" id="tracker-card-${esc(bet.id)}">
  <div class="tracker-card-inner">
    <div class="tracker-avatar-strip">
      ${imgSrc ? `<img src="${imgSrc}" alt="${esc(bet.player_name)}" onerror="this.style.display='none'">` : '<div style="width:80px;height:80px;display:flex;align-items:center;justify-content:center;font-size:1.8rem;opacity:0.4">🏀</div>'}
    </div>
    <div class="tracker-card-body">
      <div class="tracker-card-top">
        <div class="tracker-player-name">${esc(bet.player_name)}</div>
        <span class="tracker-stat-pill">${esc(bet.stat)}</span>
        <span class="tracker-side-pill ${sideClass}">${esc(bet.side)}</span>
        ${bet.book ? `<span class="tracker-book-pill">${esc(bet.book)}</span>` : ''}
        <button class="tracker-remove-btn" data-remove-id="${esc(bet.id)}" title="Remove this prop" type="button">✕</button>
      </div>

      <div class="tracker-value-row">
        <span class="tracker-current-val">${val}</span>
        <span class="tracker-line-label">/ ${line} ${esc(bet.stat)}</span>
        <span class="tracker-status-badge ${badgeClass}">${badgeLabel}</span>
        ${injPill}${srcPill}
        ${gameCtx ? '<span class="tracker-game-label">' + gameCtx + '</span>' : ''}
      </div>

      <div class="tracker-bar-wrap">
        <div class="${barClass}" style="width:${barPct}%"></div>
        <div class="tracker-bar-line-tick" style="left:calc(100% - 2px)"></div>
      </div>

      <div class="tracker-card-meta">
        <div class="tracker-meta-item"><span class="ml">Odds</span><span class="mv">${parseFloat(bet.odds||1.91).toFixed(2)}</span></div>
        <div class="tracker-meta-item"><span class="ml">Line</span><span class="mv">${line}</span></div>
        <div class="tracker-meta-item"><span class="ml">Source</span><span class="mv">${esc(bet.source || '—')}</span></div>
        <div class="tracker-meta-item"><span class="ml">Updated</span><span class="mv">${lastUpd}</span></div>
      </div>
    </div>
  </div>
</div>`;
  }

  // ── Re-render all cards ────────────────────────────────────────────────
  function renderAll() {
    if (!bets.length) { showEmpty(true); return; }
    showEmpty(false);
    // Animate bars after render using rAF
    trackerCards.innerHTML = bets.map(renderCard).join('');
    // Wire remove buttons
    trackerCards.querySelectorAll('[data-remove-id]').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        const id = btn.dataset.removeId;
        bets = bets.filter(function(b) { return b.id !== id; });
        saveBets();
        renderAll();
      });
    });
    // Trigger bar animation on next frame (bars start at 0 via CSS, then widen)
    requestAnimationFrame(function() {
      trackerCards.querySelectorAll('.tracker-bar-track').forEach(function(bar) {
        bar.style.transition = 'width 1.1s cubic-bezier(0.34,1.56,0.64,1), background 0.5s';
      });
    });
  }

  // ── Fetch live stat — today/future only, no historical fallback ────────
  async function refreshBet(bet) {
    if (!bet.player_id) return;
    try {
      const lp = new URLSearchParams({ player_id: bet.player_id, stat: bet.stat });
      const lr = await fetch('/api/tracker/live-stat?' + lp.toString());
      if (!lr.ok) return;
      const d = await lr.json();
      bet.is_injured    = d.is_injured    || false;
      bet.injury_status = d.injury_status || '';
      const gs = d.game_status;
      if ((gs === 'live' || gs === 'final') && d.live_val !== null && d.live_val !== undefined) {
        bet.current_val  = d.live_val;
        bet.game_status  = gs;
        bet.game_label   = d.game_label || '';
        bet.period       = d.period || '';
        bet.clock        = d.clock  || '';
        bet.source       = gs === 'live' ? 'LIVE' : 'FINAL';
        bet.last_updated = new Date().toISOString();
        return;
      }
      if (gs === 'scheduled') {
        bet.game_status  = 'scheduled';
        bet.game_label   = d.game_label || '';
        bet.source       = 'UPCOMING';
        bet.current_val  = null;
        bet.last_updated = new Date().toISOString();
        return;
      }
      bet.game_status  = 'no_game';
      bet.source       = 'NO GAME TODAY';
      bet.last_updated = new Date().toISOString();
    } catch(e) {
      console.warn('Refresh failed for', bet.player_name, e);
    }
  }

  // ── Resolve player_id from name via search ────────────────────────────
  async function resolvePlayerId(name) {
    // If user picked from autocomplete dropdown, use the cached id directly
    if (_selectedPlayerId && _selectedPlayerName.toLowerCase() === name.toLowerCase()) {
      return _selectedPlayerId;
    }
    try {
      const ctrl = new AbortController();
      const timer = setTimeout(function() { ctrl.abort(); }, 4000);
      const resp = await fetch('/api/players/search?q=' + encodeURIComponent(name), { signal: ctrl.signal });
      clearTimeout(timer);
      if (!resp.ok) return null;
      const data = await resp.json();
      const results = data.results || [];
      if (Array.isArray(results) && results.length) return results[0].id || null;
    } catch(e) {}
    return null;
  }

  // ── Add prop from parlay to tracker ────────────────────────────────────
  // Returns true if actually added, false if duplicate or invalid.
  function addPropToTracker(prop) {
    if (!prop || !prop.player_name) return false;
    const parsedLine = parseFloat(prop.line);
    if (!parsedLine || parsedLine <= 0) return false;

    const newBet = {
      id:           Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
      player_name:  String(prop.player_name),
      player_id:    prop.player_id || null,
      stat:         prop.stat || 'PTS',
      line:         parsedLine,
      side:         prop.side || 'OVER',
      odds:         parseFloat(prop.odds) || 1.91,
      book:         prop.bookmaker || prop.book || '',
      game_label:   prop.game_label || '',
      current_val:  null,
      game_status:  null,
      source:       null,
      is_injured:   false,
      injury_status:'',
      last_updated: null,
    };

    let stored = [];
    try { stored = JSON.parse(localStorage.getItem('nba-props-tracker-bets') || '[]'); } catch(e) { stored = []; }
    if (!Array.isArray(stored)) stored = [];

    // Dupe check: prefer player_id match when both sides have one, else fall back
    // to name match — prevents null-id props from all blocking each other.
    const isDupe = stored.some(function(b) {
      const idMatch = newBet.player_id && b.player_id
        ? String(b.player_id) === String(newBet.player_id)
        : (b.player_name || '').toLowerCase() === newBet.player_name.toLowerCase();
      return idMatch && b.stat === newBet.stat &&
             parseFloat(b.line) === newBet.line && b.side === newBet.side;
    });

    if (isDupe) {
      _showTrackerToast(prop.player_name + ' already in Tracker');
      return false;
    }

    stored.unshift(newBet);
    try { localStorage.setItem('nba-props-tracker-bets', JSON.stringify(stored)); } catch(e) {}

    // Keep the IIFE-scoped bets in sync so renderAll sees the new entry
    bets = stored;

    _showTrackerToast(prop.player_name + ' added to Tracker');
    return true;
  }

  function _showTrackerToast(msg) {
    let el = document.getElementById('_trackerToast');
    if (!el) {
      el = document.createElement('div'); el.id = '_trackerToast';
      el.style.cssText = 'position:fixed;bottom:28px;left:50%;transform:translateX(-50%) translateY(20px);'
        + 'background:var(--accent,#7c6ef5);color:#fff;padding:10px 22px;border-radius:40px;'
        + 'font-size:.9rem;font-weight:600;z-index:9999;opacity:0;transition:opacity .25s,transform .25s;'
        + 'pointer-events:none;white-space:nowrap;';
      document.body.appendChild(el);
    }
    el.textContent = msg;
    el.style.opacity = '1'; el.style.transform = 'translateX(-50%) translateY(0)';
    clearTimeout(el._t);
    el._t = setTimeout(function() {
      el.style.opacity = '0'; el.style.transform = 'translateX(-50%) translateY(20px)';
    }, 2600);
  }

  // ── Add bet handler ────────────────────────────────────────────────────
  trackerAddBtn.addEventListener('click', async function() {
    showErr('');
    const playerName = (trackerPlayerInput.value || '').trim();
    const stat       = trackerStatSelect.value;
    const line       = parseFloat(trackerLineInput.value);
    const side       = trackerSideSelect.value;
    const odds       = parseFloat(trackerOddsInput.value) || 1.91;
    const book       = (trackerBookInput.value || '').trim();

    if (!playerName) return showErr('Enter a player name.');
    if (isNaN(line) || line <= 0) return showErr('Enter a valid line (e.g. 25.5).');

    trackerAddBtn.disabled = true;
    trackerAddBtn.textContent = 'Adding…';

    try {
      const playerId = await resolvePlayerId(playerName);

      const bet = {
        id: uid(),
        player_name: playerName,
        player_id: playerId,
        stat, line, side, odds, book,
        current_val: null,
        games_count: null,
        last_updated: null,
        game_status: null,
        source: null,
        is_injured: false,
        injury_status: '',
        status: 'pending',
      };

      bets.unshift(bet);
      saveBets();
      try { renderAll(); } catch(e) { console.error('Tracker renderAll error:', e); }

      // Clear inputs and reset autocomplete state
      trackerPlayerInput.value = '';
      trackerLineInput.value   = '';
      trackerOddsInput.value   = '';
      trackerBookInput.value   = '';
      _selectedPlayerId   = null;
      _selectedPlayerName = '';
      closeDropdown();

      // Auto-refresh the newly added bet to get live stat
      if (playerId) {
        try {
          await refreshBet(bet);
          saveBets();
          try { renderAll(); } catch(e) { /* non-fatal */ }
        } catch(e) { /* non-fatal */ }
      }
    } catch(e) {
      console.error('Add prop error:', e);
      showErr('Failed to add prop. Please try again.');
    } finally {
      trackerAddBtn.disabled   = false;
      trackerAddBtn.textContent = 'Add Prop';
    }
  });

  // ── Refresh all handler ────────────────────────────────────────────────
  trackerRefreshBtn.addEventListener('click', async function() {
    if (!bets.length) return;
    trackerRefreshBtn.classList.add('spinning');
    trackerRefreshBtn.disabled = true;

    const refreshable = bets.filter(function(b) { return b.player_id; });
    await Promise.allSettled(refreshable.map(refreshBet));
    saveBets();

    // Pulse any newly-hit cards
    renderAll();
    trackerCards.querySelectorAll('.tracker-card.hit').forEach(function(card) {
      card.classList.add('hit-new');
      setTimeout(function() { card.classList.remove('hit-new'); }, 2000);
    });

    trackerRefreshBtn.classList.remove('spinning');
    trackerRefreshBtn.disabled = false;
  });

  // ── Init: restore from localStorage ───────────────────────────────────
  bets = loadBets();
  renderAll();

  // Auto-refresh every 30s when live/upcoming game
  setInterval(async function() {
    const active = bets.some(function(b) { return b.game_status === 'live' || b.game_status === 'scheduled'; });
    if (!bets.length || !active) return;
    await Promise.allSettled(bets.filter(function(b){ return b.player_id; }).map(refreshBet));
    saveBets(); renderAll();
  }, 30000);

  // ── Expose globally so parlay builder (different IIFE) can call in ────
  window._trackerAddProp = function(prop) {
    const added = addPropToTracker(prop);
    try { renderAll(); } catch(e) { console.error('Tracker renderAll failed:', e); }
    if (typeof switchView === 'function') switchView('tracker');
  };

})();
