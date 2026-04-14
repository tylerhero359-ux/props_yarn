/**
 * apiFetch — drop-in fetch() replacement with timeout + consistent error handling.
 * All NBA API calls go through this so a slow/hung backend never freezes the UI.
 *
 * @param {string} url
 * @param {RequestInit} [opts]
 * @param {number} [timeoutMs=20000]
 * @returns {Promise<any>} parsed JSON
 * @throws {Error} with a user-readable .message on any failure
 */
async function apiFetch(url, opts = {}, timeoutMs = 20000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...opts, signal: controller.signal });
    clearTimeout(timer);
    if (!res.ok) {
      let detail = `Server error ${res.status}`;
      try { const d = await res.json(); detail = d.detail || d.message || detail; } catch (e) {}
      throw new Error(detail);
    }
    return await res.json();
  } catch (err) {
    clearTimeout(timer);
    if (err.name === 'AbortError') {
      throw new Error(`Request timed out after ${timeoutMs / 1000}s — the NBA API may be slow. Try again.`);
    }
    throw err;
  }
}

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
const marketInspectTray = document.getElementById('marketInspectTray');
const oddsApiKeyInput = document.getElementById('oddsApiKeyInput');
const oddsSportSelect = document.getElementById('oddsSportSelect');
const oddsRegionsInput = document.getElementById('oddsRegionsInput');
const oddsFormatSelect = document.getElementById('oddsFormatSelect');
const oddsMarketsInput = document.getElementById('oddsMarketsInput');
const oddsEventSelect = document.getElementById('oddsEventSelect');
const oddsSaveKeyBtn    = document.getElementById('oddsSaveKeyBtn');
const oddsCheckBalBtn   = document.getElementById('oddsCheckBalanceBtn');
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
const overviewBoardTabsEl = document.getElementById('overviewBoardTabs');
const overviewTopCountEl = document.getElementById('overviewTopCount');
const overviewCautionCountEl = document.getElementById('overviewCautionCount');
const overviewBoostCountEl = document.getElementById('overviewBoostCount');
const interpretationTone = document.getElementById('interpretationTone');
const interpretationBody = document.getElementById('interpretationBody');
const opportunityTone = document.getElementById('opportunityTone');
const opportunityBody = document.getElementById('opportunityBody');
const environmentTone = document.getElementById('environmentTone');
const environmentBody = document.getElementById('environmentBody');
const marketTone = document.getElementById('marketTone');
const marketBody = document.getElementById('marketBody');
const varianceTone = document.getElementById('varianceTone');
const varianceBody = document.getElementById('varianceBody');
const decisionStripToneEl = document.getElementById('decisionStripTone');
const analyzerDecisionStripGrid = document.getElementById('analyzerDecisionStripGrid');
const analyzerDecisionStrip = document.getElementById('analyzerDecisionStrip');
const cacheDebugPanel = document.getElementById('cacheDebugPanel');

const RECENT_PLAYERS_KEY = 'nba-props-recent-players';
const THEME_KEY = 'nba-props-theme';
const SIDEBAR_COLLAPSED_KEY = 'nba-props-sidebar-collapsed';
const MARKET_RESULTS_KEY = 'nba-props-latest-market-results';
const MARKET_EXPERT_FILTERS_KEY = 'nba-props-market-expert-filters';
const ODDS_API_KEY_STORAGE = 'nba-props-odds-api-key';
const ODDS_API_SETTINGS_STORAGE = 'nba-props-odds-api-settings';
const INJURY_DEBUG_STORAGE = 'nba-props-injury-debug';
const CACHE_DEBUG_STORAGE = 'nba-props-cache-debug';
const LAST_PLAYER_KEY = 'nba-props-last-player';
const LAST_STAT_KEY = 'nba-props-last-stat';
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

if (marketInspectTray && marketInspectTray.parentElement !== document.body) {
  document.body.appendChild(marketInspectTray);
}

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
let todayGamesLoadInFlight = false;
let todayGamesLoadAttempts = 0;
let todayGamesLastSuccessAt = 0;
let todayGamesLastErrorAt = 0;
let todayGamesRetryTimer = null;
const TODAY_GAMES_CACHE_KEY = 'nba-props-todays-games-cache';
const TODAY_GAMES_CACHE_TTL_MS = 10 * 60 * 1000;
let overviewBoardTab = 'top';
let currentGameLogPayload = null;
let activeGameLogView = 'recent';
let currentMarketResultsPayload = null;
let currentMarketSort = localStorage.getItem('nba-props-market-sort') || 'best_ev';
let currentMarketSortDirection = localStorage.getItem('nba-props-market-sort-direction') || 'desc';
let currentMarketFilter = localStorage.getItem('nba-props-market-filter') || 'all';
let currentExpertFilters = loadStoredExpertFilters();
const currentExpertSettings = { min_minutes: 22, min_fga: 10, min_h2h_games: 2, min_h2h_hit_rate: 60 };
let currentMarketInspectKey = '';
let currentMarketInspectKeys = [];
const marketTeamInjuryCache = new Map();
const analyzerGameContextCache = new Map();
const todayGameContextCache = new Map();

// ── Market advanced filters (bookmaker + odds range) ──────────────────
let currentMarketBookFilter = '';
let currentMarketMinOdds = null;
let currentMarketMaxOdds = null;

// ── Key Vault ─────────────────────────────────────────────────────────
const KEY_VAULT_STORAGE = 'nba-props-key-vault';
const KEY_VAULT_ACTIVE_STORAGE = 'nba-props-key-vault-active';
const KEY_VAULT_MIN_ROTATING_KEYS = 5;
const KEY_VAULT_MIN_USABLE_CREDITS = 1;
const KEY_VAULT_SOFT_DISABLE_CREDITS = 3;
const KEY_VAULT_BALANCE_STALE_MS = 5 * 60 * 1000;
let cachedEligibleVaultKeys = [];
let cachedEligibleVaultKeysAt = 0;
let oddsKeyVaultState = [];
let oddsKeyVaultActiveId = '';
let keyVaultBootstrapPromise = null;
let keyVaultSyncPromise = Promise.resolve();

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
  },
  keyvault: {
    eyebrow: 'API key management',
    title: 'Key Vault',
    subtitle: 'Store, activate, and check credits for your API keys — used automatically across all pages.'
  }
};

function loadOddsKeyVault(provider = 'odds_api') {
  const vault = Array.isArray(oddsKeyVaultState) ? oddsKeyVaultState : [];
  return provider ? vault.filter(entry => entry?.provider === provider) : vault.slice();
}

function readLegacyOddsKeyVault() {
  try {
    const raw = JSON.parse(localStorage.getItem(KEY_VAULT_STORAGE) || '[]');
    return Array.isArray(raw) ? raw : [];
  } catch {
    return [];
  }
}

function readLegacyOddsKeyVaultActiveId() {
  try {
    return localStorage.getItem(KEY_VAULT_ACTIVE_STORAGE) || '';
  } catch {
    return '';
  }
}

async function ensureOddsKeyVaultLoaded(force = false) {
  if (!force && keyVaultBootstrapPromise) {
    return keyVaultBootstrapPromise;
  }
  keyVaultBootstrapPromise = fetch('/api/key-vault')
    .then(async function (response) {
      if (!response.ok) {
        throw new Error('Failed to load Key Vault.');
      }
      const data = await response.json();
      oddsKeyVaultState = Array.isArray(data.entries) ? data.entries : [];
      oddsKeyVaultActiveId = String(data.active_id || '');
      if (!oddsKeyVaultState.length) {
        const legacyVault = readLegacyOddsKeyVault();
        if (legacyVault.length) {
          oddsKeyVaultState = legacyVault.slice();
          oddsKeyVaultActiveId = readLegacyOddsKeyVaultActiveId();
          await saveOddsKeyVault(oddsKeyVaultState, { activeId: oddsKeyVaultActiveId });
        }
      }
      return oddsKeyVaultState;
    })
    .catch(function (error) {
      console.error('Key Vault load failed:', error);
      oddsKeyVaultState = [];
      oddsKeyVaultActiveId = '';
      throw error;
    });
  return keyVaultBootstrapPromise;
}

function saveOddsKeyVault(vault, { activeId = oddsKeyVaultActiveId } = {}) {
  oddsKeyVaultState = Array.isArray(vault) ? vault.slice() : [];
  oddsKeyVaultActiveId = String(activeId || '');
  keyVaultSyncPromise = keyVaultSyncPromise
    .catch(function () { })
    .then(async function () {
      const response = await apiFetch('/api/key-vault', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          entries: oddsKeyVaultState,
          active_id: oddsKeyVaultActiveId,
        }),
      }, 8000);
    })
    .catch(function (error) {
      console.error('Key Vault save failed:', error);
    });
  return keyVaultSyncPromise;
}

function updateOddsKeyVaultEntry(entryId, patch) {
  const vault = loadOddsKeyVault(null);
  const next = vault.map(entry => entry.id === entryId ? { ...entry, ...patch } : entry);
  saveOddsKeyVault(next);
  invalidateEligibleVaultKeyCache();
  return next.find(entry => entry.id === entryId) || null;
}

function deleteOddsKeyVaultEntry(entryId) {
  const vault = loadOddsKeyVault(null).filter(entry => entry.id !== entryId);
  const nextActiveId = oddsKeyVaultActiveId === entryId ? '' : oddsKeyVaultActiveId;
  return saveOddsKeyVault(vault, { activeId: nextActiveId });
  invalidateEligibleVaultKeyCache();
}

function maskVaultKey(key) {
  const raw = String(key || '');
  if (raw.length < 8) return '••••••••';
  return raw.slice(0, 4) + '••••••••' + raw.slice(-4);
}

function shuffleArray(items) {
  const copy = Array.isArray(items) ? items.slice() : [];
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}

function hasFreshVaultBalance(entry) {
  const checkedAt = entry?.last_checked_at ? Date.parse(entry.last_checked_at) : 0;
  return Number.isFinite(Number(entry?.remaining)) && checkedAt && ((Date.now() - checkedAt) < KEY_VAULT_BALANCE_STALE_MS);
}

function invalidateEligibleVaultKeyCache() {
  cachedEligibleVaultKeys = [];
  cachedEligibleVaultKeysAt = 0;
}

function getVaultHealth(entry, { requiredCredits = 1 } = {}) {
  const remaining = Number(entry?.remaining);
  if (!Number.isFinite(remaining)) {
    return { state: 'unknown', label: 'Unchecked', tone: 'neutral', usable: false, softDisabled: false };
  }
  if (remaining < Math.max(requiredCredits, KEY_VAULT_MIN_USABLE_CREDITS)) {
    return { state: 'critical', label: 'Unusable', tone: 'bad', usable: false, softDisabled: true };
  }
  if (remaining < KEY_VAULT_SOFT_DISABLE_CREDITS) {
    return { state: 'low', label: 'Low credits', tone: 'warning', usable: true, softDisabled: true };
  }
  return { state: 'healthy', label: 'Healthy', tone: 'good', usable: true, softDisabled: false };
}

function markVaultKeyUsed(entry, sourceLabel = 'rotation') {
  if (!entry?.id) return entry;
  return updateOddsKeyVaultEntry(entry.id, {
    last_used_at: new Date().toISOString(),
    last_used_for: sourceLabel,
  }) || entry;
}

function getCachedEligibleVaultKeys({ provider = 'odds_api', requiredCredits = 1 } = {}) {
  if (!cachedEligibleVaultKeys.length || !cachedEligibleVaultKeysAt) return [];
  if ((Date.now() - cachedEligibleVaultKeysAt) >= KEY_VAULT_BALANCE_STALE_MS) return [];
  return cachedEligibleVaultKeys.filter(entry =>
    (!provider || entry?.provider === provider) &&
    hasFreshVaultBalance(entry) &&
    getVaultHealth(entry, { requiredCredits }).usable
  );
}

function primeEligibleVaultKeyCache(entries) {
  cachedEligibleVaultKeys = Array.isArray(entries) ? entries.slice() : [];
  cachedEligibleVaultKeysAt = cachedEligibleVaultKeys.length ? Date.now() : 0;
}

async function refreshOddsVaultEntryCredits(entry, { force = false, onLowCredits } = {}) {
  await ensureOddsKeyVaultLoaded();
  if (!entry?.key) return null;
  const checkedAt = entry.last_checked_at ? Date.parse(entry.last_checked_at) : 0;
  const hasFreshBalance = Number.isFinite(Number(entry.remaining)) && checkedAt && ((Date.now() - checkedAt) < KEY_VAULT_BALANCE_STALE_MS);
  if (!force && hasFreshBalance) return entry;

  const data = await apiFetch('/api/odds/check-quota', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ api_key: entry.key })
  }, 10000);

  const next = updateOddsKeyVaultEntry(entry.id, {
    remaining: data.quota?.remaining ?? null,
    used: data.quota?.used ?? null,
    last_cost: data.quota?.last ?? null,
    last_checked_at: new Date().toISOString(),
    api_key_masked: data.api_key_masked || maskVaultKey(entry.key),
    soft_disabled: Number(data.quota?.remaining) < KEY_VAULT_SOFT_DISABLE_CREDITS,
  });

  if (Number(next?.remaining) < KEY_VAULT_MIN_USABLE_CREDITS && typeof onLowCredits === 'function') {
    await onLowCredits(next);
  }
  return next;
}

async function promptDeleteLowCreditVaultKey(entry, sourceLabel = 'this feature') {
  if (!entry?.id) return;
  const remaining = Number(entry.remaining);
  const prettyRemaining = Number.isFinite(remaining) ? remaining : 0;
  const shouldDelete = confirm(`"${entry.label}" only has ${prettyRemaining} remaining credit${prettyRemaining === 1 ? '' : 's'} and is not usable for ${sourceLabel}.\n\nDo you want to delete it from the Key Vault?`);
  if (shouldDelete) {
    deleteOddsKeyVaultEntry(entry.id);
    showAppToast(`Removed low-credit key: ${entry.label}`, 'warning');
  } else {
    invalidateEligibleVaultKeyCache();
  }
}

function syncVaultKeyIntoOddsInputs(key) {
  const raw = String(key || '').trim();
  if (!raw) return;
  if (oddsApiKeyInput) {
    oddsApiKeyInput.value = raw;
  }
  if (typeof setOddsApiKeyMeta === 'function') {
    setOddsApiKeyMeta(raw);
  }
}

async function pickRandomVaultKeyForFeature({
  provider = 'odds_api',
  requiredCredits = 1,
  sourceLabel = 'this feature',
  enforceMinimum = true,
} = {}) {
  await ensureOddsKeyVaultLoaded();
  const cachedPool = shuffleArray(getCachedEligibleVaultKeys({ provider, requiredCredits }));
  if (cachedPool.length) {
    const winner = markVaultKeyUsed(cachedPool[0], sourceLabel);
    if (winner?.key) syncVaultKeyIntoOddsInputs(winner.key);
    return winner;
  }

  const vault = loadOddsKeyVault(provider);
  if (enforceMinimum && vault.length < KEY_VAULT_MIN_ROTATING_KEYS) {
    throw new Error(`Add at least ${KEY_VAULT_MIN_ROTATING_KEYS} Odds API keys to Key Vault before using ${sourceLabel}.`);
  }
  if (!vault.length) {
    throw new Error('No Odds API keys saved in Key Vault.');
  }

  const shuffled = shuffleArray(vault);
  const healthyEligible = [];
  const fallbackEligible = [];
  for (const candidate of shuffled) {
    let refreshed = candidate;
    try {
      refreshed = await refreshOddsVaultEntryCredits(candidate, {
        force: false,
        onLowCredits: async (lowEntry) => promptDeleteLowCreditVaultKey(lowEntry, sourceLabel),
      });
    } catch (error) {
      console.warn('Key credit check failed for', candidate.label, error);
      continue;
    }
    const health = getVaultHealth(refreshed, { requiredCredits });
    if (health.usable) {
      if (health.softDisabled) fallbackEligible.push(refreshed);
      else healthyEligible.push(refreshed);
    }
  }
  const eligible = healthyEligible.length ? healthyEligible : fallbackEligible;
  if (eligible.length) {
    primeEligibleVaultKeyCache(eligible);
    const winner = markVaultKeyUsed(shuffleArray(eligible)[0], sourceLabel);
    if (winner?.key) syncVaultKeyIntoOddsInputs(winner.key);
    return winner;
  }
  throw new Error(`No usable Odds API key found in Key Vault for ${sourceLabel}.`);
}

async function getRotatingVaultKeysForFeature({
  provider = 'odds_api',
  minimumKeys = KEY_VAULT_MIN_ROTATING_KEYS,
  requiredCredits = 1,
  sourceLabel = 'this feature',
} = {}) {
  await ensureOddsKeyVaultLoaded();
  const cachedPool = getCachedEligibleVaultKeys({ provider, requiredCredits });
  if (cachedPool.length >= minimumKeys) {
    const eligible = shuffleArray(cachedPool).slice(0, Math.max(minimumKeys, cachedPool.length));
    const first = markVaultKeyUsed(eligible[0], sourceLabel);
    if (first?.key) syncVaultKeyIntoOddsInputs(first.key);
    return eligible;
  }

  const vault = loadOddsKeyVault(provider);
  if (vault.length < minimumKeys) {
    throw new Error(`Add at least ${minimumKeys} Odds API keys to Key Vault before using ${sourceLabel}.`);
  }
  const shuffled = shuffleArray(vault);
  const healthyEligible = [];
  const fallbackEligible = [];
  for (const candidate of shuffled) {
    try {
      const refreshed = await refreshOddsVaultEntryCredits(candidate, {
        force: false,
        onLowCredits: async (lowEntry) => promptDeleteLowCreditVaultKey(lowEntry, sourceLabel),
      });
      const health = getVaultHealth(refreshed, { requiredCredits });
      if (health.usable) {
        if (health.softDisabled) fallbackEligible.push(refreshed);
        else healthyEligible.push(refreshed);
      }
    } catch (error) {
      console.warn('Key credit check failed for', candidate.label, error);
    }
  }
  const eligible = healthyEligible.length >= minimumKeys
    ? healthyEligible
    : healthyEligible.concat(fallbackEligible);
  if (eligible.length < minimumKeys) {
    throw new Error(`Only ${eligible.length} usable Odds API key${eligible.length === 1 ? '' : 's'} found. ${sourceLabel} requires at least ${minimumKeys}.`);
  }
  primeEligibleVaultKeyCache(eligible);
  const first = markVaultKeyUsed(eligible[0], sourceLabel);
  if (first?.key) syncVaultKeyIntoOddsInputs(first.key);
  return eligible;
}

// ── Global app toast ─────────────────────────────────────────────────
function showAppToast(msg, type = 'default', options = {}) {
  if (type && typeof type === 'object') {
    options = type;
    type = options.type || 'default';
  }
  const icons = {
    default: '•',
    success: '✓',
    info: 'i',
    warning: '!',
    error: '×'
  };
  let host = document.getElementById('_appToastHost');
  if (!host) {
    host = document.createElement('div');
    host.id = '_appToastHost';
    host.className = 'app-toast-host';
    document.body.appendChild(host);
  }
  const normalizedType = ['success', 'warning', 'info', 'error'].includes(type) ? type : 'default';
  const toast = document.createElement('div');
  toast.className = `app-toast app-toast--${normalizedType}`;
  toast.setAttribute('role', 'status');
  toast.innerHTML = `
    <span class="app-toast-icon">${icons[normalizedType] || icons.default}</span>
    <div class="app-toast-body"><span class="app-toast-text"></span></div>
  `;
  toast.querySelector('.app-toast-text').textContent = String(msg || '');
  if (options.actionLabel && typeof options.onAction === 'function') {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'app-toast-action';
    btn.textContent = options.actionLabel;
    btn.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      try { options.onAction(); } catch (e) {}
      toast.classList.remove('show');
      setTimeout(() => toast.remove(), 220);
    });
    toast.appendChild(btn);
  }
  if (options.undoLabel && typeof options.onUndo === 'function') {
    const undoBtn = document.createElement('button');
    undoBtn.type = 'button';
    undoBtn.className = 'app-toast-action is-undo';
    undoBtn.textContent = options.undoLabel || 'Undo';
    undoBtn.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      try { options.onUndo(); } catch (e) {}
      toast.classList.remove('show');
      setTimeout(() => toast.remove(), 220);
    });
    toast.appendChild(undoBtn);
  }
  if (options.persist) {
    toast.classList.add('is-persistent');
  }
  host.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add('show'));
  const baseDuration = Number(options.duration || 0);
  let ttl = baseDuration;
  if (!ttl) {
    if (normalizedType === 'success') ttl = 2200;
    else if (normalizedType === 'info') ttl = 2600;
    else if (normalizedType === 'warning') ttl = 3600;
    else if (normalizedType === 'error') ttl = 4200;
    else ttl = 2600;
    if (options.undoLabel || options.actionLabel) {
      ttl = Math.max(ttl, 4200);
    }
  }
  if (!options.persist) {
    setTimeout(() => {
      toast.classList.remove('show');
      setTimeout(() => toast.remove(), 240);
    }, ttl);
  }
}

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
    const shouldForce = todayGamesLastErrorAt > 0 || !latestTodayGamesPayload;
    loadTodayGames(shouldForce);
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

function buildTrustDiagnostics({ availability = null, environment = null, teamContext = null, marketSide = '', selectedSide = '', injuryFilterNames = [], teamInjuryNames = [], marketDisagrees = null, marketOddsAvailable = false } = {}) {
  const items = [];
  const headline = String(availability?.headline || availability?.status || '').toLowerCase();
  if (availability) {
    if (/pending report/.test(headline)) {
      items.push({ tone: 'warning', label: 'Official report pending' });
    } else if (/report unavailable/.test(headline) || /unavailable/.test(headline)) {
      items.push({ tone: 'bad', label: 'Official report unavailable' });
    } else if (availability.is_unavailable) {
      items.push({ tone: 'bad', label: availability.status || 'Player unavailable' });
    } else if (availability.is_risky) {
      items.push({ tone: 'warning', label: availability.status || 'Status risk' });
    } else {
      items.push({ tone: 'good', label: 'Official status loaded' });
    }
  }

  const hasMarketContext = Number.isFinite(Number(environment?.market_team_total)) ||
    Number.isFinite(Number(environment?.market_game_total)) ||
    Number.isFinite(Number(environment?.market_spread));
  if (hasMarketContext) {
    items.push({ tone: 'good', label: 'Market context loaded' });
  } else if (marketOddsAvailable) {
    items.push({ tone: 'neutral', label: 'Market odds loaded' });
  } else {
    items.push({ tone: 'neutral', label: 'Market context missing' });
  }

  const filterNames = Array.isArray(injuryFilterNames) ? injuryFilterNames.filter(Boolean) : [];
  const injuryNames = Array.isArray(teamInjuryNames) ? teamInjuryNames.filter(Boolean) : [];
  if (filterNames.length) {
    items.push({ tone: 'warning', label: `Lineup filter: ${filterNames.length} out` });
  } else if (injuryNames.length) {
    items.push({ tone: 'neutral', label: `${injuryNames.length} same-team absence${injuryNames.length === 1 ? '' : 's'} noted` });
  } else if (Number(teamContext?.impact_count || 0) > 0) {
    items.push({ tone: 'warning', label: `${Number(teamContext.impact_count)} lineup flag${Number(teamContext.impact_count) === 1 ? '' : 's'}` });
  } else {
    items.push({ tone: 'neutral', label: 'No lineup filter used' });
  }

  const side = String(selectedSide || '').toUpperCase();
  const marketLean = String(marketSide || '').toUpperCase();
  const disagreement = marketDisagrees == null ? (!!side && !!marketLean && side !== marketLean) : !!marketDisagrees;
  if (marketLean) {
    items.push({
      tone: disagreement ? 'warning' : 'good',
      label: disagreement ? `Picked against market lean (${marketLean})` : `Aligned with market lean (${marketLean})`
    });
  }

  return items.slice(0, 4);
}

function renderTrustDiagnostics(items = [], compact = false) {
  if (!Array.isArray(items) || !items.length) return '';
  return `<div class="trust-diagnostics ${compact ? 'compact' : ''}">${
    items.map(item => `<span class="trust-pill tone-${escapeHtml(item.tone || 'neutral')}">${escapeHtml(item.label || '')}</span>`).join('')
  }</div>`;
}

function getWithoutPlayerNamesFromPayload(payload) {
  const names = payload?.active_filters?.without_player_names;
  if (Array.isArray(names) && names.length) return names.filter(Boolean);
  const single = payload?.active_filters?.without_player_name;
  return single ? [single] : [];
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
  const bestBet = item?.best_bet || {};
  const teamContext = item?.analysis?.team_context || item?.team_context || {};

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
  if (bestBet.market_side) {
    angles.push({
      key: bestBet.market_disagrees ? 'market_disagrees' : 'market_aligned',
      label: bestBet.market_disagrees ? `Against market lean • ${bestBet.market_side}` : `Market aligned • ${bestBet.market_side}`,
      tone: bestBet.market_disagrees ? 'tough' : 'positive',
      kind: 'market'
    });
  }
  if (Number.isFinite(Number(bestBet.market_penalty)) && Number(bestBet.market_penalty) > 0) {
    angles.push({
      key: 'market_penalty',
      label: `Market penalty • -${Number(bestBet.market_penalty).toFixed(0)}`,
      tone: 'tough',
      kind: 'market'
    });
  }
  if (Number(teamContext?.impact_count || 0) > 0) {
    angles.push({
      key: 'lineup_context',
      label: `${Number(teamContext.impact_count)} lineup flag${Number(teamContext.impact_count) === 1 ? '' : 's'}`,
      tone: Number(teamContext.impact_count) >= 2 ? 'positive' : 'info',
      kind: 'lineup'
    });
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

function setOverviewBoardTab(tabKey = 'top') {
  overviewBoardTab = ['top', 'caution', 'boost'].includes(tabKey) ? tabKey : 'top';
  if (overviewBoardTabsEl) {
    overviewBoardTabsEl.querySelectorAll('.overview-board-tab').forEach(btn => {
      const active = btn.dataset.boardTab === overviewBoardTab;
      btn.classList.toggle('active', active);
      btn.setAttribute('aria-selected', active ? 'true' : 'false');
    });
  }
  document.querySelectorAll('[data-board-panel]').forEach(panel => {
    const active = panel.dataset.boardPanel === overviewBoardTab;
    panel.classList.toggle('active', active);
    panel.hidden = !active;
  });
}

function setSelectedPlayer(player) {
  const previousId = selectedPlayer?.id ?? null;
  const nextId = player?.id ?? null;
  const changedPlayer = previousId !== nextId;

  selectedPlayer = player;

  if (changedPlayer) {
    resetAnalyzerFiltersState();
    clearAnalysisForNewSelection();
    setStatus('Player selected');
  }

  renderSelectedPlayer();
  populateAnalyzerWithoutPlayerFilter(true).catch(() => { });
  loadInjuryBoostChips(player).catch(() => { });
  playerSearchInput.value = player.full_name;
  searchResults.classList.add('hidden');
  updateSelectedCardStyles();
  saveRecentPlayer(player);

  if (player.team_abbreviation) {
    applyTeamAccent(player.team_abbreviation);
  }

  // Persist last player across refreshes
  try {
    localStorage.setItem(LAST_PLAYER_KEY, JSON.stringify({
      id: player.id, full_name: player.full_name, is_active: player.is_active,
      team_abbreviation: player.team_abbreviation || '', team_name: player.team_name || '',
      team_id: player.team_id || null, position: player.position || '', jersey: player.jersey || ''
    }));
  } catch { /* ignore */ }
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

  let data;
  try {
    data = await apiFetch('/api/teams', {}, 10000);
  } catch (err) {
    teamSelect.innerHTML = '<option value="">Failed to load teams</option>';
    throw err;
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
  const payload = await apiFetch(`/api/teams/${teamId}/roster${query ? `?${query}` : ''}`, {}, 15000);

  rosterPlayers = payload.results || [];
  selectedTeam = payload.team;
  applyTeamAccent(payload.team?.abbreviation);
  renderRoster(payload.team, payload.season, rosterPlayers);
}

function renderRoster(team, season, players) {
  rosterTitle.textContent = team.full_name + ' roster';
  const outCnt = players.filter(p => p.is_unavailable).length;
  const riskCnt = players.filter(p => p.is_risky && !p.is_unavailable).length;
  let injNote = '';
  if (outCnt) injNote += ' • ' + outCnt + ' out';
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
    const injSt = player.injury_status || '';
    const isOut = !!player.is_unavailable;
    const isRisk = !!player.is_risky && !isOut;
    const cardCls = isOut ? 'player-card player-card--out' : isRisk ? 'player-card player-card--risk' : 'player-card';
    const badge = isOut
      ? `<span class="inj-badge out">${escapeHtml(injSt || 'OUT')}</span>`
      : isRisk
        ? `<span class="inj-badge risk">${escapeHtml(injSt || 'Q')}</span>`
        : `<span class="inj-badge ok">Active</span>`;
    const hoverLine = player.last_game ? `Last ${player.last_game}` : (player.avg ? `Avg ${player.avg}` : '');
    const hoverHtml = hoverLine ? `<div class="player-card-hover"><span>${escapeHtml(hoverLine)}</span></div>` : `<div class="player-card-hover"><span>Tap to analyze</span></div>`;
    return `<button class="${cardCls}" data-id="${player.id}">
      <div class="player-card-avatar">
        <img src="${getPlayerImage(player.id)}" alt="${escapeHtml(player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
      </div>
      <div class="player-card-head">
        <div class="player-card-name">${escapeHtml(player.full_name)}</div>
        <span class="jersey-pill">${escapeHtml(player.jersey || '--')}</span>
      </div>
      <div class="player-card-meta">
        <span class="player-card-pos">${escapeHtml(player.position || 'N/A')}</span>
        ${badge}
      </div>
      <div class="player-card-team">${escapeHtml(player.team_abbreviation || team.abbreviation)}</div>
      ${hoverHtml}
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
    const d = await apiFetch('/api/teams/' + teamId + '/injury-report', {}, 12000);
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
      ps.map(function (p) {
        const cls = p.is_unavailable ? 'out' : p.is_risky ? 'risk' : 'ok';
        return '<div class="inj-panel-row ' + cls + '">' +
          '<span class="ipn">' + escapeHtml(formatPlayerName(p.name)) + '</span>' +
          '<span class="ips inj-badge ' + cls + '">' + escapeHtml(p.status) + '</span>' +
          '<span class="ipr">' + escapeHtml(p.reason || '') + '</span>' +
          '</div>';
      }).join('') +
      '</div>';
  } catch (e) {
    panel.innerHTML = '<div class="inj-panel-loading">Could not load injury report.</div>';
  }
}

async function searchPlayers(query) {
  return apiFetch(`/api/players/search?q=${encodeURIComponent(query)}`, {}, 8000);
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

function getTodayGameKey(game) {
  return `${game?.away?.team_id || game?.away?.abbreviation || 'away'}-${game?.home?.team_id || game?.home?.abbreviation || 'home'}-${game?.game_label || game?.status_text || ''}`;
}

function buildTodayTrustDiagnostics(game, hasMarketContext) {
  const items = [];
  const teams = [
    { abbr: game?.away?.abbreviation || 'Away', availability: game?.away?.availability || null, injuries: Array.isArray(game?.away?.injury_players) ? game.away.injury_players : [] },
    { abbr: game?.home?.abbreviation || 'Home', availability: game?.home?.availability || null, injuries: Array.isArray(game?.home?.injury_players) ? game.home.injury_players : [] },
  ];

  const pendingTeams = teams.filter(team => /pending report/i.test(String(team.availability?.headline || team.availability?.status || '')));
  const unavailableTeams = teams.filter(team => /report unavailable/i.test(String(team.availability?.headline || team.availability?.status || '')));
  if (pendingTeams.length) {
    items.push({ tone: 'warning', label: `${pendingTeams.map(team => team.abbr).join('/')} report pending` });
  } else if (unavailableTeams.length) {
    items.push({ tone: 'bad', label: `${unavailableTeams.map(team => team.abbr).join('/')} report unavailable` });
  } else {
    items.push({ tone: 'good', label: 'Official reports loaded' });
  }

  if (hasMarketContext) {
    items.push({ tone: 'good', label: 'Market context loaded' });
  } else if (game?.__marketContextPending) {
    items.push({ tone: 'neutral', label: 'Market context loading' });
  } else if (game?.__marketContextAttempted) {
    items.push({ tone: 'neutral', label: 'Market context unavailable' });
  }

  const totalListedAbsences = teams.reduce((sum, team) => sum + team.injuries.length, 0);
  if (totalListedAbsences > 0) {
    items.push({ tone: totalListedAbsences >= 4 ? 'warning' : 'neutral', label: `${totalListedAbsences} listed absence${totalListedAbsences === 1 ? '' : 's'}` });
  } else if (!pendingTeams.length && !unavailableTeams.length) {
    items.push({ tone: 'good', label: 'Clean reports' });
  }

  return items.slice(0, 4);
}

function buildTodayGameCard(game, compact = false) {
  const statusClass = game.status_category || 'scheduled';
  const homeSummary = game.home.availability?.headline || 'Clean report';
  const awaySummary = game.away.availability?.headline || 'Clean report';
  const marketContext = game.market_context || {};
  const hasMarketContext = Number.isFinite(Number(marketContext.market_game_total)) ||
    Number.isFinite(Number(marketContext.market_home_spread)) ||
    Number.isFinite(Number(marketContext.market_away_spread));
  const gameTotalValue = Number.isFinite(Number(marketContext.market_game_total)) ? Number(marketContext.market_game_total).toFixed(1) : '—';
  const awayTotalValue = Number.isFinite(Number(marketContext.market_away_implied_total)) ? Number(marketContext.market_away_implied_total).toFixed(1) : '—';
  const homeTotalValue = Number.isFinite(Number(marketContext.market_home_implied_total)) ? Number(marketContext.market_home_implied_total).toFixed(1) : '—';
  const awaySpreadValue = Number.isFinite(Number(marketContext.market_away_spread))
    ? `${Number(marketContext.market_away_spread) > 0 ? '+' : ''}${Number(marketContext.market_away_spread).toFixed(1)}`
    : '—';
  const homeSpreadValue = Number.isFinite(Number(marketContext.market_home_spread))
    ? `${Number(marketContext.market_home_spread) > 0 ? '+' : ''}${Number(marketContext.market_home_spread).toFixed(1)}`
    : '—';
  const trustDiagnostics = buildTodayTrustDiagnostics(game, hasMarketContext);
  const marketRow = hasMarketContext
    ? `<div class="today-market-row">
        <span class="today-market-pill">GT ${escapeHtml(gameTotalValue)}</span>
        <span class="today-market-pill">${escapeHtml(game.away.abbreviation)} TT ${escapeHtml(awayTotalValue)}</span>
        <span class="today-market-pill">${escapeHtml(game.home.abbreviation)} TT ${escapeHtml(homeTotalValue)}</span>
        <span class="today-market-pill">${escapeHtml(game.away.abbreviation)} ${escapeHtml(awaySpreadValue)}</span>
        <span class="today-market-pill">${escapeHtml(game.home.abbreviation)} ${escapeHtml(homeSpreadValue)}</span>
      </div>`
    : '';
  const scoreLine = game.status_category === 'scheduled'
    ? `<div class="today-game-time">${escapeHtml(game.status_text)}</div>`
    : `<div class="today-game-scoreline"><span>${game.away.score}</span><small>-</small><span>${game.home.score}</span></div>`;

  // Injury summary chips for each team
  function injChips(teamData) {
    const players = teamData.injury_players || [];
    if (!players.length) {
      const headline = teamData.availability?.headline || '';
      if (/pending report/i.test(headline)) return '<span class="today-inj-pending">Pending report</span>';
      if (/report unavailable/i.test(headline)) return '<span class="today-inj-pending">Report unavailable</span>';
      return '<span class="today-inj-clean">\u2713 Clean</span>';
    }
    return players.slice(0, 4).map(function (p) {
      const cls = p.is_unavailable ? 'out' : 'risky';
      const fullName = formatPlayerName(p.full_name || p.short_name || '');
      const chipTitle = [fullName, p.status, p.injury_reason].filter(Boolean).join(' • ');
      return '<span class="today-inj-chip ' + cls + '" title="' + escapeHtml(chipTitle) + '">'
        + escapeHtml(fullName) + '</span>';
    }).join('') + (players.length > 4 ? '<span class="today-inj-more">+' + (players.length - 4) + '</span>' : '');
  }

  const awayInj = injChips(game.away);
  const homeInj = injChips(game.home);
  const gameKey = (game.away.team_id || '') + '-' + (game.home.team_id || '');
  const awayLogo = game.away?.team_id ? getTeamLogo(game.away.team_id) : '';
  const homeLogo = game.home?.team_id ? getTeamLogo(game.home.team_id) : '';

  return `
    <article class="today-game-card ${compact ? 'compact' : ''} ${statusClass}" data-game-key="${gameKey}">
      <div class="today-game-head">
        <span class="small-badge ${statusClass}">${escapeHtml(game.status_text)}</span>
        <span class="small-meta">${escapeHtml(game.game_label)}</span>
      </div>
      ${renderTrustDiagnostics(trustDiagnostics, true)}
      ${marketRow}
      <div class="today-game-main">
        <div class="today-team-row with-logos">
          <div class="today-team-side">
            ${awayLogo ? `<span class="today-team-logo-shell"><img class="today-team-logo" src="${awayLogo}" alt="${escapeHtml(game.away.abbreviation)} logo" onerror="this.parentElement.style.display='none'"></span>` : ''}
            <div>
            <strong>${escapeHtml(game.away.abbreviation)}</strong>
            <small>${escapeHtml(awaySummary)}</small>
            </div>
          </div>
          ${scoreLine}
          <div class="today-team-home">
            <div>
            <strong>${escapeHtml(game.home.abbreviation)}</strong>
            <small>${escapeHtml(homeSummary)}</small>
            </div>
            ${homeLogo ? `<span class="today-team-logo-shell"><img class="today-team-logo" src="${homeLogo}" alt="${escapeHtml(game.home.abbreviation)} logo" onerror="this.parentElement.style.display='none'"></span>` : ''}
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
    btn.addEventListener('click', function () {
      const gameKey = btn.dataset.gameKey;
      const panel = document.getElementById('inj-panel-' + gameKey);
      if (!panel) return;
      const open = panel.style.display !== 'none';
      panel.style.display = open ? 'none' : '';
      btn.textContent = open ? '\uD83E\uDE79 Injuries' : '\u25B2 Hide';
    });
  });
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
    await populateAnalyzerWithoutPlayerFilter();
    const params = new URLSearchParams({
      team_id: teamId,
      stat: selectedStat,
      line: lineInput.value,
      last_n: gamesSelect.value
    });

    const season = seasonInput.value.trim();
    if (season) params.set('season', season);

    const payload = await apiFetch(`/api/bet-finder?${params.toString()}`, {}, 30000);

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
    const rawOverrideTeamId = Number(prop.opponent_team_id || embeddedNextGame?.opponent_team_id || 0) || null;
    const teamAbbrUpper = String(prop.team_abbreviation || rosterMatch?.team_abbreviation || '').toUpperCase();
    const teamIdStr = teamId ? String(teamId) : '';
    const deriveOpponentId = () => {
      const options = Array.from((oppSelect?.options || teamSelect?.options || []));
      const findIdByText = (text) => {
        const normalized = String(text || '').toUpperCase();
        if (!normalized) return null;
        for (const opt of options) {
          const abbr = String(opt.dataset?.abbreviation || '').toUpperCase();
          if (abbr && (normalized === abbr || normalized.includes(abbr))) return opt.value;
          const label = String(opt.textContent || '').toUpperCase();
          if (label && normalized.includes(label)) return opt.value;
        }
        return null;
      };
      const homeId = findIdByText(prop.home_team);
      const awayId = findIdByText(prop.away_team);
      if (homeId && awayId) {
        if (teamIdStr && homeId === teamIdStr) return awayId;
        if (teamIdStr && awayId === teamIdStr) return homeId;
      }
      const label = String(prop.game_label || '').toUpperCase();
      if (label) {
        const abbrs = (label.match(/[A-Z]{2,3}/g) || []).filter(Boolean);
        for (const abbr of abbrs) {
          if (teamAbbrUpper && abbr === teamAbbrUpper) continue;
          const id = findIdByText(abbr);
          if (id && (!teamIdStr || id !== teamIdStr)) return id;
        }
      }
      return null;
    };
    let overrideTeamId = rawOverrideTeamId;
    if (overrideTeamId && teamIdStr && String(overrideTeamId) === teamIdStr) {
      overrideTeamId = null;
    }
    if (!overrideTeamId) {
      overrideTeamId = deriveOpponentId();
    }

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
      market_over_odds: prop.over_odds ?? prop.best_over_odds ?? prop.market_over_odds ?? null,
      market_under_odds: prop.under_odds ?? prop.best_under_odds ?? prop.market_under_odds ?? null,
    };

    if (typeof setSelectedPlayer === 'function') {
      setSelectedPlayer(selectedPayload);
    }
    if (typeof setActiveProp === 'function') setActiveProp(prop.stat);
    if (typeof lineInput !== 'undefined' && lineInput) lineInput.value = prop.line;
    if (typeof switchView === 'function') switchView('analyzer');
    if (typeof analyzePlayerProp === 'function') {
      await analyzePlayerProp({ preserveScroll: true, overrideLastN: prop.last_n || null });
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

  const formatMaybe = (value, digits = 2) => {
    const num = Number(value);
    return Number.isFinite(num) ? num.toFixed(digits) : '—';
  };
  let summaryText = 'Defense-vs-position data unavailable for this player and stat.';
  if (vsPosition) {
    summaryText = `${nextGame?.opponent_name || 'This opponent'} allows ${formatMaybe(vsPosition.opponent_value)} ${(getStatLabel(vsPosition.stat) || '').toLowerCase()} per player-game to ${(vsPosition.position_label || 'this position').toLowerCase()}, versus a league baseline of ${formatMaybe(vsPosition.league_average)} (${Number.isFinite(Number(vsPosition.delta_pct)) ? `${formatDelta(vsPosition.delta_pct)}%` : '—'}).`;
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
        <strong>${vsPosition ? formatMaybe(vsPosition.opponent_value) : '—'}</strong>
        <small>${vsPosition ? `League avg ${formatMaybe(vsPosition.league_average)}` : 'No sample'}</small>
      </article>
      <article class="matchup-tile">
        <span class="small-label">Delta vs average</span>
        <strong class="${leanTone === 'good' ? 'match-good' : leanTone === 'bad' ? 'match-bad' : ''}">${vsPosition && Number.isFinite(Number(vsPosition.delta_pct)) ? `${formatDelta(vsPosition.delta_pct)}%` : '—'}</strong>
        <small>${escapeHtml(vsPosition?.lean || 'Neutral')}</small>
      </article>
      <article class="matchup-tile">
        <span class="small-label">Sample</span>
        <strong>${vsPosition ? formatMaybe(vsPosition.sample_gp, 0) : '—'}</strong>
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
    sport: oddsSportSelect?.value || 'basketball_nba',
    regions: oddsRegionsInput?.value?.trim() || 'us',
    odds_format: oddsFormatSelect?.value || 'decimal',
    markets: oddsMarketsInput?.value?.trim() || '',
  };
  return settings;
}

function getGameContextRequestData(source) {
  const nextGame = source?.matchup?.next_game || source?.analysis?.matchup?.next_game || {};
  const player = source?.player || source?.analysis?.player || {};
  const teamName = String(player.team_name || player.team || '').trim();
  const teamAbbreviation = String(player.team_abbreviation || nextGame.player_team_abbreviation || '').trim();
  const opponentName = String(nextGame.opponent_name || player.opponent_name || source?.player?.opponent_name || '').trim();
  const opponentAbbreviation = String(nextGame.opponent_abbreviation || player.opponent || source?.player?.opponent || '').trim();
  if (!teamName || (!opponentName && !opponentAbbreviation)) return null;
  return {
    team_name: teamName,
    team_abbreviation: teamAbbreviation,
    opponent_name: opponentName,
    opponent_abbreviation: opponentAbbreviation,
  };
}

function mergeMarketEnvironmentIntoItem(item, marketContextResult) {
  if (!item || !marketContextResult?.environment) return item;
  const mergedEnvironment = {
    ...((item.analysis && item.analysis.environment) || item.environment || {}),
    ...(marketContextResult.environment || {}),
  };
  if (item.analysis && typeof item.analysis === 'object') {
    item.analysis.environment = mergedEnvironment;
  }
  item.environment = mergedEnvironment;
  if (item.market && typeof item.market === 'object') {
    item.market.market_context = marketContextResult.context || {};
  }
  return item;
}

async function fetchAnalyzerGameContext(source) {
  const requestData = getGameContextRequestData(source);
  if (!requestData) return null;

  const existingEnvironment = source?.environment && typeof source.environment === 'object' ? source.environment : {};
  const hasExistingMarketContext =
    Number.isFinite(Number(existingEnvironment.market_team_total)) ||
    Number.isFinite(Number(existingEnvironment.market_game_total)) ||
    Number.isFinite(Number(existingEnvironment.market_spread));
  if (hasExistingMarketContext) {
    return { ok: true, environment: existingEnvironment, context: existingEnvironment.market_context || null };
  }

  const settings = getOddsApiSettings();
  const cacheKey = JSON.stringify({
    requestData,
    sport: settings.sport || 'basketball_nba',
    regions: settings.regions || 'us',
    odds_format: settings.odds_format || 'decimal',
  });
  if (analyzerGameContextCache.has(cacheKey)) {
    return analyzerGameContextCache.get(cacheKey);
  }

  let keyEntry = null;
  try {
    keyEntry = await pickRandomVaultKeyForFeature({
      requiredCredits: 1,
      sourceLabel: 'Player Analyzer market context',
    });
  } catch (error) {
    console.warn(error.message || error);
    return null;
  }

  const result = await apiFetch('/api/odds/game-context', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      api_key: keyEntry.key,
      sport: settings.sport || 'basketball_nba',
      regions: settings.regions || 'us',
      odds_format: settings.odds_format || 'decimal',
      ...requestData,
    })
  }, 12000).catch(() => null);
  if (!result?.ok) return null;
  analyzerGameContextCache.set(cacheKey, result);
  return result;
}

async function enrichTodayGamesWithMarketContext(payload) {
  const games = Array.isArray(payload?.games) ? payload.games : [];
  const pendingGames = games.filter(game =>
    !game?.market_context &&
    game?.home?.full_name &&
    game?.away?.full_name
  );
  if (!pendingGames.length) return false;

  pendingGames.forEach(game => {
    game.__marketContextPending = true;
    game.__marketContextAttempted = true;
  });

  let keyEntries = [];
  try {
    keyEntries = await getRotatingVaultKeysForFeature({
      minimumKeys: 1,
      requiredCredits: 1,
      sourceLabel: "Today's Games market context",
    });
  } catch (error) {
    console.warn(error.message || error);
    return false;
  }
  if (!keyEntries.length) return false;

  const settings = getOddsApiSettings();
  let changed = false;
  await Promise.allSettled(pendingGames.map(async (game, index) => {
    const cacheKey = `${game?.away?.full_name || ''}|${game?.home?.full_name || ''}|${settings.sport || 'basketball_nba'}|${settings.regions || 'us'}|${settings.odds_format || 'decimal'}`;
    if (todayGameContextCache.has(cacheKey)) {
      const cached = todayGameContextCache.get(cacheKey) || {};
      game.market_context = cached.context || {};
      game.market_environment = cached.environment || {};
      game.__marketContextPending = false;
      changed = true;
      return;
    }
    const keyEntry = keyEntries[index % keyEntries.length];
    if (!keyEntry?.key) {
      game.__marketContextPending = false;
      return;
    }
    const result = await apiFetch('/api/odds/game-context', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        api_key: keyEntry.key,
        sport: settings.sport || 'basketball_nba',
        regions: settings.regions || 'us',
        odds_format: settings.odds_format || 'decimal',
        team_name: game.away.full_name || '',
        opponent_name: game.home.full_name || '',
        team_abbreviation: game.away.abbreviation || '',
        opponent_abbreviation: game.home.abbreviation || '',
      })
    }, 12000).catch(() => null);
    game.__marketContextPending = false;
    if (!result?.ok) return;
    game.market_context = result.context || {};
    game.market_environment = result.environment || {};
    todayGameContextCache.set(cacheKey, result);
    changed = true;
  }));
  pendingGames.forEach(game => {
    game.__marketContextPending = false;
  });
  return changed;
}

function persistOddsApiSettings() {
  const settings = getOddsApiSettings();
  localStorage.setItem(ODDS_API_SETTINGS_STORAGE, JSON.stringify({
    sport: settings.sport,
    regions: settings.regions,
    odds_format: settings.odds_format,
    markets: settings.markets,
  }));
  setOddsApiKeyMeta('Key Vault rotation');
}

function loadStoredOddsApiSettings() {
  let storedSettings = {};
  try {
    storedSettings = JSON.parse(localStorage.getItem(ODDS_API_SETTINGS_STORAGE) || '{}');
  } catch {
    storedSettings = {};
  }
  if (oddsSportSelect && storedSettings.sport) oddsSportSelect.value = storedSettings.sport;
  if (oddsRegionsInput && storedSettings.regions) oddsRegionsInput.value = storedSettings.regions;
  if (oddsFormatSelect && storedSettings.odds_format) oddsFormatSelect.value = storedSettings.odds_format;
  if (oddsMarketsInput && storedSettings.markets) oddsMarketsInput.value = storedSettings.markets;
  setOddsApiKeyMeta('Key Vault rotation');
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
  let keyEntry = null;
  try {
    keyEntry = await pickRandomVaultKeyForFeature({
      requiredCredits: 1,
      sourceLabel: 'Odds event loading',
    });
  } catch (error) {
    alert(error.message || 'No usable Odds API key found in Key Vault.');
    return;
  }
  persistOddsApiSettings();
  setOddsApiStatus('Loading events...', 'working');
  setOddsQuotaMeta(null);
  if (oddsLoadEventsBtn) oddsLoadEventsBtn.disabled = true;
  try {
    const payload = await apiFetch('/api/odds/events', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: keyEntry.key, sport: settings.sport })
    }, 15000);
    renderOddsEvents(payload.events || []);
    setOddsQuotaMeta(payload.quota);
    setOddsApiKeyMeta(payload.api_key_used || keyEntry.key);
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
  let keyEntry = null;
  try {
    keyEntry = await pickRandomVaultKeyForFeature({
      requiredCredits: 1,
      sourceLabel: 'Odds props import',
    });
  } catch (error) {
    alert(error.message || 'No usable Odds API key found in Key Vault.');
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
    const payload = await apiFetch('/api/odds/player-props-import', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        api_key: keyEntry.key,
        sport: settings.sport,
        event_id: eventId,
        regions: settings.regions,
        odds_format: settings.odds_format,
        markets: settings.markets,
      })
    }, 30000);
    setOddsQuotaMeta(payload.quota);
    setOddsApiKeyMeta(payload.api_key_used || keyEntry.key);
    const csvRows = Array.isArray(payload.csv_rows) ? payload.csv_rows : [];
    if (!csvRows.length) {
      setOddsApiStatus('No props found', 'bad');
      alert('No complete Over/Under player prop rows were found for this event and market selection.');
      return;
    }
    marketTextarea.value = csvRows.join('\n');
    marketMeta.textContent = `${csvRows.length} imported row(s) • ${payload.event?.away_team || ''} @ ${payload.event?.home_team || ''} • Credits used ${payload.quota?.used ?? '—'} • Remaining ${payload.quota?.remaining ?? '—'}`;
    setOddsApiStatus(`Imported ${csvRows.length} prop row(s)`, 'good');
    const importedRows = Array.isArray(payload.import_rows) ? payload.import_rows : [];
    const scanRows = importedRows.map(row => ({
      player_name: row.player_name,
      stat: row.stat,
      line: row.line,
      over_odds: row.over_odds,
      under_odds: row.under_odds,
      over_fair_prob: row.over_fair_prob,
      under_fair_prob: row.under_fair_prob,
      consensus_over_fair_prob: row.consensus_over_fair_prob,
      consensus_under_fair_prob: row.consensus_under_fair_prob,
      hold_percent: row.hold_percent ?? row.consensus_hold_percent,
      books_count: row.books_count,
      best_over_odds: row.best_over_odds,
      best_under_odds: row.best_under_odds,
      best_over_bookmaker: row.best_over_bookmaker,
      best_under_bookmaker: row.best_under_bookmaker,
      market_implied_line: row.market_implied_line,
      bookmaker_title: row.bookmaker_title,
      game_label: row.game_label,
      home_team: row.home_team,
      away_team: row.away_team,
    }));
    await runMarketScan(scanRows.length ? scanRows : null);
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

async function focusMarketPlayer(item, options = {}) {
  if (!item?.player?.id) return;

  const teamId = item.player.team_id;
  if (teamId && String(teamSelect.value) !== String(teamId)) {
    teamSelect.value = String(teamId);
    try {
      await loadRoster(teamId);
    } catch (rosterErr) {
      console.warn('Roster load failed, continuing with player from market scan:', rosterErr.message || rosterErr);
    }
  }

  const analysis = item.analysis || {};

  setSelectedPlayer({
    id: item.player.id,
    full_name: item.player.full_name,
    is_active: true,
    team_abbreviation: item.player.team_abbreviation || '',
    team_name: item.player.team_name || '',
    team_id: item.player.team_id || null,
    position: item.player.position || '',
    jersey: item.player.jersey || '',
    availability: analysis.availability || item.availability || null,
    matchup: analysis.matchup || item.matchup || null,
    environment: analysis.environment || item.environment || null
  });

  setActiveProp(item.market.stat);
  lineInput.value = item.market.line;
  switchView('analyzer');

  // Hydrate the analyzer immediately from the already-fetched market scan data.
  // No network request needed — all the data is already in item.analysis.
  if (analysis && analysis.games && analysis.games.length) {
    // Build a full payload matching the shape renderSummary/renderChart expect
    const hydratedPayload = Object.assign({}, analysis, {
      player: Object.assign({}, analysis.player || {}, {
        id: item.player.id,
        full_name: item.player.full_name,
        team_id: item.player.team_id || null,
        position: item.player.position || '',
      }),
      stat: item.market.stat,
      line: item.market.line,
      availability: analysis.availability || item.availability || null,
      matchup: analysis.matchup || item.matchup || null,
      environment: analysis.environment || item.environment || null,
    });

    // Store full game list and slice to selected window
    const _allGames = (hydratedPayload.games || []).slice();
    const _n = parseInt(gamesSelect ? gamesSelect.value : '10', 10);
    const _sliced = _allGames.length > _n ? _allGames.slice(-_n) : _allGames;
    const _hits = _sliced.filter(g => g.hit).length;
    const _vals = _sliced.map(g => g.value);
    const _avg = _vals.length ? Math.round((_vals.reduce((a, b) => a + b, 0) / _vals.length) * 10) / 10 : 0;

    const displayPayload = Object.assign({}, hydratedPayload, {
      games: _sliced,
      games_count: _sliced.length,
      hit_count: _hits,
      hit_rate: _sliced.length ? Math.round((_hits / _sliced.length) * 1000) / 10 : 0,
      average: _avg,
      last_n: _n,
      _allGames: _allGames,
    });

    renderSelectedPlayer();
    renderSummary(displayPayload);
    renderChart(displayPayload);
    renderTable(displayPayload);
    setStatus('Ready');

    // Silently refresh in background to get latest data (next game, injury status, etc.)
    if (!options?.skipRefresh) {
      analyzePlayerProp({ preserveScroll: true, forceRefresh: true }).catch(err => {
        console.warn('Background refresh failed:', err.message || err);
      });
    }
    return;
  }

  // Fallback: no pre-fetched analysis, do a full fetch
  await analyzePlayerProp({ preserveScroll: true });
}

async function focusMarketPlayerEnhanced(item) {
  if (!item?.player?.id) return;
  const analysis = item.analysis || {};
  if (analysis && Array.isArray(analysis.games) && analysis.games.length) {
    await focusMarketPlayer(item, { skipRefresh: true });
    return;
  }
  const marketLastN = analysis?.last_n || currentMarketResultsPayload?.last_n || null;
  const hydrated = await hydrateAnalyzerFromPropSelection({
    player_id: item.player.id,
    player_name: item.player.full_name,
    team_id: item.player.team_id || null,
    team_name: item.player.team_name || '',
    team_abbreviation: item.player.team_abbreviation || item.player.team || '',
    player_position: item.player.position || '',
    player_jersey: item.player.jersey || '',
    opponent_team_id: analysis?.matchup?.next_game?.opponent_team_id || item?.matchup?.next_game?.opponent_team_id || null,
    opponent_abbreviation: analysis?.matchup?.next_game?.opponent_abbreviation || item.player.opponent || '',
    stat: item.market.stat,
    line: item.market.line,
    matchup: analysis.matchup || item.matchup || null,
    environment: analysis.environment || item.environment || null,
    availability: analysis.availability || item.availability || null,
    last_n: marketLastN,
    over_odds: item.market?.over_odds,
    under_odds: item.market?.under_odds,
  });
  if (!hydrated) {
    await focusMarketPlayer(item);
  }
}

async function enrichMarketResultsWithGameContext(payload) {
  if (!payload?.results?.length) return false;

  let changed = false;
  const targets = payload.results.slice(0, 24);
  await Promise.allSettled(targets.map(async (item) => {
    if (item?.analysis?.environment?.market_team_total !== undefined && item?.analysis?.environment?.market_spread !== undefined) {
      return;
    }
    const result = await fetchAnalyzerGameContext(item);
    if (!result?.environment) return;
    mergeMarketEnvironmentIntoItem(item, result);
    changed = true;
  }));
  return changed;
}

function getMarketItemKey(item) {
  if (!item) return '';
  const playerId = item?.player?.id ?? item?.player_id ?? '';
  const stat = item?.market?.stat ?? item?.stat ?? '';
  const line = item?.market?.line ?? item?.line ?? '';
  const side = item?.best_bet?.display_side ?? item?.best_bet?.side ?? item?.side ?? '';
  return [playerId, stat, line, side].join('|');
}

function buildDecisionLensData({
  selectedSide = 'Lean',
  marketSide = '',
  hitRate = null,
  average = null,
  line = null,
  confidenceSummary = '',
  modelProbability = null,
  impliedProbability = null,
  marketDisagrees = false,
  marketPenalty = null,
  teamContext = {},
  teamInjuryNames = [],
  environment = {},
}) {
  const toMaybeNumber = (value) => {
    if (value === null || value === undefined || value === '') return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  };
  const side = String(selectedSide || 'Lean').toUpperCase();
  const market = String(marketSide || '').toUpperCase();
  const hit = toMaybeNumber(hitRate);
  const avg = toMaybeNumber(average);
  const propLine = toMaybeNumber(line);
  const modelProb = toMaybeNumber(modelProbability);
  const impliedProb = toMaybeNumber(impliedProbability);
  const penalty = toMaybeNumber(marketPenalty);
  const impactCount = Number(teamContext?.impact_count || 0);
  const lineupNames = (teamInjuryNames || []).filter(Boolean);
  const teamTotal = toMaybeNumber(environment?.market_team_total);
  const gameTotal = toMaybeNumber(environment?.market_game_total);
  const spread = toMaybeNumber(environment?.market_spread);

  let modelText = confidenceSummary || 'Model signal unavailable.';
  if (Number.isFinite(hit) && Number.isFinite(avg) && Number.isFinite(propLine)) {
    modelText = `${side} is backed by ${hit.toFixed(1)}% recent sample hit rate, with ${avg.toFixed(1)} against a ${propLine.toFixed(1)} line.`;
  } else if (Number.isFinite(hit) && Number.isFinite(propLine)) {
    modelText = `${side} is backed by ${hit.toFixed(1)}% of the recent sample at the current ${propLine.toFixed(1)} line.`;
  }

  let marketText = 'Market context is unavailable for this read.';
  if (Number.isFinite(modelProb) || Number.isFinite(impliedProb) || market) {
    if (marketDisagrees && market) {
      marketText = `The betting market leans ${market}, while the model still prefers ${side}${Number.isFinite(penalty) && penalty > 0 ? `, so a ${penalty.toFixed(1)}-point market penalty is applied` : ''}.`;
    } else if (market) {
      marketText = `The betting market is aligned with ${side}${Number.isFinite(impliedProb) ? ` at ${impliedProb.toFixed(1)}% implied` : ''}. This is a strong confirmation signal that the model read is on the right side; if the market flips the other way, we treat it as a meaningful warning and reduce confidence.`;
    } else {
      marketText = `Model probability ${Number.isFinite(modelProb) ? `${modelProb.toFixed(1)}%` : '—'} vs implied ${Number.isFinite(impliedProb) ? `${impliedProb.toFixed(1)}%` : '—'}.`;
    }
  }
  const marketEnvironmentBits = [];
  if (Number.isFinite(teamTotal)) marketEnvironmentBits.push(`team total ${teamTotal.toFixed(1)}`);
  if (Number.isFinite(gameTotal)) marketEnvironmentBits.push(`game total ${gameTotal.toFixed(1)}`);
  if (Number.isFinite(spread)) marketEnvironmentBits.push(`spread ${spread > 0 ? '+' : ''}${spread.toFixed(1)}`);
  if (marketEnvironmentBits.length) {
    marketText += ` Market setup: ${marketEnvironmentBits.join(' • ')}.`;
  }

  let lineupText = 'No major same-team absences are materially changing this read.';
  if (impactCount > 0 || lineupNames.length) {
    const namesText = lineupNames.slice(0, 3).join(', ');
    lineupText = `${teamContext?.impact_summary || teamContext?.summary || `${impactCount || lineupNames.length} same-team absence${(impactCount || lineupNames.length) === 1 ? '' : 's'} are affecting the read.`}${namesText ? ` Key names: ${namesText}${lineupNames.length > 3 ? ' +' + (lineupNames.length - 3) : ''}.` : ''}`;
  }

  return { modelText, marketText, lineupText };
}

function renderDecisionLensHtml(lens, variant = '') {
  if (!lens) return '';
  return `
    <div class="decision-lens-grid ${variant}">
      <div class="decision-lens-card">
        <span class="insight-summary-label">Model case</span>
        <p>${escapeHtml(lens.modelText || 'Model context unavailable.')}</p>
      </div>
      <div class="decision-lens-card">
        <span class="insight-summary-label">Market case</span>
        <p>${escapeHtml(lens.marketText || 'Market context unavailable.')}</p>
      </div>
      <div class="decision-lens-card">
        <span class="insight-summary-label">Lineup case</span>
        <p>${escapeHtml(lens.lineupText || 'Lineup context unavailable.')}</p>
      </div>
    </div>`;
}

function getMarketInspectDetails(item) {
  const analysis = item?.analysis || {};
  const matchup = analysis.matchup || item?.matchup || {};
  const environment = analysis.environment || item?.environment || {};
  const availability = item?.availability || analysis?.availability || { status: 'Unknown', tone: 'neutral', reason: 'No report found', note: '' };
  const bestBet = item?.best_bet || {};
  const teamContext = analysis.team_context || item?.team_context || {};
  const expertAngles = getMarketExpertAngles(item).slice(0, 4);
  const marketSide = String(bestBet.display_side || bestBet.side || 'Lean').toUpperCase();
  const marketSupport = bestBet.model_probability !== undefined && bestBet.implied_probability !== undefined
    ? `${bestBet.model_probability}% model vs ${bestBet.implied_probability}% implied`
    : 'Model and market percentages unavailable';
  const lineupNames = (teamContext.players || []).map(p => `${formatPlayerName(p.name)} (${p.status})`).slice(0, 4);
  const decisionLens = buildDecisionLensData({
    selectedSide: marketSide,
    marketSide: bestBet.market_side,
    hitRate: bestBet.hit_rate ?? analysis?.hit_rate,
    average: bestBet.average ?? analysis?.average,
    line: item?.market?.line ?? item?.line,
    confidenceSummary: bestBet.user_read || bestBet.confidence_summary,
    modelProbability: bestBet.model_probability,
    impliedProbability: bestBet.implied_probability,
    marketDisagrees: bestBet.market_disagrees,
    marketPenalty: bestBet.market_penalty,
    teamContext,
    teamInjuryNames: (teamContext.players || []).map(p => p.name).filter(Boolean),
    environment,
  });
  const matchupSubtitle = item?.game_label || `${item?.player?.team || ''} vs ${item?.player?.opponent || ''}`.trim();
  return {
    title: `${item?.player?.full_name || 'Unknown player'} • ${item?.market?.stat || 'Prop'} ${item?.market?.line ?? ''}`,
    subtitle: matchupSubtitle.trim(),
    side: marketSide,
    summary: bestBet.user_read || bestBet.confidence_summary || 'Confidence summary unavailable.',
    availability,
    matchupLean: matchup?.vs_position?.lean || 'No matchup lean',
    matchupDetail: matchup?.next_game?.matchup_label || 'No next opponent',
    marketSupport,
    rankingScore: Number.isFinite(Number(bestBet.ranking_score)) ? Number(bestBet.ranking_score).toFixed(0) : '—',
    marketPenalty: Number.isFinite(Number(bestBet.market_penalty)) ? Number(bestBet.market_penalty).toFixed(1) : '0.0',
    teamTotal: Number.isFinite(Number(environment.market_team_total)) ? Number(environment.market_team_total).toFixed(1) : '—',
    spread: Number.isFinite(Number(environment.market_spread)) ? `${Number(environment.market_spread) > 0 ? '+' : ''}${Number(environment.market_spread).toFixed(1)}` : '—',
    gameTotal: Number.isFinite(Number(environment.market_game_total)) ? Number(environment.market_game_total).toFixed(1) : '—',
    expertAngles,
    lineupHeadline: teamContext.headline || 'Lineup context unavailable',
    lineupSummary: teamContext.summary || 'No same-team absences flagged in the current context.',
    lineupNames,
    decisionLens
  };
}

function closeMarketInspectTrayItem(key) {
  if (!key) return;
  currentMarketInspectKeys = currentMarketInspectKeys.filter(entry => entry !== key);
  if (currentMarketInspectKey === key) {
    currentMarketInspectKey = currentMarketInspectKeys[0] || '';
  }
  renderMarketInspectTray();
  syncMarketInspectState();
}

function openMarketInspectTrayItem(item) {
  if (!item) return;
  const key = getMarketItemKey(item);
  if (!key) return;
  currentMarketInspectKey = key;
  currentMarketInspectKeys = [key, ...currentMarketInspectKeys.filter(entry => entry !== key)].slice(0, 4);
  renderMarketInspectTray();
  syncMarketInspectState();
}

function syncMarketInspectState() {
  if (!marketResults) return;
  const openKeys = new Set(currentMarketInspectKeys);
  marketResults.querySelectorAll('[data-item-key]').forEach(node => {
    node.classList.toggle('is-inspected', openKeys.has(node.dataset.itemKey || ''));
  });
  marketResults.querySelectorAll('[data-action="inspect"][data-index]').forEach(btn => {
    const row = btn.closest('[data-item-key]');
    const key = row?.dataset?.itemKey || '';
    btn.textContent = openKeys.has(key) ? 'Opened' : 'Inspect';
  });
}

function renderMarketInspectTray() {
  if (!marketInspectTray) return;
  const source = currentMarketResultsPayload?.results || [];
  const items = currentMarketInspectKeys
    .map(key => source.find(entry => getMarketItemKey(entry) === key))
    .filter(Boolean);
  if (!items.length) {
    marketInspectTray.classList.add('hidden');
    marketInspectTray.innerHTML = '';
    return;
  }
  marketInspectTray.classList.remove('hidden');
  marketInspectTray.innerHTML = items.map(item => {
    const details = getMarketInspectDetails(item);
    const key = getMarketItemKey(item);
    const sideLower = String(details.side).toLowerCase();
    return `
      <aside class="market-inspect-chat-card" data-inspect-key="${escapeHtml(key)}">
        <div class="market-inspect-chat-head">
          <div>
            <span class="section-kicker">Scanner Inspect</span>
            <h3>${escapeHtml(details.title)}</h3>
            <p>${escapeHtml(details.subtitle)}</p>
          </div>
          <div class="market-inspect-chat-actions">
            <span class="finder-badge ${sideLower.includes('under') ? 'bad' : 'good'}">${escapeHtml(details.side)}</span>
            <button class="secondary-btn market-chat-open-btn" type="button" data-inspect-key="${escapeHtml(key)}">Open in Analyzer</button>
            <button class="ghost-btn market-chat-close-btn" type="button" data-inspect-key="${escapeHtml(key)}" aria-label="Close inspect panel">Close</button>
          </div>
        </div>
        <div class="market-inspect-chat-body">
          <div class="market-inspect-block">
            <span class="small-label">Confidence read</span>
            <strong>${escapeHtml(details.summary)}</strong>
            <small>${escapeHtml(details.marketSupport)}</small>
          </div>
          <div class="market-inspect-block">
            <span class="small-label">Scanner intelligence</span>
            <strong>Rank ${escapeHtml(details.rankingScore)}</strong>
            <small>${escapeHtml(Number(details.marketPenalty) > 0 ? `Market penalty ${details.marketPenalty}` : 'No market disagreement penalty')}</small>
          </div>
          <div class="market-inspect-block">
            <span class="small-label">Availability</span>
            <strong>${escapeHtml(details.availability.status || 'Unknown')}</strong>
            <small>${escapeHtml(details.availability.reason || details.availability.note || 'No official note')}</small>
          </div>
          <div class="market-inspect-block">
            <span class="small-label">Matchup</span>
            <strong>${escapeHtml(details.matchupLean)}</strong>
            <small>${escapeHtml(details.matchupDetail)}</small>
          </div>
          <div class="market-inspect-block">
            <span class="small-label">Market context</span>
            <strong>TT ${escapeHtml(details.teamTotal)} • Spr ${escapeHtml(details.spread)} • GT ${escapeHtml(details.gameTotal)}</strong>
            <small>Spread and totals loaded into scanner context</small>
          </div>
          ${renderDecisionLensHtml(details.decisionLens, 'compact')}
          <div class="market-inspect-lineup">
            <span class="small-label">Lineup context</span>
            <div class="market-inspect-lineup-card">
              <strong>${escapeHtml(details.lineupHeadline)}</strong>
              <small>${escapeHtml(details.lineupSummary)}</small>
              ${details.lineupNames.length ? `<p>${escapeHtml(details.lineupNames.join(' • '))}</p>` : ''}
            </div>
          </div>
          <div class="market-slip-angle-row">
            ${details.expertAngles.length ? details.expertAngles.map(angle => `<span class="market-angle-badge ${angle.tone ? `tone-${angle.tone}` : ''} ${angle.kind ? `kind-${angle.kind}` : ''}">${escapeHtml(angle.label)}</span>`).join('') : '<span class="market-angle-empty">No expert angle</span>'}
          </div>
        </div>
      </aside>
    `;
  }).join('');

  marketInspectTray.querySelectorAll('.market-chat-close-btn').forEach(btn => {
    btn.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      closeMarketInspectTrayItem(btn.dataset.inspectKey);
    });
  });

  marketInspectTray.querySelectorAll('.market-chat-open-btn').forEach(btn => {
    btn.addEventListener('click', async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const item = source.find(entry => getMarketItemKey(entry) === btn.dataset.inspectKey);
      if (!item) return;
      try {
        setStatus('Loading market pick');
        await focusMarketPlayerEnhanced(item);
        setStatus('Ready');
      } catch (error) {
        console.error(error);
        setStatus('Error');
      }
    });
  });
}

function applyMarketInjuryDataToCell(cell, data) {
  if (!cell) return;
  const injured = Array.isArray(data?.players) ? data.players : [];
  if (!injured.length) {
    cell.innerHTML = '<span style="opacity:0.35">Clean</span>';
    return;
  }
  const outPlayers = injured.filter(p => p.is_unavailable);
  const riskyPlayers = injured.filter(p => !p.is_unavailable);
  let html = '';
  if (outPlayers.length) {
    html += '<span style="color:rgba(255,100,80,0.85);font-weight:600">' + outPlayers.map(p => escapeHtml(p.lookup_name || p.display_name)).join(', ') + ' OUT</span><br>';
  }
  if (riskyPlayers.length) {
    html += '<span style="opacity:0.6">' + riskyPlayers.map(p => escapeHtml(p.lookup_name || p.display_name)).join(', ') + ' Q</span>';
  }
  cell.innerHTML = html || '<span style="opacity:0.35">Clean</span>';
}

function populateMarketInjuryCells(results) {
  const teamNameByIndex = new Map();
  results.forEach((item, index) => {
    const teamName = String(item?.player?.team_name || item?.player?.team || '').trim();
    if (!teamName) return;
    teamNameByIndex.set(index, teamName);
    const cached = marketTeamInjuryCache.get(teamName);
    if (cached) {
      const cell = document.getElementById('market-inj-' + index);
      applyMarketInjuryDataToCell(cell, cached);
    }
  });

  const uniqueTeams = Array.from(new Set(teamNameByIndex.values())).filter(teamName => !marketTeamInjuryCache.has(teamName));
  uniqueTeams.forEach(teamName => {
    fetch('/api/team-injuries?team_name=' + encodeURIComponent(teamName))
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (!data) return;
        marketTeamInjuryCache.set(teamName, data);
        teamNameByIndex.forEach((entryTeamName, index) => {
          if (entryTeamName !== teamName) return;
          const cell = document.getElementById('market-inj-' + index);
          applyMarketInjuryDataToCell(cell, data);
        });
      })
      .catch(() => {});
  });
}

function renderMarketResults(payload) {
  currentMarketResultsPayload = payload;
  const sortKey = currentMarketSort || 'best_ev';
  const filterKey = currentMarketFilter || 'all';
  const rawResults = payload.results || [];
  let results = [...filterMarketRows(rawResults, filterKey)]
    .filter(item => passesExpertFilters(item))
    .filter(item => passesAdvancedMarketFilters(item))
    .sort((a, b) => compareMarketRows(a, b, sortKey, currentMarketSortDirection));

  if (!results.length) {
    if (rawResults.length) {
      results = [...rawResults].sort((a, b) => compareMarketRows(a, b, sortKey, currentMarketSortDirection));
      showAppToast({
        title: 'Filters hid all results',
        detail: 'Showing all rows. Clear filters to refine.',
        tone: 'warning'
      });
    } else {
      renderMarketEmpty('No rows produced a usable result. Check names and try again.');
      return;
    }
  }

  if (marketSortSelect) marketSortSelect.value = sortKey;
  renderMarketFilterChips();
  renderMarketExpertFilterChips();
  marketMeta.textContent = `${results.length} props scanned • ${getMarketSortLabel(sortKey)} • ${getMarketFilterLabel(filterKey)} • ${getExpertFilterSummary()}`;

  const resultKeySet = new Set(results.map(getMarketItemKey));
  currentMarketInspectKeys = currentMarketInspectKeys.filter(key => resultKeySet.has(key));
  if (currentMarketInspectKey && !resultKeySet.has(currentMarketInspectKey)) {
    currentMarketInspectKey = currentMarketInspectKeys[0] || '';
  }
  renderMarketInspectTray();

  const featured = results.slice(0, 6);
  marketResults.className = 'market-results-shell market-balanced';
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
    const environment = item.analysis?.environment || {};
    const teamContext = item.analysis?.team_context || {};
    const matchupLabelFallback = item.game_label || (item.player?.team && item.player?.opponent ? `${item.player.team} vs ${item.player.opponent}` : '');
    const matchupLean = matchupData?.vs_position?.lean || (matchupLabelFallback ? matchupLabelFallback : 'No matchup');
    const matchupDetail = matchupData?.next_game?.matchup_label || (matchupLabelFallback ? 'Matchup pending' : 'No next opponent');
    const expertAngles = getMarketExpertAngles(item).slice(0, 3);
    const side = (item.best_bet.display_side || item.best_bet.side || 'Lean').toUpperCase();
    const sideClass = side.toLowerCase().includes('under') ? 'under' : 'over';
    const itemKey = getMarketItemKey(item);
    const isInspected = currentMarketInspectKeys.includes(itemKey);
    const availabilityNote = availability.reason || availability.note || 'No official note';
    const marketOddsAvailable = Number.isFinite(Number(item.market?.over_odds)) || Number.isFinite(Number(item.market?.under_odds));
    const trustDiagnostics = buildTrustDiagnostics({
      availability,
      environment,
      teamContext,
      marketSide: item.best_bet.market_side || '',
      selectedSide: item.best_bet.display_side || item.best_bet.side || '',
      injuryFilterNames: item.injury_filter_player_names || [],
      teamInjuryNames: item.team_injury_player_names || [],
      marketDisagrees: item.best_bet.market_disagrees,
      marketOddsAvailable,
    });
    return `
          <article class="market-slip-card market-balanced-card ${isInspected ? 'is-inspected' : ''}" data-index="${index}" data-item-key="${escapeHtml(itemKey)}">
            <div class="market-slip-head">
              <div class="market-slip-player">
                <img src="${getPlayerImage(item.player.id)}" alt="${escapeHtml(item.player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
                <div>
                  <strong>${escapeHtml(item.player.full_name)}</strong>
                  <span>${escapeHtml(item.game_label || (item.player.team ? `${item.player.team} vs ${item.player.opponent || ''}` : ''))}</span>
                  <small class="market-slip-statusline">${renderAvailabilityBadge(availability, true)} <span>${escapeHtml(availabilityNote)}</span></small>
                </div>
              </div>
              <div class="market-slip-rank">#${index + 1}</div>
            </div>
            <div class="market-slip-body">
              <div class="market-slip-line">
                <strong>${escapeHtml(item.market.stat)} ${item.market.line}</strong>
                <span class="market-slip-side ${sideClass}">${escapeHtml(side)}</span>
              </div>
              <div class="market-slip-meta-band">
                <span class="market-slip-confidence finder-badge ${tone}">${escapeHtml(item.best_bet.confidence || 'Neutral')} ${item.best_bet.confidence_score ? escapeHtml(item.best_bet.confidence_score) : ''}</span>
                <span class="market-slip-pill">
                  <span>Avg</span>
                  <strong>${item.analysis.average}</strong>
                </span>
              </div>
              <div class="market-micro-summary">
                <span class="market-micro-pill">EV <strong>${item.best_bet.ev ?? '—'}%</strong></span>
                <span class="market-micro-pill">Edge <strong>${item.best_bet.edge ?? '—'}%</strong></span>
                <span class="market-micro-pill">Sample <strong>${item.analysis.hit_count}/${item.analysis.games_count}</strong></span>
              </div>
              <p class="market-slip-summary">${escapeHtml(item.best_bet.user_read || item.best_bet.confidence_summary || 'Confidence summary unavailable.')}</p>
              ${renderTrustDiagnostics(trustDiagnostics, true)}
              <div class="market-slip-stats">
                <div class="market-slip-stat">
                  <span>Rank score</span>
                  <strong>${item.best_bet.ranking_score ?? item.best_bet.confidence_score ?? '—'}</strong>
                  <small>Scanner intelligence</small>
                </div>
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
              </div>
            </div>
            <div class="market-slip-footer">
              <div class="market-slip-angle-row">
                ${expertAngles.length ? expertAngles.map(angle => `<span class="market-angle-badge ${angle.tone ? `tone-${angle.tone}` : ''} ${angle.kind ? `kind-${angle.kind}` : ''}">${escapeHtml(angle.label)}</span>`).join('') : '<span class="market-angle-empty">No expert angle</span>'}
              </div>
              <div class="market-slip-action-row">
                <div class="market-matchup-cell">
                  <strong>${escapeHtml(matchupLean)}</strong>
                  <small>${escapeHtml(matchupDetail)}</small>
                </div>
                <div class="market-slip-quick-actions">
                  <button class="secondary-btn market-inline-btn" type="button" data-action="inspect" data-index="${index}">${isInspected ? 'Opened' : 'Inspect'}</button>
                </div>
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
            <th>🏥 Team injuries</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          ${results.map((item, index) => {
    const tone = getConfidenceTone(item.best_bet.confidence);
    const availability = item.availability || item.analysis?.availability || { status: 'Unknown', tone: 'neutral', reason: 'No report found', note: '' };
    const matchupData = item.analysis?.matchup || item.matchup || {};
    const environment = item.analysis?.environment || {};
    const teamContext = item.analysis?.team_context || {};
    const matchupLabelFallback = item.game_label || (item.player?.team && item.player?.opponent ? `${item.player.team} vs ${item.player.opponent}` : '');
    const matchupLean = matchupData?.vs_position?.lean || (matchupLabelFallback ? matchupLabelFallback : 'No matchup');
    const matchupDetail = matchupData?.next_game?.matchup_label || (matchupLabelFallback ? 'Matchup pending' : 'No next opponent');
    const expertAngles = getMarketExpertAngles(item);
    const marketOddsAvailable = Number.isFinite(Number(item.market?.over_odds)) || Number.isFinite(Number(item.market?.under_odds));
    const trustDiagnostics = buildTrustDiagnostics({
      availability,
      environment,
      teamContext,
      marketSide: item.best_bet.market_side || '',
      selectedSide: item.best_bet.display_side || item.best_bet.side || '',
      injuryFilterNames: item.injury_filter_player_names || [],
      teamInjuryNames: item.team_injury_player_names || [],
      marketDisagrees: item.best_bet.market_disagrees,
      marketOddsAvailable,
    });
    return `
              <tr class="market-row" data-index="${index}" data-item-key="${escapeHtml(getMarketItemKey(item))}">
                <td>
                  <div class="market-player-cell">
                    <img src="${getPlayerImage(item.player.id)}" alt="${escapeHtml(item.player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
                    <div>
                      <strong>${escapeHtml(item.player.full_name)}</strong>
                      <small>${escapeHtml(item.game_label || (item.player.team ? `${item.player.team} vs ${item.player.opponent || ''}` : ''))}</small>
                    </div>
                  </div>
                </td>
                <td><div class="market-availability-cell">${renderAvailabilityBadge(availability, true)}<small>${escapeHtml(availability.reason || availability.note || 'No official note')}</small></div></td>
                <td>${escapeHtml(item.market.stat)} ${item.market.line}</td>
                <td>
                  <div class="market-confidence-cell">
                    <span class="finder-badge ${tone}">${item.best_bet.display_side || item.best_bet.side} • ${item.best_bet.confidence}${item.best_bet.confidence_score ? ` ${item.best_bet.confidence_score}` : ''}</span>
                    <small>Rank score ${item.best_bet.ranking_score ?? item.best_bet.confidence_score ?? '—'}${Number(item.best_bet.market_penalty || 0) > 0 ? ` • market penalty ${Number(item.best_bet.market_penalty).toFixed(1)}` : ''}</small>
                    <small>${escapeHtml(item.best_bet.user_read || item.best_bet.confidence_summary || 'Confidence summary unavailable.')}</small>
                    ${renderTrustDiagnostics(trustDiagnostics, true)}
                  </div>
                  <div class="market-row-micro">
                    <span>EV <strong>${item.best_bet.ev ?? '—'}%</strong></span>
                    <span>Edge <strong>${item.best_bet.edge ?? '—'}%</strong></span>
                    <span>Sample <strong>${item.analysis.hit_count}/${item.analysis.games_count}</strong></span>
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
                <td><div class="parlay-inj-cell" id="market-inj-${index}"><span style="opacity:0.3">—</span></div></td>
                <td>
                  <div class="market-row-actions">
                    <button class="secondary-btn market-inline-btn" type="button" data-action="inspect" data-index="${index}">Inspect</button>
                  </div>
                </td>
              </tr>
            `;
  }).join('')}
        </tbody>
      </table>
    </div>
  `;

  marketResults.querySelectorAll('.market-inline-btn').forEach(btn => {
    btn.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      const item = results[Number(btn.dataset.index)];
      if (!item) return;
      if (btn.dataset.action === 'inspect') {
        openMarketInspectTrayItem(item);
      }
    });
  });

  marketResults.querySelectorAll('.market-row, .market-slip-card').forEach(row => {
    row.addEventListener('click', async () => {
      const item = results[Number(row.dataset.index)];
      if (!item) return;
      try {
        setStatus('Loading market pick');
        await focusMarketPlayerEnhanced(item);
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

  syncMarketInspectState();
  populateMarketInjuryCells(results);
}

async function streamNdjson(url, payload, onMessage) {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!response.ok || !response.body) {
    let errPayload = null;
    try { errPayload = await response.json(); } catch (e) { /* noop */ }
    throw new Error(errPayload?.detail || errPayload?.message || 'Request failed.');
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      let msg = null;
      try { msg = JSON.parse(trimmed); } catch (e) { continue; }
      if (onMessage) onMessage(msg);
      if (msg?.type === 'result' || msg?.type === 'error') return msg;
    }
  }
  return null;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runAsyncJob(endpoint, payload, onTick, timeoutMs = 180000, pollMs = 1000) {
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const start = Date.now();
  const initPayload = await response.json();
  if (!response.ok) {
    throw new Error(initPayload?.detail || initPayload?.message || 'Async job failed to start.');
  }
  const jobId = initPayload?.job_id;
  if (!jobId) throw new Error('Async job did not return a job id.');

  while (true) {
    const statusResp = await fetch(`/api/jobs/${encodeURIComponent(jobId)}`);
    const statusPayload = await statusResp.json();
    if (!statusResp.ok || !statusPayload?.ok) {
      throw new Error(statusPayload?.error || 'Async job status failed.');
    }
    if (onTick) onTick(statusPayload);
    if (statusPayload.status === 'done') return statusPayload.result;
    if (statusPayload.status === 'error') {
      throw new Error(statusPayload.error || 'Async job failed.');
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error('Async job timed out.');
    }
    await sleep(pollMs);
  }
}


async function runMarketScan(rowsOverride = null) {
  switchView('market');
  let rows;
  try {
    rows = Array.isArray(rowsOverride) ? rowsOverride : parseMarketText(marketTextarea.value);
  } catch (error) {
    alert(error.message);
    return;
  }

  setStatus('Scanning market');
  marketScanBtn.disabled = true;
  marketResults.className = 'bet-finder-state empty-state-panel compact';
  marketResults.innerHTML = `
    <div class="empty-icon">⏳</div>
    <div class="market-streaming-head">
      <strong>Scanning your board...</strong>
      <div class="streaming-badge market-streaming-badge"><span class="streaming-dot"></span>Live progress</div>
    </div>
    <span class="market-streaming-text">Working through the board…</span>
  `;

  try {
    const inputPayload = {
      rows,
      last_n: Number(gamesSelect.value),
      season: seasonInput.value.trim() || undefined
    };
    const progressLabel = (msg) => {
      switch (msg.stage) {
        case 'start': return `Scanning ${msg.total_rows || rows.length} rows...`;
        case 'prepared': return `Prepared ${msg.prepared || 0} rows`;
        case 'analysis_start': return `Analyzing ${msg.total || 0} props...`;
        case 'analysis_progress': return `Analyzing ${msg.done || 0}/${msg.total || 0}`;
        case 'analysis_done': return `Analysis complete (${msg.analyzed || 0})`;
        case 'scoring_progress': return `Scoring ${msg.done || 0}/${msg.total || 0}`;
        case 'done': return `Scan complete (${msg.results || 0} results)`;
        default: return 'Scanning market';
      }
    };
    const updateProgress = (msg) => {
      if (msg?.type !== 'progress') return;
      const label = progressLabel(msg);
      setStatus(label);
      const strong = marketResults.querySelector('strong');
      const span = marketResults.querySelector('.market-streaming-text');
      if (strong) strong.textContent = label;
      if (span) span.textContent = msg.stage === 'done'
        ? 'Rendering ranked results...'
        : 'Working through the board...';
    };
    let payload = null;
    try {
      const streamResult = await streamNdjson('/api/market-scan/stream', inputPayload, updateProgress);
      if (streamResult?.type === 'error') throw new Error(streamResult.message || 'Market scan failed.');
      payload = streamResult?.payload || null;
    } catch (streamError) {
      payload = await runAsyncJob('/api/market-scan/async', inputPayload, (status) => {
        if (!status || status.type !== 'market_scan') return;
        const label = status.status === 'running' ? 'Scanning market...' : 'Queued scan...';
        setStatus(label);
        const strong = marketResults.querySelector('strong');
        const span = marketResults.querySelector('.market-streaming-text');
        if (strong) strong.textContent = label;
        if (span) span.textContent = 'Working through the board...';
      });
    }
    renderMarketResults(payload);
    enrichMarketResultsWithGameContext(payload)
      .then(changed => { if (changed && currentMarketResultsPayload === payload) renderMarketResults(payload); })
      .catch(error => console.warn('Market context enrichment failed:', error));
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
  if (marketTone) {
    marketTone.className = 'spotlight-pill neutral';
    marketTone.textContent = 'Waiting for analysis';
  }
  if (cacheDebugPanel) {
    cacheDebugPanel.style.display = 'none';
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
  if (marketBody) {
    marketBody.className = 'environment-body';
    marketBody.innerHTML = `
      <div class="environment-chip-grid">
        <div class="environment-chip"><span class="small-label">Team total</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="environment-chip"><span class="small-label">Spread</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="environment-chip"><span class="small-label">Game total</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="environment-chip"><span class="small-label">Opponent total</span><strong>—</strong><small>Waiting for analysis</small></div>
      </div>
      <div class="insight-summary neutral compact-summary">
        <span class="insight-summary-label">Market read</span>
        <p class="opportunity-summary">Analyze a player prop to load team total, spread, and full-game total.</p>
      </div>
    `;
  }

  if (chart) {
    chart.destroy();
    chart = null;
  }
  lastPayload = null;
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

// Check Balance button — hits /api/odds/check-quota (costs 0 credits)
oddsCheckBalBtn?.addEventListener('click', async () => {
  let keyEntry = null;
  try {
    keyEntry = await pickRandomVaultKeyForFeature({
      requiredCredits: 0,
      sourceLabel: 'balance check',
    });
  } catch (error) {
    alert(error.message || 'No usable Odds API key found in Key Vault.');
    return;
  }
  const orig = oddsCheckBalBtn.textContent;
  oddsCheckBalBtn.textContent = 'Checking…';
  oddsCheckBalBtn.disabled = true;
  try {
    const data = await apiFetch('/api/odds/check-quota', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: keyEntry.key })
    }, 10000);
    setOddsQuotaMeta(data.quota);
    setOddsApiKeyMeta(data.api_key_masked || keyEntry.key);
    setOddsApiStatus('Balance checked', 'good');
  } catch (err) {
    alert('Could not check balance: ' + err.message);
    setOddsApiStatus('Check failed', 'bad');
  } finally {
    oddsCheckBalBtn.textContent = orig;
    oddsCheckBalBtn.disabled = false;
  }
});
[oddsSportSelect, oddsRegionsInput, oddsFormatSelect, oddsMarketsInput].forEach(el => {
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

// ── User Guide modal ──────────────────────────────────────────────────
(function () {
  const modal    = document.getElementById('userGuideModal');
  const openBtn  = document.getElementById('openUserGuideBtn');
  const closeTop = document.getElementById('closeUserGuideBtn');
  const closeBot = document.getElementById('closeUserGuideBtnBottom');
  function openGuide()  { if (modal) modal.style.display = 'block'; document.body.style.overflow = 'hidden'; }
  function closeGuide() { if (modal) modal.style.display = 'none';  document.body.style.overflow = ''; }
  openBtn?.addEventListener('click', openGuide);
  closeTop?.addEventListener('click', closeGuide);
  closeBot?.addEventListener('click', closeGuide);
  modal?.addEventListener('click', e => { if (e.target === modal) closeGuide(); });
  document.addEventListener('keydown', e => { if (e.key === 'Escape' && modal?.style.display === 'block') closeGuide(); });
})();

// ══════════════════════════════════════════════════════════════════════
// ── DATA VAULT / HISTORY PANEL ────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════

(function initHistoryPanel() {
  const historyType = document.getElementById('historyType');
  const historyLimit = document.getElementById('historyLimit');
  const historyRefreshBtn = document.getElementById('historyRefreshBtn');
  const historyList = document.getElementById('historyList');
  const historyStatus = document.getElementById('historyStatus');

  if (!historyList) return;

  const typeConfig = {
    injury: { endpoint: '/api/history/injury-reports', label: 'Injury report', empty: 'No injury reports stored yet.' },
    odds: { endpoint: '/api/history/odds-snapshots', label: 'Odds snapshot', empty: 'No odds snapshots stored yet.' },
    market_scan: { endpoint: '/api/history/market-scans', label: 'Market scan', empty: 'No market scans stored yet.' },
    parlay: { endpoint: '/api/history/parlay-runs', label: 'Parlay run', empty: 'No parlay runs stored yet.' },
    backtest: { endpoint: '/api/history/backtest-entries', label: 'Backtest entry', empty: 'No backtest entries stored yet.' },
    player_info: { endpoint: '/api/history/player-info', label: 'Player info', empty: 'No player info cached yet.' },
  };

  function setHistoryStatus(text, tone = 'neutral') {
    if (!historyStatus) return;
    historyStatus.textContent = text;
    historyStatus.dataset.tone = tone;
  }

  function renderEmpty(message) {
    historyList.innerHTML = `
      <div class="bet-finder-state empty-state-panel compact history-empty">
        <div class="empty-icon">🗂️</div>
        <strong>${escapeHtml(message)}</strong>
        <span>Run a scan or analysis to populate history.</span>
      </div>`;
  }

  function formatDate(value) {
    if (!value) return '—';
    const parsed = Date.parse(value);
    if (!Number.isFinite(parsed)) return String(value);
    return new Date(parsed).toLocaleString();
  }

  function renderEntries(typeKey, entries) {
    if (!entries.length) {
      renderEmpty(typeConfig[typeKey].empty);
      return;
    }
    historyList.innerHTML = entries.map(entry => {
      if (typeKey === 'injury') {
        return `
          <div class="history-row">
            <div>
              <strong>${escapeHtml(entry.report_label || 'Injury Report')}</strong>
              <small>${escapeHtml(entry.report_url || '')}</small>
            </div>
            <div class="history-meta">
              ${escapeHtml(formatDate(entry.report_timestamp))}
              <small>${escapeHtml(entry.rows_count)} players</small>
              <button class="text-btn history-view-btn" data-type="injury" data-id="${escapeHtml(entry.report_url || '')}" type="button">View full payload</button>
            </div>
          </div>`;
      }
      if (typeKey === 'odds') {
        return `
          <div class="history-row">
            <div>
              <strong>${escapeHtml(entry.endpoint || 'Odds API')}</strong>
              <small>${escapeHtml(JSON.stringify(entry.params || {}))}</small>
            </div>
            <div class="history-meta">
              ${escapeHtml(formatDate(entry.fetched_at))}
              <small>#${escapeHtml(entry.id)}</small>
              <button class="text-btn history-view-btn" data-type="odds" data-id="${escapeHtml(entry.id)}" type="button">View full payload</button>
            </div>
          </div>`;
      }
      if (typeKey === 'market_scan') {
        return `
          <div class="history-row">
            <div>
              <strong>Market scan</strong>
              <small>${escapeHtml(entry.count)} results • ${escapeHtml(entry.errors)} errors</small>
            </div>
            <div class="history-meta">
              ${escapeHtml(formatDate(entry.requested_at))}
              <small>#${escapeHtml(entry.id)}</small>
              <button class="text-btn history-view-btn" data-type="market_scan" data-id="${escapeHtml(entry.id)}" type="button">View full payload</button>
            </div>
          </div>`;
      }
      if (typeKey === 'parlay') {
        return `
          <div class="history-row">
            <div>
              <strong>Parlay run</strong>
              <small>${escapeHtml(entry.legs ?? '—')} legs • ${escapeHtml(entry.props_found ?? '—')} props</small>
            </div>
            <div class="history-meta">
              ${escapeHtml(formatDate(entry.requested_at))}
              <small>#${escapeHtml(entry.id)}</small>
              <button class="text-btn history-view-btn" data-type="parlay" data-id="${escapeHtml(entry.id)}" type="button">View full payload</button>
            </div>
          </div>`;
      }
      if (typeKey === 'player_info') {
        return `
          <div class="history-row">
            <div>
              <strong>${escapeHtml(entry.player_name || 'Player')}</strong>
              <small>${escapeHtml(entry.team_abbreviation || '—')}</small>
            </div>
            <div class="history-meta">
              ${escapeHtml(formatDate(entry.updated_at))}
              <small>#${escapeHtml(entry.player_id)}</small>
              <button class="text-btn history-view-btn" data-type="player_info" data-id="${escapeHtml(entry.player_id)}" type="button">View full payload</button>
            </div>
          </div>`;
      }
      return `
        <div class="history-row">
          <div>
            <strong>${escapeHtml(entry.player || 'Backtest')}</strong>
            <small>${escapeHtml(entry.stat || '')} ${escapeHtml(entry.side || '')} ${escapeHtml(entry.line ?? '')}</small>
          </div>
          <div class="history-meta">
            ${escapeHtml(formatDate(entry.updated_at))}
            <small>${escapeHtml(entry.result || '')}</small>
            <button class="text-btn history-view-btn" data-type="backtest" data-id="${escapeHtml(entry.id)}" type="button">View full payload</button>
          </div>
        </div>`;
    }).join('');
    historyList.querySelectorAll('.history-view-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        const id = btn.dataset.id;
        const type = btn.dataset.type;
        if (!id || !type) return;
        let endpoint = '';
        if (type === 'injury') endpoint = `/api/history/injury-reports/${encodeURIComponent(id)}`;
        if (type === 'odds') endpoint = `/api/history/odds-snapshots/${encodeURIComponent(id)}`;
        if (type === 'market_scan') endpoint = `/api/history/market-scans/${encodeURIComponent(id)}`;
        if (type === 'parlay') endpoint = `/api/history/parlay-runs/${encodeURIComponent(id)}`;
        if (type === 'backtest') endpoint = `/api/history/backtest-entries/${encodeURIComponent(id)}`;
        if (type === 'player_info') endpoint = `/api/history/player-info/${encodeURIComponent(id)}`;
        if (!endpoint) return;
        btn.textContent = 'Loading…';
        btn.disabled = true;
        try {
          const payload = await apiFetch(endpoint, {}, 10000);
          if (!payload || payload.ok === false) {
            showAppToast({ title: 'History payload unavailable', detail: payload?.error || 'Request failed', tone: 'warning' });
            return;
          }
          const pretty = JSON.stringify(payload.payload || {}, null, 2);
          showPayloadModal(pretty);
        } catch (err) {
          showAppToast({ title: 'History payload failed', detail: err?.message || 'Request failed', tone: 'warning' });
        } finally {
          btn.textContent = 'View full payload';
          btn.disabled = false;
        }
      });
    });
  }

  function showPayloadModal(content) {
    let modal = document.getElementById('historyPayloadModal');
    if (!modal) {
      modal = document.createElement('div');
      modal.id = 'historyPayloadModal';
      modal.className = 'history-modal';
      modal.innerHTML = `
        <div class="history-modal-backdrop" data-close="1"></div>
        <div class="history-modal-card">
          <div class="history-modal-head">
            <strong>Stored payload</strong>
            <button class="ghost-btn" data-close="1" type="button">Close</button>
          </div>
          <pre class="history-modal-body"></pre>
        </div>`;
      document.body.appendChild(modal);
      modal.querySelectorAll('[data-close="1"]').forEach(btn => {
        btn.addEventListener('click', () => modal.classList.remove('is-open'));
      });
    }
    const pre = modal.querySelector('.history-modal-body');
    if (pre) pre.textContent = content || '';
    modal.classList.add('is-open');
  }

  async function loadHistory() {
    const typeKey = historyType?.value || 'injury';
    const limit = Math.max(5, Math.min(200, Number(historyLimit?.value || 25)));
    const config = typeConfig[typeKey] || typeConfig.injury;
    setHistoryStatus('Loading…', 'working');
    try {
      const payload = await apiFetch(`${config.endpoint}?limit=${limit}`, {}, 12000).catch(() => null);
      if (!payload || payload.ok === false) {
        setHistoryStatus(payload?.error || 'History unavailable', 'bad');
        renderEmpty(payload?.error || 'History unavailable');
        return;
      }
      setHistoryStatus('Ready', 'good');
      renderEntries(typeKey, payload.entries || []);
    } catch (err) {
      setHistoryStatus('History load failed', 'bad');
      renderEmpty('History load failed');
    }
  }

  historyRefreshBtn?.addEventListener('click', loadHistory);
  historyType?.addEventListener('change', loadHistory);
  historyLimit?.addEventListener('change', loadHistory);

  loadHistory();
})();

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
const stickySignalLabelEl = document.getElementById('stickySignalLabel');
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
let favoritesUpgradeCache = [];
let favoritesUpgradeLoadPromise = null;
let upgradedChartPrefs = (() => {
  try {
    const parsed = JSON.parse(localStorage.getItem(CHART_PREFS_KEY) || '{}');
    return { mode: parsed.mode === 'combo' ? 'combo' : 'bars', highlight: parsed.highlight !== false };
  } catch {
    return { mode: 'bars', highlight: true };
  }
})();

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

function getTeamLogo(teamId) {
  return teamId ? `https://cdn.nba.com/logos/nba/${teamId}/global/L/logo.svg` : '';
}

function loadFavoritesUpgrade() {
  return Array.isArray(favoritesUpgradeCache) ? favoritesUpgradeCache.slice() : [];
}

async function ensureFavoritesUpgradeLoaded() {
  if (favoritesUpgradeLoadPromise) return favoritesUpgradeLoadPromise;
  favoritesUpgradeLoadPromise = (async () => {
    try {
      const data = await apiFetch('/api/favorites', {}, 8000);
      favoritesUpgradeCache = Array.isArray(data.entries) ? data.entries : [];
      if (!favoritesUpgradeCache.length) {
        try {
          const legacy = JSON.parse(localStorage.getItem(FAVORITES_KEY) || '[]');
          if (Array.isArray(legacy) && legacy.length) {
            favoritesUpgradeCache = legacy.slice(0, 12);
            await saveFavoritesUpgrade(favoritesUpgradeCache, { skipLegacyWrite: true });
          }
        } catch {}
      }
    } catch (error) {
      console.warn('Favorites load failed, using local fallback:', error);
      try {
        favoritesUpgradeCache = JSON.parse(localStorage.getItem(FAVORITES_KEY) || '[]');
      } catch {
        favoritesUpgradeCache = [];
      }
    }
    return favoritesUpgradeCache;
  })();
  return favoritesUpgradeLoadPromise;
}

async function saveFavoritesUpgrade(items, options = {}) {
  const trimmed = Array.isArray(items) ? items.slice(0, 12) : [];
  favoritesUpgradeCache = trimmed.slice();
  if (!options.skipLegacyWrite) {
    localStorage.setItem(FAVORITES_KEY, JSON.stringify(trimmed));
  }
  await apiFetch('/api/favorites', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ entries: trimmed }),
  }, 8000);
}

function getAnalyzerHeroState() {
  if (!selectedPlayer) return null;
  const payload = lastPayload && String(lastPayload?.player?.id || '') === String(selectedPlayer?.id || '') ? lastPayload : null;
  const contextData = getSelectedPlayerContextData();
  const environment = payload?.environment || contextData.environment || {};
  const matchup = payload?.matchup || contextData.matchup || {};
  const teamContext = payload?.team_context || {};
  const recommendation = payload?.traffic_light || {};
  const confidence = payload?.confidence || {};
  const chosenSide = String(payload?.recommended_side || '').toUpperCase();
  const nextGame = matchup?.next_game || {};
  const teamTotal = Number.isFinite(Number(environment.market_team_total)) ? Number(environment.market_team_total).toFixed(1) : '';
  const spread = Number.isFinite(Number(environment.market_spread))
    ? `${Number(environment.market_spread) > 0 ? '+' : ''}${Number(environment.market_spread).toFixed(1)}`
    : '';
  const lineupCount = Number(teamContext.impact_count || 0);
  const signalText = payload
    ? `${recommendation.label || chosenSide || 'Lean'}${confidence.grade ? ` • ${confidence.grade} ${confidence.score || ''}` : ''}${nextGame.matchup_label ? ` • ${nextGame.matchup_label}` : ''}`
    : 'Awaiting analysis';
  const chips = payload ? [
    chosenSide ? `${chosenSide} lean` : '',
    confidence.grade ? `${confidence.grade} ${confidence.score || ''}` : '',
    nextGame.matchup_label ? `Next ${nextGame.matchup_label}` : '',
    teamTotal ? `TT ${teamTotal}` : '',
    spread ? `Spr ${spread}` : '',
    lineupCount ? `${lineupCount} lineup flag${lineupCount === 1 ? '' : 's'}` : ''
  ].filter(Boolean) : [];
  return {
    payload,
    signalText,
    chips,
    summary: payload?.interpretation?.summary || payload?.confidence?.summary || '',
    availability: contextData.availability || selectedPlayer.availability || null,
  };
}

function updateStickyAnalyzerSummary() {
  if (stickyPlayerNameEl) stickyPlayerNameEl.textContent = selectedPlayer?.full_name || 'No player selected';
  if (stickyPropLabelEl) stickyPropLabelEl.textContent = `${selectedStat} ${lineInput?.value || '—'}`;
  if (stickySignalLabelEl) stickySignalLabelEl.textContent = getAnalyzerHeroState()?.signalText || 'Awaiting analysis';
  if (favoritePlayerBtnEl) {
    const favorites = loadFavoritesUpgrade();
    const isFavorite = !!selectedPlayer && favorites.some(item => item.type === 'player' && item.key === String(selectedPlayer.id));
    favoritePlayerBtnEl.classList.toggle('active', isFavorite);
  }
}

async function toggleFavoriteUpgrade(item) {
  const favorites = loadFavoritesUpgrade();
  const key = `${item.type}:${item.key}`;
  const existingIndex = favorites.findIndex(entry => `${entry.type}:${entry.key}` === key);
  if (existingIndex >= 0) favorites.splice(existingIndex, 1);
  else favorites.unshift(item);
  try {
    await saveFavoritesUpgrade(favorites);
    renderFavoritesUpgrade();
    updateStickyAnalyzerSummary();
    return true;
  } catch (error) {
    console.error('Favorite toggle failed:', error);
    return false;
  }
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
  if (chartHighlightBtnEl) chartHighlightBtnEl.textContent = upgradedChartPrefs.highlight ? 'Hit/Miss colors' : 'Hit/Miss colors';
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
        <div class="overview-best-bet-main">
          <div class="overview-best-bet-player">
            <img class="overview-best-bet-avatar" src="${getPlayerImage(item.player.id)}" alt="${escapeHtml(item.player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
            <div class="overview-best-bet-copy">
              <strong>${escapeHtml(item.player.full_name)}</strong>
              <small>${escapeHtml(item.market.stat)} ${item.market.line} • ${escapeHtml(item.player.team || '')}${item.player.opponent ? ` vs ${escapeHtml(item.player.opponent)}` : ''}</small>
              <div class="overview-best-bet-detail-row">
                <span class="overview-best-bet-chip stat">${escapeHtml(item.market.stat)} ${item.market.line}</span>
                <span class="overview-best-bet-chip">Edge ${item.best_bet.edge ?? '—'}%</span>
                <span class="overview-best-bet-chip">EV ${item.best_bet.ev ?? '—'}%</span>
                <span class="overview-best-bet-chip">Hit ${item.best_bet.hit_rate ?? item.analysis?.hit_rate ?? '—'}%</span>
              </div>
            </div>
          </div>
          <div class="overview-best-bet-grade">
            <span class="overview-grade-label">Board read</span>
            <strong>${escapeHtml(item.best_bet.display_side || item.best_bet.side || 'Lean')}</strong>
            <small>${escapeHtml(item.best_bet.traffic_light?.label || item.best_bet.confidence || 'Playable')}</small>
          </div>
        </div>
        <p class="overview-best-bet-summary">${escapeHtml(item.best_bet.explanation || item.best_bet.user_read || 'Latest board note.')}</p>
      </button>
    `).join('');
    node.querySelectorAll('.overview-best-bet-card').forEach((card, idx) => card.addEventListener('click', async () => focusMarketPlayerEnhanced(items[idx])));
  };

  const strongest = results.slice(0, 5);
  const caution = results.filter(item => ['yellow', 'red'].includes(item.best_bet.traffic_light?.tone || '') || item.analysis?.availability?.is_risky || item.availability?.is_risky || (item.analysis?.matchup?.vs_position?.lean_tone === 'bad')).slice(0, 3);
  const boosts = results.filter(item => Number(item.analysis?.team_context?.impact_count || item.team_context?.impact_count || 0) > 0 || /context|expanded|thin|rise/i.test(String(item.analysis?.team_context?.headline || item.team_context?.headline || ''))).slice(0, 3);
  if (overviewTopCountEl) overviewTopCountEl.textContent = String(strongest.length);
  if (overviewCautionCountEl) overviewCautionCountEl.textContent = String(caution.length);
  if (overviewBoostCountEl) overviewBoostCountEl.textContent = String(boosts.length);
  renderBoard(overviewBestBets, strongest, '⭐', 'No best bets saved yet.', 'Run Market Scanner to pin the strongest current board edges here.');
  renderBoard(overviewCautionBoardEl, caution, '⚠️', 'No caution spots yet.', 'Risky plays will appear here after a board scan.');
  renderBoard(overviewBoostBoardEl, boosts, '📈', 'No lineup-context edges yet.', 'Plays with strong teammate-absence context will appear here.');
  setOverviewBoardTab(overviewBoardTab);
}

overviewBoardTabsEl?.querySelectorAll('.overview-board-tab').forEach(btn => {
  btn.addEventListener('click', () => {
    setOverviewBoardTab(btn.dataset.boardTab || 'top');
  });
});

// buildTodayGameCard defined above (with injury panel)

function renderTodayGames(payload) {
  latestTodayGamesPayload = payload;
  try {
    localStorage.setItem(TODAY_GAMES_CACHE_KEY, JSON.stringify({
      savedAt: Date.now(),
      payload
    }));
  } catch (error) {
    // ignore storage failures
  }
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

function renderDecisionStrip(payload) {
  if (!analyzerDecisionStripGrid) return;
  const isDebug = localStorage.getItem(INJURY_DEBUG_STORAGE) === '1';
  const cacheDebugOn = localStorage.getItem(CACHE_DEBUG_STORAGE) === '1';
  const confidence = payload?.confidence || {};
  const recommendation = payload?.traffic_light || {};
  const teamContext = payload?.team_context || {};
  const environment = payload?.environment || {};
  const matchup = payload?.matchup || {};
  const vsPosition = matchup?.vs_position || {};
  const hitRate = Number(payload?.hit_rate || 0);
  const avg = Number(payload?.average || 0);
  const line = Number(payload?.line || 0);
  const diff = Number.isFinite(avg) && Number.isFinite(line) ? avg - line : null;
  const diffLabel = diff === null ? '—' : `${diff >= 0 ? '+' : ''}${diff.toFixed(1)}`;
  const gamesCount = Number(payload?.games_count || 0);
  const sampleLabel = gamesCount ? `${gamesCount}g sample` : 'Sample pending';
  const side = String(payload?.recommended_side || '').toUpperCase() || 'LEAN';
  const trafficTone = recommendation.tone === 'green' ? 'good' : recommendation.tone === 'red' ? 'bad' : 'warning';
  const confidenceText = confidence.grade ? `${confidence.grade} ${confidence.score || ''}`.trim() : '—';
  const matchupLean = vsPosition?.lean || 'Matchup TBD';
  const matchupTone = vsPosition?.lean_tone === 'good' ? 'good' : vsPosition?.lean_tone === 'bad' ? 'bad' : 'neutral';
  const injuryCount = Number(teamContext.impact_count || 0);
  const injuryText = injuryCount ? `${injuryCount} flagged` : 'No absences';
  const debugPayload = teamContext?.debug || {};
  const resolvedTeam = debugPayload.resolved_team_name || teamContext.team_name || '';
  const matchedRows = Array.isArray(debugPayload.matched_injury_rows) ? debugPayload.matched_injury_rows : [];
  const debugLabel = isDebug
    ? `Team ${resolvedTeam || '—'} · rows ${matchedRows.length}`
    : 'Off';
  const debugMeta = isDebug
    ? (matchedRows.length ? matchedRows.map(row => `${row.name} (${row.status})`).slice(0, 3).join(', ') : 'No matched rows')
    : 'Enable to verify team match';
  const impliedPct = Number.isFinite(Number(confidence.market_support_pct)) ? `${Number(confidence.market_support_pct).toFixed(1)}%` : '—';
  const marketLabel = confidence.market_side
    ? `${confidence.market_side} ${confidence.market_disagrees ? 'disagree' : 'aligned'}`
    : 'No market';
  const marketTone = confidence.market_disagrees ? 'warning' : (confidence.market_side ? 'good' : 'neutral');
  const restLabel = Number.isInteger(environment.rest_days) ? `${environment.rest_days}d rest` : 'Rest TBD';

  analyzerDecisionStripGrid.innerHTML = `
    <div class="decision-strip-chip ${trafficTone}">
      <span class="small-label">Recommendation</span>
      <strong>${escapeHtml(recommendation.label || side || 'Lean')}</strong>
      <small>${escapeHtml(recommendation.summary || 'Awaiting analysis')}</small>
    </div>
    <div class="decision-strip-chip ${trafficTone}">
      <span class="small-label">Signal</span>
      <strong>${escapeHtml(side)}</strong>
      <small>${escapeHtml(confidenceText || '—')}</small>
    </div>
    <div class="decision-strip-chip">
      <span class="small-label">Hit rate</span>
      <strong>${hitRate ? `${hitRate.toFixed(1)}%` : '—'}</strong>
      <small>${escapeHtml(sampleLabel)}</small>
    </div>
    <div class="decision-strip-chip ${diff !== null && diff >= 0 ? 'good' : 'warning'}">
      <span class="small-label">Avg vs line</span>
      <strong>${escapeHtml(diffLabel)}</strong>
      <small>${escapeHtml(`Avg ${avg ? avg.toFixed(1) : '—'} vs ${line ? line.toFixed(1) : '—'}`)}</small>
    </div>
    <div class="decision-strip-chip ${matchupTone}">
      <span class="small-label">Matchup</span>
      <strong>${escapeHtml(matchupLean)}</strong>
      <small>${escapeHtml(restLabel)}</small>
    </div>
    <div class="decision-strip-chip ${injuryCount ? 'warning' : 'neutral'}">
      <span class="small-label">Lineup</span>
      <strong>${escapeHtml(injuryText)}</strong>
      <small>${escapeHtml(teamContext.headline || 'Team context')}</small>
    </div>
    <div class="decision-strip-chip ${isDebug ? 'warning' : 'neutral'}" id="injuryDebugChip">
      <span class="small-label">Injury debug</span>
      <strong>${escapeHtml(debugLabel)}</strong>
      <small>${escapeHtml(debugMeta)}</small>
    </div>
    <div class="decision-strip-chip ${marketTone}">
      <span class="small-label">Market</span>
      <strong>${escapeHtml(impliedPct)}</strong>
      <small>${escapeHtml(marketLabel)}</small>
    </div>
    <div class="decision-strip-chip ${cacheDebugOn ? 'warning' : 'neutral'}" id="cacheDebugChip">
      <span class="small-label">Cache debug</span>
      <strong>${cacheDebugOn ? 'On' : 'Off'}</strong>
      <small>Show cache sources</small>
    </div>
  `;
  if (decisionStripToneEl) {
    decisionStripToneEl.className = `spotlight-pill ${trafficTone}`;
    decisionStripToneEl.textContent = recommendation.label || 'Decision strip';
  }
  const debugChip = analyzerDecisionStripGrid.querySelector('#injuryDebugChip');
  if (debugChip) {
    debugChip.addEventListener('click', () => {
      const next = localStorage.getItem(INJURY_DEBUG_STORAGE) === '1' ? '0' : '1';
      localStorage.setItem(INJURY_DEBUG_STORAGE, next);
      if (lastPayload) {
        renderDecisionStrip(lastPayload);
      }
    });
  }
  const cacheChip = analyzerDecisionStripGrid.querySelector('#cacheDebugChip');
  if (cacheChip) {
    cacheChip.addEventListener('click', () => {
      const next = localStorage.getItem(CACHE_DEBUG_STORAGE) === '1' ? '0' : '1';
      localStorage.setItem(CACHE_DEBUG_STORAGE, next);
      if (lastPayload) {
        renderDecisionStrip(lastPayload);
      }
    });
  }
  renderCacheDebugPanel(payload);
}

function renderCacheDebugPanel(payload) {
  if (!cacheDebugPanel) return;
  const enabled = localStorage.getItem(CACHE_DEBUG_STORAGE) === '1';
  if (!enabled) {
    cacheDebugPanel.style.display = 'none';
    return;
  }
  const freshness = payload?.freshness || {};
  const entries = [
    {
      label: 'Game log',
      source: freshness.game_log_cache_source || freshness.game_log_source || 'unknown',
      age: freshness.game_log_seconds_ago,
      queued: freshness.game_log_refresh_queued,
    },
    {
      label: 'Player info',
      source: freshness.player_info_cache_source || freshness.player_info_source || 'unknown',
      age: freshness.player_info_seconds_ago,
      queued: freshness.player_info_refresh_queued,
    },
    {
      label: 'Next game',
      source: freshness.next_game_cache_source || freshness.next_game_source || 'unknown',
      age: freshness.next_game_seconds_ago,
      queued: freshness.next_game_refresh_queued,
    },
    {
      label: 'Injury report',
      source: freshness.injury_report_source || 'unknown',
      age: freshness.injury_report_seconds_ago,
      queued: false,
    },
  ];
  const grid = cacheDebugPanel.querySelector('.cache-debug-grid');
  const pill = cacheDebugPanel.querySelector('.cache-debug-pill');
  if (pill) {
    pill.textContent = freshness?.injury_report_source === 'postgres' ? 'Postgres source' : 'Hybrid source';
  }
  if (grid) {
    grid.innerHTML = entries.map(item => {
      const ageText = typeof item.age === 'number' ? `${item.age.toFixed(1)}s ago` : '—';
      const queuedText = item.queued ? 'refresh queued' : 'ready';
      return `
        <div class="cache-debug-card">
          <strong>${escapeHtml(item.label)}</strong>
          <small>Source: ${escapeHtml(String(item.source || 'unknown'))}</small><br/>
          <small>Age: ${escapeHtml(ageText)} • ${escapeHtml(queuedText)}</small>
        </div>
      `;
    }).join('');
  }
  cacheDebugPanel.style.display = '';
}

function renderInterpretationPanels(payload) {
  const interpretation = payload?.interpretation || {};
  const opportunity = payload?.opportunity || {};
  const teamContext = payload?.team_context || {};
  const environment = payload?.environment || {};
  const recommendation = payload?.traffic_light || { label: 'Caution', tone: 'yellow', summary: 'Analyze a prop to generate a clearer read.' };
  const confidence = payload?.confidence || {};
  renderDecisionStrip(payload);
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
    const sampleWarning = payload?.sample_warning || null;
    const variance = payload?.variance || {};
    const chosenSide = payload?.recommended_side || 'LEAN';
    const consistencyScore = Number(variance.consistency_score ?? -1);
    const consistencyHtml = consistencyScore >= 0
      ? `<span style="font-size:11px;opacity:0.65;margin-left:8px">Consistency ${consistencyScore.toFixed(0)}/100</span>`
      : '';
    const warningHtml = sampleWarning
      ? `<div style="margin-top:10px;padding:8px 12px;border-radius:10px;background:rgba(245,158,11,0.12);border:1px solid rgba(245,158,11,0.25);font-size:12px;color:var(--warning,#f59e0b)">⚠ ${escapeHtml(sampleWarning)}</div>`
      : '';
    recommendationBodyEl.className = 'interpretation-body';
    recommendationBodyEl.innerHTML = `
      <div class="insight-summary ${trafficToneClass}">
        <span class="insight-summary-label">Traffic light</span>
        <strong>${escapeHtml(recommendation.label || 'Caution')}</strong>
        <p>${escapeHtml(recommendation.summary || 'Analyze a player prop to generate a clearer read.')}</p>
      </div>
      <div class="traffic-light-row">
        <span class="traffic-pill ${trafficToneClass}">${escapeHtml(chosenSide)}</span>
        <span class="traffic-meta">${escapeHtml(confidence.grade || '—')} ${escapeHtml(String(confidence.score || ''))} • ${escapeHtml(confidence.summary || '')}</span>
      </div>
      <div style="display:flex;align-items:center;gap:6px;margin-top:8px;flex-wrap:wrap">
        ${consistencyScore >= 0 ? `<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:rgba(255,255,255,0.07);opacity:0.8">Consistency <strong>${consistencyScore.toFixed(0)}/100</strong></span>` : ''}
        ${variance.std_dev !== undefined ? `<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:rgba(255,255,255,0.07);opacity:0.7">σ <strong>${Number(variance.std_dev).toFixed(1)}</strong></span>` : ''}
        ${variance.median !== undefined ? `<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:rgba(255,255,255,0.07);opacity:0.7">Median <strong>${Number(variance.median).toFixed(1)}</strong></span>` : ''}
      </div>
      ${warningHtml}`;
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
  if (marketTone) {
    const hasMarketContext = Number.isFinite(Number(environment.market_team_total)) || Number.isFinite(Number(environment.market_game_total)) || Number.isFinite(Number(environment.market_spread));
    marketTone.className = `spotlight-pill ${hasMarketContext ? 'good' : 'neutral'}`;
    marketTone.textContent = hasMarketContext ? 'Market loaded' : 'Market unavailable';
  }
  if (interpretationBody) {
    const bullets = Array.isArray(interpretation.bullets) ? interpretation.bullets : [];
    const decisionLens = buildDecisionLensData({
      selectedSide: payload?.recommended_side,
      marketSide: confidence.market_side,
      hitRate: payload?.hit_rate,
      average: payload?.average,
      line: payload?.line,
      confidenceSummary: confidence.summary || interpretation.summary,
      modelProbability: confidence.model_probability,
      impliedProbability: confidence.implied_probability,
      marketDisagrees: confidence.market_disagrees,
      marketPenalty: confidence.market_penalty,
      teamContext,
      teamInjuryNames: (teamContext.players || []).map(item => item.name).filter(Boolean),
      environment,
    });
    const trustDiagnostics = buildTrustDiagnostics({
      availability: payload?.availability || null,
      environment,
      teamContext,
      marketSide: confidence.market_side,
      selectedSide: payload?.recommended_side,
      marketDisagrees: confidence.market_disagrees,
      injuryFilterNames: getWithoutPlayerNamesFromPayload(payload),
      teamInjuryNames: (teamContext.players || []).map(item => item.name).filter(Boolean),
    });
    interpretationBody.className = 'interpretation-body';
    interpretationBody.innerHTML = `
      <div class="insight-summary ${interpretationToneClass}">
        <span class="insight-summary-label">Quick read</span>
        <strong>${escapeHtml(interpretation.headline || 'Quick read unavailable')}</strong>
        <p>${escapeHtml(interpretation.summary || 'Analyze a player prop to generate a simple read.')}</p>
      </div>${renderTrustDiagnostics(trustDiagnostics)}
      ${renderDecisionLensHtml(decisionLens)}
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
        <div class="insight-summary neutral compact-summary"><span class="insight-summary-label">Model read</span><p class="opportunity-summary">${escapeHtml(opportunity.summary || 'Opportunity trends will appear after analysis.')}</p></div>
        <div class="team-context-box ${teamContext.impact_count ? 'warning' : 'neutral'}"><strong>${escapeHtml(teamContext.headline || 'Lineup context')}</strong><p>${escapeHtml(teamContext.impact_summary || teamContext.summary || 'No team-availability context yet.')}</p>${listedPlayers.length ? `<small>${escapeHtml(listedPlayers.join(' • '))}</small>` : '<small>No major same-team absences flagged on the latest report.</small>'}</div>
      </div>`;
  }
  if (environmentBody) {
    const restValue = Number.isInteger(environment.rest_days) ? `${environment.rest_days} day${environment.rest_days === 1 ? '' : 's'}` : 'Unknown';
    const teamTotalValue = Number.isFinite(Number(environment.market_team_total)) ? Number(environment.market_team_total).toFixed(1) : '—';
    const gameTotalValue = Number.isFinite(Number(environment.market_game_total)) ? Number(environment.market_game_total).toFixed(1) : '—';
    const spreadValue = Number.isFinite(Number(environment.market_spread))
      ? `${Number(environment.market_spread) > 0 ? '+' : ''}${Number(environment.market_spread).toFixed(1)}`
      : '—';
    environmentBody.className = 'environment-body';
    environmentBody.innerHTML = `
      <div class="environment-chip-grid">
        <div class="environment-chip"><span class="small-label">Venue</span><strong>${escapeHtml(environment.venue_label || 'TBD')}</strong><small>${escapeHtml(environment.next_opponent ? `Next ${environment.next_opponent}` : 'Upcoming game')}</small></div>
        <div class="environment-chip"><span class="small-label">Rest</span><strong>${escapeHtml(restValue)}</strong><small>${escapeHtml(environment.headline || 'Schedule spot')}</small></div>
        <div class="environment-chip ${environment.is_back_to_back ? 'warning' : ''}"><span class="small-label">Back-to-back</span><strong>${environment.is_back_to_back ? 'Yes' : 'No'}</strong><small>${environment.is_back_to_back ? 'More fatigue risk' : 'No immediate fatigue flag'}</small></div>
        <div class="environment-chip"><span class="small-label">Games in 7</span><strong>${Number(environment.games_last7 || 0)}</strong><small>${Number(environment.games_last7 || 0) >= 4 ? 'Busy recent schedule' : 'Normal recent load'}</small></div>
        <div class="environment-chip"><span class="small-label">Team total</span><strong>${escapeHtml(teamTotalValue)}</strong><small>${escapeHtml(environment.market_team_total ? 'Implied points for this team' : 'Market total unavailable')}</small></div>
        <div class="environment-chip"><span class="small-label">Spread</span><strong>${escapeHtml(spreadValue)}</strong><small>${escapeHtml(environment.spread_label || 'Market spread unavailable')}</small></div>
        <div class="environment-chip"><span class="small-label">Game total</span><strong>${escapeHtml(gameTotalValue)}</strong><small>${escapeHtml(environment.market_game_total ? 'Full-game total from market' : 'Game total unavailable')}</small></div>
      </div>
      <div class="insight-summary ${environmentToneClass} compact-summary"><span class="insight-summary-label">Schedule read</span><strong>${escapeHtml(environment.headline || 'Schedule spot')}</strong><p class="opportunity-summary">${escapeHtml(environment.summary || 'Schedule context will appear after analysis.')}</p></div>`;
  }
  if (marketBody) {
    const teamTotalValue = Number.isFinite(Number(environment.market_team_total)) ? Number(environment.market_team_total).toFixed(1) : '—';
    const gameTotalValue = Number.isFinite(Number(environment.market_game_total)) ? Number(environment.market_game_total).toFixed(1) : '—';
    const spreadValue = Number.isFinite(Number(environment.market_spread))
      ? `${Number(environment.market_spread) > 0 ? '+' : ''}${Number(environment.market_spread).toFixed(1)}`
      : '—';
    const opponentTotalValue = Number.isFinite(Number(environment.market_opponent_total)) ? Number(environment.market_opponent_total).toFixed(1) : '—';
    const hasMarketContext = Number.isFinite(Number(environment.market_team_total)) || Number.isFinite(Number(environment.market_game_total)) || Number.isFinite(Number(environment.market_spread));
    const marketSummary = environment.market_summary || 'Market context unavailable for this matchup. Add usable Odds API keys to Key Vault, then run Analyze Prop again.';
    const trustDiagnostics = buildTrustDiagnostics({
      availability: payload?.availability || null,
      environment,
      teamContext,
      marketSide: confidence.market_side,
      selectedSide: payload?.recommended_side,
      marketDisagrees: confidence.market_disagrees,
      injuryFilterNames: getWithoutPlayerNamesFromPayload(payload),
      teamInjuryNames: (teamContext.players || []).map(item => item.name).filter(Boolean),
    });
    marketBody.className = 'environment-body';
    marketBody.innerHTML = `
      <div class="environment-chip-grid">
        <div class="environment-chip"><span class="small-label">Team total</span><strong>${escapeHtml(teamTotalValue)}</strong><small>${escapeHtml(environment.market_team_total ? 'Implied points for this team' : 'No team total found')}</small></div>
        <div class="environment-chip"><span class="small-label">Spread</span><strong>${escapeHtml(spreadValue)}</strong><small>${escapeHtml(environment.market_spread !== undefined && environment.market_spread !== null ? 'Current market spread' : 'No spread found')}</small></div>
        <div class="environment-chip"><span class="small-label">Game total</span><strong>${escapeHtml(gameTotalValue)}</strong><small>${escapeHtml(environment.market_game_total ? 'Full-game betting total' : 'No game total found')}</small></div>
        <div class="environment-chip"><span class="small-label">Opponent total</span><strong>${escapeHtml(opponentTotalValue)}</strong><small>${escapeHtml(environment.market_opponent_total ? 'Opponent implied points' : 'No opponent total found')}</small></div>
      </div>
      ${renderTrustDiagnostics(trustDiagnostics)}
      <div class="insight-summary ${hasMarketContext ? 'good' : 'neutral'} compact-summary"><span class="insight-summary-label">Market read</span><strong>${escapeHtml(hasMarketContext ? 'Betting market context loaded' : 'Market context unavailable')}</strong><p class="opportunity-summary">${escapeHtml(marketSummary)}</p><small>${escapeHtml(hasMarketContext ? 'Aligned market direction is a strong confirmation of the play; a conflicting market lean triggers a penalty and lowers confidence.' : 'Add usable Odds API keys to Key Vault and re-run analysis to add market context.')}</small></div>`;
  }

  // ── Variance / distribution panel ────────────────────────────────────
  const variance = payload?.variance || {};
  const sampleWarning = payload?.sample_warning || null;
  if (varianceTone) {
    const cs = Number(variance.consistency_score ?? -1);
    let vtLabel = 'No data';
    let vtClass = 'neutral';
    if (cs >= 0) {
      if (cs >= 72) { vtLabel = 'Consistent'; vtClass = 'good'; }
      else if (cs >= 48) { vtLabel = 'Mixed'; vtClass = 'warning'; }
      else { vtLabel = 'Volatile'; vtClass = 'bad'; }
    }
    varianceTone.className = `spotlight-pill ${vtClass}`;
    varianceTone.textContent = cs >= 0 ? `${vtLabel} • ${cs.toFixed(0)}` : 'Waiting for analysis';
  }
  if (varianceBody) {
    if (!variance || Object.keys(variance).length === 0) {
      varianceBody.innerHTML = `<div class="empty-state-panel compact matchup-empty"><div class="empty-icon">📊</div><strong>No distribution data yet.</strong><span>Analyze a player prop to see consistency, median, floor and ceiling.</span></div>`;
    } else {
      const cs = Number(variance.consistency_score ?? 0);
      const csColor = cs >= 72 ? 'var(--good)' : cs >= 48 ? 'var(--warning,#f59e0b)' : 'var(--bad)';
      const csLabel = cs >= 72 ? 'Consistent' : cs >= 48 ? 'Mixed signals' : 'Volatile';
      const statLabel = payload?.stat || 'stat';
      const line = Number(payload?.line || 0);

      // Build percentile bar: show p25 / median / p75 relative to floor-ceiling range
      const floor = Number(variance.floor ?? 0);
      const ceiling = Number(variance.ceiling ?? 1);
      const rangeSpan = ceiling - floor || 1;
      const p25Pct  = Math.round(((Number(variance.p25  ?? floor) - floor) / rangeSpan) * 100);
      const medPct  = Math.round(((Number(variance.median ?? (floor+ceiling)/2) - floor) / rangeSpan) * 100);
      const p75Pct  = Math.round(((Number(variance.p75  ?? ceiling) - floor) / rangeSpan) * 100);
      const linePct = Math.round(((line - floor) / rangeSpan) * 100);
      const linePctClamped = Math.max(0, Math.min(100, linePct));

      const warningHtml = sampleWarning
        ? `<div class="insight-summary warning compact-summary" style="margin-bottom:10px"><span class="insight-summary-label">⚠ Small sample</span><p>${escapeHtml(sampleWarning)}</p></div>`
        : '';

      varianceBody.className = 'environment-body';
      varianceBody.innerHTML = `
        ${warningHtml}
        <div class="opportunity-chip-grid" style="grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:10px;padding:16px 20px 10px">
          <div class="opportunity-chip">
            <span class="small-label">Consistency</span>
            <strong style="color:${csColor}">${cs.toFixed(0)}/100</strong>
            <small>${csLabel}</small>
          </div>
          <div class="opportunity-chip">
            <span class="small-label">Std Dev</span>
            <strong>${Number(variance.std_dev ?? 0).toFixed(1)}</strong>
            <small>${statLabel} variation</small>
          </div>
          <div class="opportunity-chip">
            <span class="small-label">Median</span>
            <strong>${Number(variance.median ?? 0).toFixed(1)}</strong>
            <small>vs avg ${Number(payload?.average ?? 0).toFixed(1)}</small>
          </div>
          <div class="opportunity-chip">
            <span class="small-label">Floor</span>
            <strong>${Number(variance.floor ?? 0).toFixed(1)}</strong>
            <small>worst recent</small>
          </div>
          <div class="opportunity-chip">
            <span class="small-label">Ceiling</span>
            <strong>${Number(variance.ceiling ?? 0).toFixed(1)}</strong>
            <small>best recent</small>
          </div>
          <div class="opportunity-chip">
            <span class="small-label">P25 / P75</span>
            <strong>${Number(variance.p25 ?? 0).toFixed(1)} – ${Number(variance.p75 ?? 0).toFixed(1)}</strong>
            <small>middle band</small>
          </div>
        </div>
        <div style="padding:0 20px 16px">
          <div class="insight-summary-label" style="margin-bottom:6px">Distribution range</div>
          <div style="position:relative;height:28px;background:var(--surface2,rgba(255,255,255,0.06));border-radius:14px;overflow:hidden">
            <!-- P25–P75 band -->
            <div style="position:absolute;top:0;bottom:0;left:${p25Pct}%;width:${p75Pct - p25Pct}%;background:rgba(99,179,237,0.22);border-radius:4px"></div>
            <!-- Median marker -->
            <div style="position:absolute;top:3px;bottom:3px;left:${medPct}%;width:3px;background:#63b3ed;border-radius:2px" title="Median ${Number(variance.median ?? 0).toFixed(1)}"></div>
            <!-- Prop line marker -->
            <div style="position:absolute;top:0;bottom:0;left:${linePctClamped}%;width:2px;background:var(--bad,#f87171);opacity:0.9" title="Line ${line}"></div>
          </div>
          <div style="display:flex;justify-content:space-between;margin-top:4px;font-size:10px;opacity:0.55">
            <span>Floor ${Number(variance.floor ?? 0).toFixed(1)}</span>
            <span style="color:#63b3ed">◆ Median</span>
            <span style="color:var(--bad)">┃ Line ${line}</span>
            <span>Ceiling ${Number(variance.ceiling ?? 0).toFixed(1)}</span>
          </div>
          <div class="insight-summary neutral compact-summary" style="margin-top:10px">
            <span class="insight-summary-label">What this means</span>
            <p>${variance.p25 > line
              ? `Floor (${Number(variance.p25).toFixed(1)}) sits above the line — player has barely dipped below in recent games. Strong OVER indicator.`
              : variance.p75 < line
              ? `Ceiling (${Number(variance.p75).toFixed(1)}) is under the line — even good games recently fall short. Strong UNDER indicator.`
              : cs < 48
              ? `High variance (std dev ${Number(variance.std_dev ?? 0).toFixed(1)}) — player output swings widely. Probability model applies a volatility discount.`
              : `Stable output with ${cs.toFixed(0)}/100 consistency. Model edge is more reliable when consistency is high.`
            }</p>
          </div>
        </div>`;
    }
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

function createTrendHitBandPlugin(chartGames, payload) {
  return {
    id: 'trendHitBandPlugin',
    beforeDatasetsDraw(chartInstance) {
      const { ctx, chartArea, scales: { x } } = chartInstance;
      if (!chartArea || !x) return;
      ctx.save();
      chartGames.forEach((game, index) => {
        if (!game || game.hit == null) return;
        const center = x.getPixelForValue(index);
        const width = Math.min(30, x.getPixelForValue(index + 1) - x.getPixelForValue(index)) || 22;
        const left = center - width / 2;
        const color = game.hit ? 'rgba(57,217,138,0.08)' : 'rgba(255,99,99,0.08)';
        ctx.fillStyle = color;
        ctx.fillRect(left, chartArea.top + 6, width, chartArea.bottom - chartArea.top - 12);
      });
      ctx.restore();
    }
  };
}

function createTrendConfidenceBandPlugin(payload) {
  return {
    id: 'trendConfidenceBandPlugin',
    beforeDatasetsDraw(chartInstance) {
      const { ctx, chartArea, scales: { y } } = chartInstance;
      if (!chartArea || !y) return;
      const p25 = Number(payload?.variance?.p25);
      const p75 = Number(payload?.variance?.p75);
      if (!Number.isFinite(p25) || !Number.isFinite(p75)) return;
      const yTop = y.getPixelForValue(p75);
      const yBottom = y.getPixelForValue(p25);
      ctx.save();
      ctx.fillStyle = 'rgba(96,165,250,0.10)';
      ctx.strokeStyle = 'rgba(96,165,250,0.22)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.rect(chartArea.left, yTop, chartArea.right - chartArea.left, yBottom - yTop);
      ctx.fill();
      ctx.stroke();
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
  const hitBandPlugin = createTrendHitBandPlugin(chartGames, payload);
  const confidenceBandPlugin = createTrendConfidenceBandPlugin(payload);
  const comboParts = getComboStatParts(payload.stat);
  const isComboMarket = comboParts.length > 1;

  if (chart) chart.destroy();
  const chartWrap = document.querySelector('.chart-wrap');
  if (chartWrap) {
    chartWrap.querySelectorAll('.chart-hit-legend').forEach(el => el.remove());
    if (!isComboMarket) {
      const legend = document.createElement('div');
      legend.className = 'chart-hit-legend';
      legend.innerHTML = '<span class="chart-hit-dot hit"></span>Hit <span class="chart-hit-dot miss"></span>Miss <span class="chart-hit-band"></span>Median band';
      chartWrap.appendChild(legend);
    }
  }
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
    } else {
      datasets.push({ type: 'line', label: 'Total', data: values, borderColor: accent, pointBackgroundColor: accent, pointRadius: 3, pointHoverRadius: 4, borderWidth: 2, tension: 0.28, fill: false, yAxisID: 'y' });
    }
  } else {
    const baseColors = hits.map(hit => hit ? `${good}CC` : `${bad}CC`);
    const baseBorders = hits.map(hit => hit ? good : bad);
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
    plugins: [confidenceBandPlugin, hitBandPlugin, linePlugin, valueLabelPlugin]
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
  if (!selectedPlayer) return;

  // Slice client-side instantly from the stored full game list — no fetch needed.
  if (lastPayload && lastPayload._allGames) {
    const n = parseInt(btn.dataset.sample, 10);
    if (lastPayload._allGames.length < n) {
      await analyzePlayerProp({ preserveScroll: true, preserveSection: true, forceRefresh: true, overrideLastN: n });
      return;
    }
    const sliced = lastPayload._allGames.slice(-n);
    const hits = sliced.filter(g => g.hit).length;
    const values = sliced.map(g => g.value);
    const avg = values.length ? Math.round((values.reduce((a, b) => a + b, 0) / values.length) * 10) / 10 : 0;
    const slicedPayload = Object.assign({}, lastPayload, {
      games: sliced,
      games_count: sliced.length,
      hit_count: hits,
      hit_rate: sliced.length ? Math.round((hits / sliced.length) * 1000) / 10 : 0,
      average: avg,
      last_n: n,
      _allGames: lastPayload._allGames,
    });
    renderSummary(slicedPayload);
    renderChart(slicedPayload);
    renderTable(slicedPayload);
    return;
  }

  // Fallback: full fetch (no payload loaded yet)
  const n = parseInt(btn.dataset.sample, 10);
  await analyzePlayerProp({ preserveScroll: true, preserveSection: true, forceRefresh: true, overrideLastN: n });
}));
collapseButtonsEl.forEach(btn => btn.addEventListener('click', () => toggleSectionByIdUpgrade(btn.dataset.collapseTarget)));
favoritePlayerBtnEl?.addEventListener('click', async () => {
  if (!selectedPlayer) return;
  try {
    await toggleFavoriteUpgrade({ type: 'player', key: String(selectedPlayer.id), title: selectedPlayer.full_name, subtitle: `${selectedPlayer.team_abbreviation || ''} ${selectedPlayer.position || ''}`.trim(), team_id: selectedPlayer.team_id || null, player: selectedPlayer });
  } catch (error) {
    showAppToast(error.message || 'Could not save player favorite.', 'warning');
  }
});
savePropBtnEl?.addEventListener('click', async () => {
  if (!selectedPlayer) return;
  const propKey = `${selectedPlayer.id}:${selectedStat}:${lineInput.value}`;
  const existing = loadFavoritesUpgrade().some(e => `${e.type}:${e.key}` === `prop:${propKey}`);
  const favoritePayload = { type: 'prop', key: propKey, title: `${selectedPlayer.full_name} • ${selectedStat} ${lineInput.value}`, subtitle: lastPayload?.traffic_light?.summary || lastPayload?.interpretation?.summary || 'Saved from analyzer', stat: selectedStat, line: Number(lineInput.value || 0), player: selectedPlayer };
  toggleFavoriteUpgrade(favoritePayload);
  if (existing) {
    showAppToast('Prop removed from Saved Props', 'warning', {
      undoLabel: 'Undo',
      onUndo: () => toggleFavoriteUpgrade(favoritePayload)
    });
  } else {
    showAppToast('Prop saved to Saved Props!', 'success');
  }
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
ensureFavoritesUpgradeLoaded().then(() => {
  renderFavoritesUpgrade();
  updateStickyAnalyzerSummary();
}).catch(() => {});
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
    summaryText = `${nextGame?.opponent_name || 'This opponent'} allows ${oppVal} ${(getStatLabel(vsPosition.stat) || '').toLowerCase()} per player-game to ${String(vsPosition.position_label || 'this position').toLowerCase()}, versus a league baseline of ${lgVal} (${delta}).`;
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
  const heroState = getAnalyzerHeroState();
  const heroChipsHtml = heroState?.chips?.length
    ? `<div class="selected-player-signal-row">${heroState.chips.map(chip => `<span class="selected-player-signal-chip">${escapeHtml(chip)}</span>`).join('')}</div>`
    : '';
  const heroSummaryHtml = heroState?.summary
    ? `<div class="selected-player-summary">${escapeHtml(heroState.summary)}</div>`
    : '<div class="selected-player-summary muted">Analyze the selected prop to surface the strongest signals here.</div>';

  selectedPlayerBadge.className = 'selected-player';
  selectedPlayerBadge.innerHTML = `
    <img src="${getPlayerImage(selectedPlayer.id)}" alt="${escapeHtml(selectedPlayer.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
    <div class="selected-player-copy">
      <span class="selected-player-label">Selected player</span>
      <strong>${escapeHtml(selectedPlayer.full_name)}</strong>
      <small>${escapeHtml(subLine || (selectedPlayer.is_active ? 'Active player' : 'Player'))}</small>
      ${availabilityHtml}
      ${heroChipsHtml}
      ${heroSummaryHtml}
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
  if (decisionStripToneEl) {
    decisionStripToneEl.className = 'spotlight-pill neutral';
    decisionStripToneEl.textContent = 'Waiting for analysis';
  }
  if (analyzerDecisionStripGrid) {
    analyzerDecisionStripGrid.innerHTML = `
      <div class="decision-strip-chip neutral">
        <span class="small-label">Recommendation</span>
        <strong>Awaiting analysis</strong>
        <small>Run Analyze Prop to populate</small>
      </div>
      <div class="decision-strip-chip neutral">
        <span class="small-label">Hit rate</span>
        <strong>—</strong>
        <small>Recent sample</small>
      </div>
      <div class="decision-strip-chip neutral">
        <span class="small-label">Avg vs line</span>
        <strong>—</strong>
        <small>Line cushion</small>
      </div>
      <div class="decision-strip-chip neutral">
        <span class="small-label">Lineup</span>
        <strong>—</strong>
        <small>Same-team absences</small>
      </div>
      <div class="decision-strip-chip neutral">
        <span class="small-label">Market</span>
        <strong>—</strong>
        <small>Alignment status</small>
      </div>
    `;
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
        <div class="insight-summary neutral compact-summary"><span class="insight-summary-label">Model read</span><p class="opportunity-summary">Analyze a player prop to load minutes, attempts, and team context.</p></div>
        <div class="team-context-box neutral"><strong>Lineup context</strong><p>No team-availability context yet.</p><small>Latest same-team absences will appear here after analysis.</small></div>
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
  if (marketBody) {
    marketBody.className = 'environment-body';
    marketBody.innerHTML = `
      <div class="environment-chip-grid">
        <div class="environment-chip"><span class="small-label">Team total</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="environment-chip"><span class="small-label">Spread</span><strong>—</strong><small>Waiting for analysis</small></div>
        <div class="environment-chip"><span class="small-label">Game total</span><strong>—</strong><small>Waiting for analysis</small></div>
      </div>
      <div class="insight-summary neutral compact-summary"><span class="insight-summary-label">Market read</span><p class="opportunity-summary">Analyze a player prop to load team total, spread, and full-game total.</p></div>
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
  if (sortKey === 'best_combo') return 'Sorted by scanner intelligence';
  return 'Sorted by best EV';
}

function getMarketSortValue(item, sortKey) {
  const ev = Number(item?.best_bet?.ev ?? Number.NEGATIVE_INFINITY);
  const edge = Number(item?.best_bet?.edge ?? Number.NEGATIVE_INFINITY);
  const hitRate = Number(item?.analysis?.hit_rate ?? Number.NEGATIVE_INFINITY);
  const confidence = Number(item?.best_bet?.confidence_score ?? Number.NEGATIVE_INFINITY);
  const rankingScore = Number(item?.best_bet?.ranking_score ?? confidence ?? Number.NEGATIVE_INFINITY);
  const marketPenalty = Number(item?.best_bet?.market_penalty ?? 0);
  const availabilityRank = Number(item?.availability?.sort_rank ?? 3);

  if (sortKey === 'best_edge') return [edge, ev, hitRate, confidence, -availabilityRank];
  if (sortKey === 'highest_hit_rate') return [hitRate, ev, edge, confidence, -availabilityRank];
  if (sortKey === 'best_combo') return [rankingScore, ev, edge, hitRate, -marketPenalty, -availabilityRank];
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
  if (todayGamesRetryTimer) {
    clearTimeout(todayGamesRetryTimer);
    todayGamesRetryTimer = null;
  }
  if (todayGamesLoadInFlight) return;
  if (!latestTodayGamesPayload || force) {
    try {
      const cached = JSON.parse(localStorage.getItem(TODAY_GAMES_CACHE_KEY) || 'null');
      if (cached?.payload && cached?.savedAt && Date.now() - cached.savedAt < TODAY_GAMES_CACHE_TTL_MS) {
        renderTodayGames(cached.payload);
        if (todayGamesMeta) {
          todayGamesMeta.textContent = `Showing cached slate • ${todayGamesMeta.textContent || ''}`.trim();
        }
      }
    } catch (error) {
      // ignore cache read errors
    }
  }
  if (latestTodayGamesPayload && !force) {
    renderTodayGames(latestTodayGamesPayload);
    if (!latestTodayGamesPayload.__marketContextLoaded) {
      enrichTodayGamesWithMarketContext(latestTodayGamesPayload)
        .then(changed => {
          latestTodayGamesPayload.__marketContextLoaded = true;
          if (changed) renderTodayGames(latestTodayGamesPayload);
        })
        .catch(error => console.warn("Today's Games market context enrichment failed:", error));
    }
    return;
  }

  todayGamesLoadInFlight = true;
  if (todayGamesMeta) todayGamesMeta.textContent = "Loading today's NBA slate...";
  if (overviewTodayMeta) overviewTodayMeta.textContent = "Fetching today's slate...";
  if (todayGamesBoard) todayGamesBoard.innerHTML = `<div class="empty-state-panel compact skeleton-panel today-game-empty"><div class="empty-icon">🗓️</div><strong>Loading games...</strong><span>Pulling the current slate and report context.</span><div class="skeleton-line"></div><div class="skeleton-line short"></div></div>`;
  if (overviewTodayGames) overviewTodayGames.innerHTML = `<div class="empty-state-panel compact skeleton-panel today-game-empty"><div class="empty-icon">🗓️</div><strong>Loading games...</strong><span>Pulling the current slate and report context.</span></div>`;
  try {
    const payload = await apiFetch('/api/todays-games', {}, 20000);
    try {
      renderTodayGames(payload);
    } catch (renderError) {
      console.error('Today games render failed:', renderError);
      if (todayGamesBoard) {
        todayGamesBoard.innerHTML = `<div class="empty-state-panel compact today-game-empty"><div class="empty-icon">⚠️</div><strong>Could not render today&#39;s games.</strong><span>${escapeHtml(renderError.message || 'Render failed.')}</span></div>`;
      }
    }
    todayGamesLastSuccessAt = Date.now();
    todayGamesLastErrorAt = 0;
    todayGamesLoadAttempts = 0;
    enrichTodayGamesWithMarketContext(payload)
      .then(changed => {
        payload.__marketContextLoaded = true;
        if (changed) renderTodayGames(payload);
      })
      .catch(error => console.warn("Today's Games market context enrichment failed:", error));

    const hasGames = Array.isArray(payload.games) && payload.games.length > 0;
    if (!hasGames) {
      todayGamesLoadAttempts += 1;
      if (todayGamesLoadAttempts <= 2) {
        todayGamesRetryTimer = setTimeout(() => loadTodayGames(true), 2500);
      }
    }
  } catch (error) {
    console.error(error);
    const fallbackMessage = error.message || "Failed to load today's slate.";
    if (todayGamesBoard) {
      todayGamesBoard.innerHTML = `<div class="empty-state-panel compact today-game-empty"><div class="empty-icon">⚠️</div><strong>Could not load today&#39;s games.</strong><span>${escapeHtml(fallbackMessage)}</span></div>`;
    }
    if (overviewTodayGames) {
      overviewTodayGames.innerHTML = `<div class="empty-state-panel compact today-game-empty"><div class="empty-icon">⚠️</div><strong>Could not load the slate.</strong><span>${escapeHtml(fallbackMessage)}</span></div>`;
    }
    todayGamesLastErrorAt = Date.now();
    todayGamesLoadAttempts += 1;
    if (todayGamesLoadAttempts <= 3) {
      todayGamesRetryTimer = setTimeout(() => loadTodayGames(true), 2500);
    }
  } finally {
    todayGamesLoadInFlight = false;
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
const filterOpponentRankRangeEl = document.getElementById('filterOpponentRankRange');
const filterWithoutPlayerEl = document.getElementById('filterWithoutPlayer');
const withoutChipPickerEl = document.getElementById('withoutChipPicker');
const filterH2HOnlyEl = document.getElementById('filterH2HOnly');
const applyFiltersBtnEl = document.getElementById('applyFiltersBtn');
const resetFiltersBtnEl = document.getElementById('resetFiltersBtn');

function _syncHiddenSelectFromChips() {
  if (!filterWithoutPlayerEl || !withoutChipPickerEl) return;
  const selectedIds = new Set(
    Array.from(withoutChipPickerEl.querySelectorAll('.without-chip.selected'))
      .map(chip => chip.dataset.playerId)
  );
  Array.from(filterWithoutPlayerEl.options).forEach(opt => {
    opt.selected = selectedIds.has(opt.value);
  });
  filterWithoutPlayerEl.dispatchEvent(new Event('change'));
}

function _renderWithoutChips(players, selectedIds = new Set()) {
  if (!withoutChipPickerEl) return;
  if (!players || !players.length) {
    withoutChipPickerEl.innerHTML = '<span class="without-chip-placeholder">No teammates found</span>';
    return;
  }
  withoutChipPickerEl.innerHTML = players.map(player => {
    const isSelected = selectedIds.has(String(player.id));
    return `<button type="button" class="without-chip${isSelected ? ' selected' : ''}" data-player-id="${player.id}" data-player-name="${escapeHtml(player.full_name || '')}">${escapeHtml(player.full_name || 'Unknown')}</button>`;
  }).join('');
  withoutChipPickerEl.querySelectorAll('.without-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      chip.classList.toggle('selected');
      _syncHiddenSelectFromChips();
      refreshFilterChipStates();
      updateFilterSummaryUpgrade();
    });
  });
}

async function loadInjuryBoostChips(player) {
  const injuryBoostGroup = document.getElementById('injuryBoostGroup');
  const injuryBoostChips = document.getElementById('injuryBoostChips');
  if (!injuryBoostGroup || !injuryBoostChips) return;

  const teamName = player?.team_name || '';
  if (!teamName) {
    injuryBoostGroup.style.display = 'none';
    return;
  }

  try {
    const data = await apiFetch('/api/team-injuries?team_name=' + encodeURIComponent(teamName), {}, 10000).catch(() => null);
    if (!data) { injuryBoostGroup.style.display = 'none'; return; }
    const players = (data.players || []).filter(p => String(p.player_id) !== String(player?.id || ''));

    if (!players.length) {
      injuryBoostGroup.style.display = 'none';
      return;
    }

    injuryBoostGroup.style.display = '';
    injuryBoostChips.innerHTML = players.map(p => {
      const isOut = p.is_unavailable;
      const statusLabel = isOut ? 'OUT' : (p.status || 'Q');
      return '<button class="inj-boost-chip' + (isOut ? ' out-chip' : '') + '" ' +
        'data-player-id="' + escapeHtml(String(p.player_id || '')) + '" ' +
        'data-player-name="' + escapeHtml(p.lookup_name || p.display_name || '') + '" ' +
        'title="Apply without-' + escapeHtml(p.lookup_name || p.display_name || '') + ' filter (' + escapeHtml(p.status) + ')">' +
        '<span class="inj-status-dot"></span>' +
        escapeHtml(p.lookup_name || p.display_name || '') + ' <span style="opacity:0.6;font-size:0.65rem">(' + escapeHtml(statusLabel) + ')</span>' +
        '</button>';
    }).join('');

    // Wire up chip clicks → add to without-teammate filter
    injuryBoostChips.querySelectorAll('.inj-boost-chip').forEach(chip => {
      chip.addEventListener('click', () => {
        const pid = chip.dataset.playerId;
        if (!pid || !filterWithoutPlayerEl) return;
        const opt = Array.from(filterWithoutPlayerEl.options).find(o => o.value === pid);
        if (opt) {
          opt.selected = !opt.selected;
          // Re-render the without chips to reflect new selection
          const currentSelectedIds = new Set(
            Array.from(filterWithoutPlayerEl.selectedOptions || []).map(o => String(o.value))
          );
          const allPlayers = Array.from(filterWithoutPlayerEl.options).map(o => ({
            id: o.value, full_name: o.text
          }));
          _renderWithoutChips(allPlayers, currentSelectedIds);
          chip.style.outline = opt.selected ? '2px solid rgba(255,160,40,0.6)' : '';
          chip.style.background = opt.selected ? 'rgba(255,160,40,0.18)' : '';
        }
      });
    });

  } catch (e) {
    console.warn('loadInjuryBoostChips failed:', e);
    injuryBoostGroup.style.display = 'none';
  }
}

async function populateAnalyzerWithoutPlayerFilter(force = false) {
  if (!filterWithoutPlayerEl) return;
  const teamId = Number(selectedPlayer?.team_id || 0);
  if (!teamId) {
    if (withoutChipPickerEl) withoutChipPickerEl.innerHTML = '<span class="without-chip-placeholder">Select a player first</span>';
    filterWithoutPlayerEl.innerHTML = '';
    return;
  }
  if (!force && Number(filterWithoutPlayerEl.dataset.teamId || 0) === teamId && filterWithoutPlayerEl.options.length > 1) return;
  try {
    let players = Array.isArray(rosterPlayers) ? rosterPlayers.filter(player => Number(player.team_id || 0) === teamId) : [];
    if (!players.length) {
      const season = seasonInput?.value?.trim();
      const query = season ? `?season=${encodeURIComponent(season)}` : '';
      const payload = await apiFetch(`/api/teams/${teamId}/roster${query}`, {}, 15000);
      players = Array.isArray(payload.results) ? payload.results : [];
    }

    const currentSelectedIds = new Set(
      Array.from(filterWithoutPlayerEl.selectedOptions || []).map(o => String(o.value))
    );

    const filteredPlayers = players
      .filter(player => String(player.id) !== String(selectedPlayer?.id || ''))
      .sort((a, b) => String(a.full_name || '').localeCompare(String(b.full_name || '')));

    // Sync hidden select
    filterWithoutPlayerEl.innerHTML = filteredPlayers.map(player =>
      `<option value="${player.id}">${escapeHtml(player.full_name || 'Unknown teammate')}</option>`
    ).join('');
    filterWithoutPlayerEl.dataset.teamId = String(teamId);
    Array.from(filterWithoutPlayerEl.options).forEach(option => {
      option.selected = currentSelectedIds.has(String(option.value));
    });

    _renderWithoutChips(filteredPlayers, currentSelectedIds);

  } catch (error) {
    console.warn('Unable to populate teammate absence filter:', error.message || error);
    if (withoutChipPickerEl) withoutChipPickerEl.innerHTML = '<span class="without-chip-placeholder">Could not load roster</span>';
    filterWithoutPlayerEl.innerHTML = '';
    delete filterWithoutPlayerEl.dataset.teamId;
  }
}

function parseNullableNumber(value) {
  if (value === null || value === undefined) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const number = Number(raw);
  return Number.isFinite(number) ? number : null;
}

function getSelectedWithoutPlayers() {
  if (!filterWithoutPlayerEl) return { ids: [], names: [] };
  const selectedOptions = Array.from(filterWithoutPlayerEl.selectedOptions || []);
  const ids = selectedOptions
    .map(option => Number(option.value))
    .filter(value => Number.isFinite(value) && value > 0);
  const names = selectedOptions
    .map(option => String(option.textContent || '').trim())
    .filter(Boolean);
  return { ids, names };
}

function getAnalyzerFiltersState() {
  const withoutTeammates = getSelectedWithoutPlayers();
  return {
    location: filterLocationSelectEl?.value || 'all',
    result: filterResultSelectEl?.value || 'all',
    margin_min: parseNullableNumber(filterMarginMinEl?.value),
    margin_max: parseNullableNumber(filterMarginMaxEl?.value),
    min_minutes: parseNullableNumber(filterMinMinutesEl?.value),
    max_minutes: parseNullableNumber(filterMaxMinutesEl?.value),
    min_fga: parseNullableNumber(filterMinFgaEl?.value),
    max_fga: parseNullableNumber(filterMaxFgaEl?.value),
    opponent_rank_range: filterOpponentRankRangeEl?.value || 'all',
    without_player_id: withoutTeammates.ids[0] || null,
    without_player_ids: withoutTeammates.ids,
    without_player_name: withoutTeammates.names[0] || '',
    without_player_names: withoutTeammates.names,
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
  if (filterOpponentRankRangeEl) filterOpponentRankRangeEl.value = state.opponent_rank_range || 'all';
  if (filterWithoutPlayerEl) {
    const selectedIds = new Set((Array.isArray(state.without_player_ids) ? state.without_player_ids : (state.without_player_id ? [state.without_player_id] : [])).map(value => String(value)));
    Array.from(filterWithoutPlayerEl.options || []).forEach(option => {
      option.selected = selectedIds.has(String(option.value));
    });
    // Sync chip UI
    if (withoutChipPickerEl) {
      withoutChipPickerEl.querySelectorAll('.without-chip').forEach(chip => {
        chip.classList.toggle('selected', selectedIds.has(chip.dataset.playerId));
      });
    }
  }
  if (filterH2HOnlyEl) filterH2HOnlyEl.checked = !!state.h2h_only;
  refreshFilterChipStates();
  updateFilterSummaryUpgrade();
}

function resetAnalyzerFiltersState() {
  [filterLocationSelectEl, filterResultSelectEl, filterMarginMinEl, filterMarginMaxEl, filterMinMinutesEl, filterMaxMinutesEl, filterMinFgaEl, filterMaxFgaEl, filterOpponentRankRangeEl, filterWithoutPlayerEl, filterH2HOnlyEl].forEach(el => {
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
  if (filters.opponent_rank_range && filters.opponent_rank_range !== 'all') {
    const map = { top10: 'Opp rank 1-10', top5: 'Opp rank 1-5', mid10: 'Opp rank 11-20', bottom10: 'Opp rank 21-30', bottom5: 'Opp rank 26-30' };
    chips.push(map[filters.opponent_rank_range] || `Opp rank ${filters.opponent_rank_range}`);
  }
  if ((filters.without_player_ids && filters.without_player_ids.length) || filters.without_player_id) {
    const teammateNames = Array.isArray(filters.without_player_names) ? filters.without_player_names.filter(Boolean) : [];
    const teammate = teammateNames.length ? teammateNames.join(', ') : String(filters.without_player_name || '').trim();
    chips.push(teammate ? `Without ${teammate}` : 'Without teammate');
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
  if (filters.location === 'home' && filters.result === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && (filters.opponent_rank_range || 'all') === 'all' && !((filters.without_player_ids && filters.without_player_ids.length) || filters.without_player_id) && !filters.h2h_only) return 'home';
  if (filters.location === 'away' && filters.result === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && (filters.opponent_rank_range || 'all') === 'all' && !((filters.without_player_ids && filters.without_player_ids.length) || filters.without_player_id) && !filters.h2h_only) return 'away';
  if (filters.result === 'win' && filters.location === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && (filters.opponent_rank_range || 'all') === 'all' && !((filters.without_player_ids && filters.without_player_ids.length) || filters.without_player_id) && !filters.h2h_only) return 'wins';
  if (filters.result === 'loss' && filters.location === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && (filters.opponent_rank_range || 'all') === 'all' && !((filters.without_player_ids && filters.without_player_ids.length) || filters.without_player_id) && !filters.h2h_only) return 'losses';
  if (filters.h2h_only && filters.location === 'all' && filters.result === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && (filters.opponent_rank_range || 'all') === 'all' && !((filters.without_player_ids && filters.without_player_ids.length) || filters.without_player_id)) return 'h2h';
  if (filters.min_minutes === 30 && filters.max_minutes === null && filters.location === 'all' && filters.result === 'all' && filters.margin_min === null && filters.margin_max === null && filters.min_fga === null && filters.max_fga === null && (filters.opponent_rank_range || 'all') === 'all' && !((filters.without_player_ids && filters.without_player_ids.length) || filters.without_player_id) && !filters.h2h_only) return 'min30';
  if (filters.margin_min === 0 && filters.margin_max === 5 && filters.location === 'all' && filters.result === 'all' && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && (filters.opponent_rank_range || 'all') === 'all' && !((filters.without_player_ids && filters.without_player_ids.length) || filters.without_player_id) && !filters.h2h_only) return 'close';
  if (filters.margin_min === 11 && filters.margin_max === null && filters.location === 'all' && filters.result === 'all' && filters.min_minutes === null && filters.max_minutes === null && filters.min_fga === null && filters.max_fga === null && (filters.opponent_rank_range || 'all') === 'all' && !((filters.without_player_ids && filters.without_player_ids.length) || filters.without_player_id) && !filters.h2h_only) return 'blowout';
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
  const environment = payload.environment || {};
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
  const marketTeamTotal = Number.isFinite(Number(environment.market_team_total)) ? Number(environment.market_team_total).toFixed(1) : '';
  const marketGameTotal = Number.isFinite(Number(environment.market_game_total)) ? Number(environment.market_game_total).toFixed(1) : '';
  const marketSpread = Number.isFinite(Number(environment.market_spread))
    ? `${Number(environment.market_spread) > 0 ? '+' : ''}${Number(environment.market_spread).toFixed(1)}`
    : '';
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
    ${marketTeamTotal ? `<span class="chart-chip">Team total ${escapeHtml(marketTeamTotal)}</span>` : ''}
    ${marketSpread ? `<span class="chart-chip">Spread ${escapeHtml(marketSpread)}</span>` : ''}
    ${marketGameTotal ? `<span class="chart-chip">Game total ${escapeHtml(marketGameTotal)}</span>` : ''}
    ${h2h.games_count ? `<span class="chart-chip">H2H ${h2h.hit_count}/${h2h.games_count} vs ${escapeHtml(h2h.opponent_abbreviation || h2h.opponent_name || 'opponent')}</span>` : ''}
  `;

  renderMatchup(payload);
  renderInterpretationPanels(payload);
  updateFilterSummaryUpgrade(payload);
  // Prefill the backtest log form with current analysis results
  if (typeof window._backtestPrefillFromPayload === 'function') {
    window._backtestPrefillFromPayload(payload);
  }
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
    await populateAnalyzerWithoutPlayerFilter();
    const requestedLastN = Number.isFinite(Number(options.overrideLastN)) && Number(options.overrideLastN) > 0
      ? Number(options.overrideLastN)
      : 20;
    const params = new URLSearchParams({
      player_id: selectedPlayer.id,
      stat: selectedStat,
      line: lineInput.value,
      last_n: requestedLastN  // keep analyzer aligned with the originating surface
    });
    if (selectedPlayer.market_over_odds) params.set('over_odds', selectedPlayer.market_over_odds);
    if (selectedPlayer.market_under_odds) params.set('under_odds', selectedPlayer.market_under_odds);
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
    if (filters.opponent_rank_range && filters.opponent_rank_range !== 'all') params.set('opponent_rank_range', filters.opponent_rank_range);
    if (Array.isArray(filters.without_player_ids) && filters.without_player_ids.length) {
      filters.without_player_ids.forEach(id => params.append('without_player_ids', String(id)));
      if (Array.isArray(filters.without_player_names) && filters.without_player_names.length) params.set('without_player_name', filters.without_player_names.join(', '));
    } else if (filters.without_player_id) {
      params.set('without_player_id', String(filters.without_player_id));
      if (filters.without_player_name) params.set('without_player_name', filters.without_player_name);
    }
    if (filters.h2h_only) params.set('h2h_only', 'true');

    const overrideOppId = oppSelect?.value || selectedPlayer?.matchup?.next_game?.opponent_team_id || selectedPlayer?.opponent_team_id;
    if (overrideOppId) params.set('override_opponent_id', String(overrideOppId));

    const response = await apiFetch(`/api/player-prop?${params.toString()}${localStorage.getItem(INJURY_DEBUG_STORAGE) === '1' ? '&debug=true' : ''}`, {}, 25000);
    const payload = response;
    if (!payload || typeof payload !== 'object') throw new Error('Failed to analyze player prop.');

    const priorAvailability = selectedPlayer?.availability && typeof selectedPlayer.availability === 'object'
      ? selectedPlayer.availability
      : null;
    const priorMatchup = selectedPlayer?.matchup && typeof selectedPlayer.matchup === 'object'
      ? selectedPlayer.matchup
      : {};
    const priorEnvironment = selectedPlayer?.environment && typeof selectedPlayer.environment === 'object'
      ? selectedPlayer.environment
      : {};

    payload.availability = payload.availability || priorAvailability;
    payload.matchup = {
      ...priorMatchup,
      ...(payload.matchup || {}),
      next_game: {
        ...(priorMatchup?.next_game || {}),
        ...((payload.matchup || {}).next_game || {}),
      },
      vs_position: {
        ...(priorMatchup?.vs_position || {}),
        ...((payload.matchup || {}).vs_position || {}),
      },
    };
    payload.environment = {
      ...priorEnvironment,
      ...(payload.environment || {}),
    };

    try {
      const marketContextResult = await fetchAnalyzerGameContext(payload);
      if (marketContextResult?.environment) {
        payload.environment = { ...(payload.environment || {}), ...(marketContextResult.environment || {}) };
      }
    } catch (marketError) {
      console.warn('Analyzer market context fetch failed:', marketError);
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
    await populateAnalyzerWithoutPlayerFilter();
    if (filterWithoutPlayerEl) {
      const selectedIds = new Set((payload.filter_options?.without_player_ids || (payload.filter_options?.without_player_id ? [payload.filter_options.without_player_id] : [])).map(value => String(value)));
      Array.from(filterWithoutPlayerEl.options || []).forEach(option => {
        option.selected = selectedIds.has(String(option.value));
      });
      if (withoutChipPickerEl) {
        withoutChipPickerEl.querySelectorAll('.without-chip').forEach(chip => {
          chip.classList.toggle('selected', selectedIds.has(chip.dataset.playerId));
        });
      }
    }
    // Store the full 20-game list and slice down to the selected window for display
    const _allGames = (payload.games || []).slice();
    const _selectedN = parseInt(gamesSelect ? gamesSelect.value : '10', 10);
    let displayPayload;
    if (_allGames.length > _selectedN) {
      const _sliced = _allGames.slice(-_selectedN);
      const _hits = _sliced.filter(g => g.hit).length;
      const _values = _sliced.map(g => g.value);
      const _avg = _values.length ? Math.round((_values.reduce((a, b) => a + b, 0) / _values.length) * 10) / 10 : 0;
      displayPayload = Object.assign({}, payload, {
        games: _sliced,
        games_count: _sliced.length,
        hit_count: _hits,
        hit_rate: _sliced.length ? Math.round((_hits / _sliced.length) * 1000) / 10 : 0,
        average: _avg,
        last_n: _selectedN,
        _allGames: _allGames,
      });
    } else {
      displayPayload = Object.assign({}, payload, { _allGames: _allGames });
    }
    renderSummary(displayPayload);
    if (displayPayload.games && displayPayload.games.length) {
      renderChart(displayPayload);
      renderTable(displayPayload);
    } else {
      renderEmptyFilterStatesUpgrade();
      renderInterpretationPanels(displayPayload);
      updateFilterSummaryUpgrade(displayPayload);
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
  // lastN excluded — backend slices from shared filtered_pool, all windows share one cache entry.
  // without_player_ids included — filtered/unfiltered results must not collide.
  const state = filters || (typeof getAnalyzerFiltersState === 'function' ? getAnalyzerFiltersState() : {});
  const withoutIds = Array.isArray(state.without_player_ids) && state.without_player_ids.length
    ? state.without_player_ids.slice().sort().join(',')
    : (state.without_player_id ? String(state.without_player_id) : '');
  const filterKey = [
    state.location || 'all',
    state.result || 'all',
    state.margin_min ?? '',
    state.margin_max ?? '',
    state.min_minutes ?? '',
    state.max_minutes ?? '',
    state.min_fga ?? '',
    state.max_fga ?? '',
    state.h2h_only ? '1' : '0',
    withoutIds
  ].join(':');
  return `prop:${playerId}:${stat}:${line}:${filterKey}`;
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
  const pts = values.map(function (v, i) { return toX(i).toFixed(1) + ',' + toY(v).toFixed(1); }).join(' ');
  const lineY = toY(line).toFixed(1);
  const dots = values.map(function (v, i) {
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
  renderBetFinderResults = function (payload) {
    _orig.call(this, payload);
    const results = payload.results || [];
    document.querySelectorAll('.finder-card').forEach(function (card, idx) {
      const item = results[idx];
      if (!item) return;
      const values = (item.games || []).map(function (g) { return Number(g.value || 0); });
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
  analyzePlayerProp = async function (options) {
    options = options || {};
    if (!selectedPlayer) { alert('Please select a player first.'); return; }

    // Remove stale error overlay
    document.querySelectorAll('.chart-error-overlay').forEach(function (el) { el.remove(); });

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
        // Re-slice from _allGames to the currently selected window
        let cachedDisplay = cached;
        if (cached._allGames && cached._allGames.length) {
          const _n = parseInt(gamesSelect ? gamesSelect.value : '10', 10);
          const _sliced = cached._allGames.slice(-_n);
          const _hits = _sliced.filter(function (g) { return g.hit; }).length;
          const _vals = _sliced.map(function (g) { return g.value; });
          const _avg = _vals.length ? Math.round((_vals.reduce(function (a, b) { return a + b; }, 0) / _vals.length) * 10) / 10 : 0;
          cachedDisplay = Object.assign({}, cached, {
            games: _sliced,
            games_count: _sliced.length,
            hit_count: _hits,
            hit_rate: _sliced.length ? Math.round((_hits / _sliced.length) * 1000) / 10 : 0,
            average: _avg,
            last_n: _n,
            _allGames: cached._allGames,
          });
        }
        renderSummary(cachedDisplay);
        if (cachedDisplay.games && cachedDisplay.games.length) {
          renderChart(cachedDisplay);
          renderTable(cachedDisplay);
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
  renderChart = function (payload) {
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
  lineInput.addEventListener('input', function () {
    if (typeof updateStickyAnalyzerSummary === 'function') updateStickyAnalyzerSummary();
    clearTimeout(window._propLineDebounce);
    window._propLineDebounce = setTimeout(function () {
      if (selectedPlayer && lastPayload) analyzePlayerProp({ preserveScroll: true });
    }, 800);
  });
})();

/* ── Keyboard shortcuts: J/K cycle players, ←/→ cycle stats ──── */
(function bindKeyboardShortcuts() {
  document.addEventListener('keydown', function (e) {
    const tag = (document.activeElement || {}).tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
    if (e.metaKey || e.ctrlKey || e.altKey) return;

    if (e.key === 'j' || e.key === 'k') {
      e.preventDefault();
      const cards = Array.from(document.querySelectorAll('.player-card'));
      if (!cards.length) return;
      const curIdx = selectedPlayer ? cards.findIndex(function (c) { return Number(c.dataset.id) === selectedPlayer.id; }) : -1;
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
      const curIdx = chips.findIndex(function (c) { return c.dataset.stat === selectedStat; });
      const nextIdx = e.key === 'ArrowRight' ? Math.min(curIdx + 1, chips.length - 1) : Math.max(curIdx - 1, 0);
      if (chips[nextIdx]) chips[nextIdx].click();
    }
  });
})();

/* ── Restore last player + stat on page load ─────────────────── */
(function restoreLastSession() {
  const lastStat = localStorage.getItem(LAST_STAT_KEY);
  if (lastStat && propButtonsWrap) {
    try { setActiveProp(lastStat); } catch (e) { /* ignore */ }
  }
  const raw = localStorage.getItem(LAST_PLAYER_KEY);
  if (raw) {
    try {
      const player = JSON.parse(raw);
      if (player && player.id) {
        setSelectedPlayer(player);
      }
    } catch (e) { /* ignore */ }
  }
})();

/* ── Patch setActiveProp to persist stat ────────────────────────── */
(function patchSetActivePropPersist() {
  const _orig = setActiveProp;
  setActiveProp = function (stat) {
    _orig.call(this, stat);
    try { localStorage.setItem(LAST_STAT_KEY, stat); } catch (e) { /* ignore */ }
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
  const PARLAY_SETTINGS_STORAGE = 'nba-props-parlay-settings';

  // ── DOM refs ──────────────────────────────────────────────────────────
  const parlayApiKeys = document.getElementById('parlayApiKeys');
  const parlaySportSelect = document.getElementById('parlaySportSelect');
  const parlayOddsFormatSel = document.getElementById('parlayOddsFormatSelect');
  const parlayLegsSelect = document.getElementById('parlayLegsSelect');
  const parlayLastNSelect = document.getElementById('parlayLastNSelect');
  const parlayBookmakerSelect = document.getElementById('parlayBookmakerSelect');
  const parlayLoadEventsBtn = document.getElementById('parlayLoadEventsBtn');

  // ── Progress bar helper ───────────────────────────────────────────────
  function setParlayProgress(pct, label) {
    var bar = document.getElementById('parlayProgressBar');
    var lbl = document.getElementById('parlayProgressLabel');
    var percent = document.getElementById('parlayProgressPercent');
    var ball = document.getElementById('parlayProgressBall');
    var wrap = document.getElementById('parlayProgressWrap');
    if (!wrap) return;
    if (pct <= 0) { wrap.style.display = 'none'; return; }
    wrap.style.display = '';
    if (bar) { bar.style.width = pct + '%'; bar.setAttribute('aria-valuenow', pct); }
    if (lbl && label) lbl.textContent = label;
    if (percent) percent.textContent = Math.max(0, Math.round(pct)) + '%';
    if (ball) {
      var clampedPct = Math.max(0, Math.min(100, Number(pct) || 0));
      ball.style.left = 'calc(' + clampedPct + '% - 12px)';
      ball.style.transform = 'translateY(' + (clampedPct >= 100 ? '-4px' : ((clampedPct % 18) < 9 ? '-10px' : '-2px')) + ')';
    }
  }
  const parlayEventPickerWrap = document.getElementById('parlayEventPickerWrap');
  const parlayEventList = document.getElementById('parlayEventList');
  const parlaySelectAllBtn = document.getElementById('parlaySelectAllBtn');
  const parlaySelectNoneBtn = document.getElementById('parlaySelectNoneBtn');
  const parlayEventSelMeta = document.getElementById('parlayEventSelectionMeta');
  const parlayBuildWrap = document.getElementById('parlayBuildWrap');
  const parlayBuildBtn = document.getElementById('parlayBuildBtn');
  const parlayRebuildBtn = document.getElementById('parlayRebuildBtn');
  const parlayRescrapeBtn = document.getElementById('parlayRescrapeBtn');
  const parlayStatusMeta = document.getElementById('parlayStatusMeta');
  const parlayQuotaBar = document.getElementById('parlayQuotaBar');
  const parlayQuotaList = document.getElementById('parlayQuotaList');
  const parlayTicket = document.getElementById('parlayTicket');
  const parlayTicketOdds = document.getElementById('parlayTicketOdds');
  const parlayAllPropsWrap = document.getElementById('parlayAllPropsWrap');
  const parlayAllPropsTitle = document.getElementById('parlayAllPropsTitle');
  const parlayPropCount = document.getElementById('parlayPropCount');
  const parlayAllPropsBody = document.getElementById('parlayAllPropsBody');
  const parlayEmptyState = document.getElementById('parlayEmptyState');
  const parlayInjuryAware = document.getElementById('parlayInjuryAware');
  const parlayInjurySummaryEl = document.getElementById('parlayInjurySummary');

  if (!parlayLoadEventsBtn) return;

  // ── State ─────────────────────────────────────────────────────────────
  let allEvents = [];       // events returned by /api/odds/events
  let selectedEventIds = new Set();
  let cachedScoredProps = null;
  let cachedQuotaLog = null;
  let cachedScrapeMeta = null;

  // ── Persist / restore ─────────────────────────────────────────────────
  try {
    const s = JSON.parse(localStorage.getItem(PARLAY_SETTINGS_STORAGE) || '{}');
    if (s.sport && parlaySportSelect) parlaySportSelect.value = s.sport;
    if (s.odds_format && parlayOddsFormatSel) parlayOddsFormatSel.value = s.odds_format;
    if (s.legs && parlayLegsSelect) parlayLegsSelect.value = String(s.legs);
    if (s.last_n && parlayLastNSelect) parlayLastNSelect.value = String(s.last_n);
    if (s.bookmaker && parlayBookmakerSelect) parlayBookmakerSelect.value = s.bookmaker;
  } catch (e) { }

  function saveSettings() {
    try {
      localStorage.setItem(PARLAY_SETTINGS_STORAGE, JSON.stringify({
        sport: parlaySportSelect.value, odds_format: parlayOddsFormatSel.value,
        legs: parseInt(parlayLegsSelect.value), last_n: parseInt(parlayLastNSelect.value),
        bookmaker: parlayBookmakerSelect ? parlayBookmakerSelect.value : 'draftkings',
      }));
    } catch (e) { }
  }

  // ── Helpers ───────────────────────────────────────────────────────────
  function escHtml(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
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
    } catch (e) { return ''; }
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
    parlayEventList.innerHTML = events.map(function (ev) {
      const time = formatEventTime(ev.commence_time);
      return '<div class="parlay-event-chip" data-event-id="' + escHtml(ev.id) + '">' +
        '<span class="parlay-chip-check">✓</span>' +
        '<div>' +
        '<div class="parlay-chip-teams">' + escHtml(ev.away_team) + ' @ ' + escHtml(ev.home_team) + '</div>' +
        (time ? '<div class="parlay-chip-time">' + time + '</div>' : '') +
        '</div>' +
        '</div>';
    }).join('');

    parlayEventList.querySelectorAll('.parlay-event-chip').forEach(function (chip) {
      chip.addEventListener('click', function () {
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
    show(parlayRebuildBtn, hasCache);
    show(parlayRescrapeBtn, hasCache);
    if (parlayBuildBtn) parlayBuildBtn.style.display = hasCache ? 'none' : '';
  }

  // ── Render quota pills ─────────────────────────────────────────────────
  function renderQuota(log) {
    if (!log || !log.length) return;
    show(parlayQuotaBar, true);
    parlayQuotaList.innerHTML = log.map(function (e) {
      const q = e.quota || {};
      return '<span class="parlay-quota-pill"><strong>' + escHtml(e.call || '?') + '</strong>' +
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
    return Math.round(legs.reduce(function (a, l) { return a * l.odds; }, 1) * 100) / 100;
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
    grid.innerHTML = parlay.map(function (leg, i) {
      const sc = leg.side === 'OVER' ? 'over' : 'under';
      const fp = Math.min(100, leg.hit_rate || 0);
      const tone = getConfidenceTone(leg.confidence);
      const tags = Array.isArray(leg.confidence_tags) ? leg.confidence_tags.slice(0, 2) : [];
      const reasonParts = Array.isArray(leg.selection_reason_parts) ? leg.selection_reason_parts.slice(0, 3) : [];
      const trustDiagnostics = buildTrustDiagnostics({
        availability: leg.availability || null,
        environment: leg.environment || {},
        teamContext: leg.team_context || {},
        marketSide: leg.market_side,
        selectedSide: leg.side,
        injuryFilterNames: leg.injury_filter_player_names || [],
        teamInjuryNames: leg.team_injury_player_names || [],
        marketDisagrees: leg.market_disagrees,
      });
      const decisionLens = buildDecisionLensData({
        selectedSide: leg.side,
        marketSide: leg.market_side,
        hitRate: leg.hit_rate,
        average: leg.average,
        line: leg.line,
        confidenceSummary: leg.confidence_summary,
        modelProbability: leg.model_probability,
        impliedProbability: leg.implied_probability ?? leg.fair_probability,
        marketDisagrees: leg.market_disagrees,
        marketPenalty: leg.market_penalty,
        teamContext: leg.team_context || {},
        teamInjuryNames: leg.team_injury_player_names || [],
        environment: leg.environment || {},
      });
      const reasonHtml = reasonParts.length
        ? '<div class="parlay-leg-why"><span class="parlay-leg-why-label">Why selected</span><ul>' + reasonParts.map(function (part) { return '<li>' + escHtml(part) + '</li>'; }).join('') + '</ul></div>'
        : '';
      return '<div class="parlay-leg-card" data-leg-idx="' + i + '" title="Analyze ' + escHtml(leg.player_name) + '" style="cursor:pointer">' +
        '<span class="parlay-leg-rank">#' + (i + 1) + '</span>' +
        '<div class="parlay-leg-player">' + escHtml(leg.player_name) + '</div>' +
        '<div class="parlay-leg-market">' +
        '<span class="parlay-leg-stat">' + escHtml(leg.stat) + '</span>' +
        '<span class="parlay-leg-line">' + leg.line + '</span>' +
        '<span class="parlay-leg-side ' + sc + '">' + leg.side + '</span>' +
        '</div>' +
        '<div style="display:flex;gap:6px;flex-wrap:wrap;margin:6px 0 2px">' +
        '<span class="finder-badge ' + tone + '">' + escHtml(leg.confidence || 'C') + ' ' + escHtml(String(leg.confidence_score || '')) + '</span>' +
        (leg.confidence_tier ? '<span class="finder-badge">' + escHtml(leg.confidence_tier) + '</span>' : '') +
        (leg.injury_boost ? '<span class="parlay-leg-inj-boost">🏥 +' + Math.round((leg.hit_rate || 0) - (leg.base_hit_rate || leg.hit_rate || 0)) + '% context edge</span>' : '') +
        tags.map(function (tag) { return '<span class="finder-chip">' + escHtml(tag) + '</span>'; }).join('') +
        '</div>' +
        (
          leg.injury_filter_player_names && leg.injury_filter_player_names.length
            ? '<div class="parlay-leg-inj-context">w/o ' + leg.injury_filter_player_names.map(escHtml).join(', ') + '</div>'
            : (
              leg.team_injury_player_names && leg.team_injury_player_names.length
                ? '<div class="parlay-leg-inj-context">' + leg.team_injury_player_names.slice(0, 3).map(escHtml).join(', ') + (leg.team_injury_player_names.length > 3 ? ' +' + (leg.team_injury_player_names.length - 3) : '') + '</div>'
                : ''
            )
        ) +
        renderTrustDiagnostics(trustDiagnostics, true) +
        '<div class="parlay-leg-stats-row">' +
        '<div class="parlay-leg-stat-item"><span class="slabel">Hit %</span><span class="sval">' + leg.hit_rate + '%</span></div>' +
        '<div class="parlay-leg-stat-item"><span class="slabel">Avg</span><span class="sval">' + (leg.average || '—') + '</span></div>' +
        '<div class="parlay-leg-stat-item"><span class="slabel">Games</span><span class="sval">' + (leg.games_count || '—') + '</span></div>' +
        '<div class="parlay-leg-stat-item"><span class="slabel">Odds</span><span class="sval">' + fmtOdds(leg.odds) + '</span></div>' +
        '</div>' +
        '<div class="parlay-leg-hit-bar"><div class="parlay-leg-hit-fill" style="width:' + fp + '%"></div></div>' +
        reasonHtml +
        renderDecisionLensHtml(decisionLens, 'compact') +
        '<div class="parlay-leg-analyze-hint" style="font-size:0.72rem;color:var(--muted);margin-top:6px;text-align:center">' + escHtml(leg.confidence_summary || 'Click to analyze →') + '</div>' +
        '</div>';
    }).join('');

    // Wire up click → analyzer with full auto-populate
    grid.querySelectorAll('.parlay-leg-card[data-leg-idx]').forEach(function (card) {
      card.addEventListener('click', async function () {
        const leg = parlay[parseInt(card.dataset.legIdx)];
        if (!leg || !leg.player_id) return;
        try {
          await hydrateAnalyzerFromPropSelection(leg);
        } catch (err) { console.warn('Parlay ticket leg nav failed:', err); }
      });
    });
  }

  // ── Render all props table (clickable → analyzer) ─────────────────────
  function renderAllProps(all, selectedIds) {
    if (!all || !all.length) return;
    show(parlayAllPropsWrap, true);
    if (parlayPropCount) parlayPropCount.textContent = all.length + ' props — click any row to analyze';
    if (parlayAllPropsTitle) parlayAllPropsTitle.textContent = all.length + ' props ranked by hit rate';
    const sel = new Set((selectedIds || []).map(Number));
    parlayAllPropsBody.innerHTML = all.map(function (p, idx) {
      const isSel = sel.has(Number(p.player_id));
      const imgSrc = 'https://cdn.nba.com/headshots/nba/latest/1040x760/' + (p.player_id || '') + '.png';
      const gameTag = p.game_label ? '<span class="parlay-game-label">' + escHtml(p.game_label) + '</span>' : '';
      const tone = getConfidenceTone(p.confidence);
      const statusLabel = p.selection_status === 'selected' ? 'Why selected' : 'Why not selected';
      const reasonText = p.selection_reason || '';
      const confText = p.confidence ? ('<div class="parlay-conf-cell"><span class="finder-badge ' + tone + '">' + escHtml(p.confidence) + ' ' + escHtml(String(p.confidence_score || '')) + '</span><small>' + escHtml(p.confidence_summary || '') + '</small>' + (reasonText ? '<small class="parlay-selection-note"><strong>' + escHtml(statusLabel) + ':</strong> ' + escHtml(reasonText) + '</small>' : '') + '</div>') : '<span style="opacity:0.35">—</span>';
      return '<tr class="' + (isSel ? 'parlay-selected-row' : '') + '" data-prop-idx="' + idx + '" data-player-id="' + (p.player_id || '') + '" data-player-name="' + escHtml(p.player_name || '') + '" data-stat="' + escHtml(p.stat || '') + '" data-line="' + (p.line || 0) + '" data-side="' + escHtml(p.side || 'OVER') + '" title="Analyze ' + escHtml(p.player_name) + '">' +
        '<td><span style="display:flex;align-items:center;gap:8px"><img src="' + imgSrc + '" style="width:26px;height:26px;border-radius:50%;object-fit:cover;object-position:top" onerror="this.hidden=true">' +
        '<span style="display:flex;flex-direction:column;gap:2px">' +
        '<span>' + escHtml(p.player_name) + (isSel ? ' ⭐' : '') + '</span>' + gameTag +
        '</span></span></td>' +
        '<td>' + escHtml(p.stat) + '</td><td>' + p.line + '</td>' +
        '<td class="' + (p.side === 'OVER' ? 'side-over' : 'side-under') + '">' + p.side + '</td>' +
        '<td class="hit-pct-cell ' + hitClass(p.hit_rate) + '">' + p.hit_rate + '%</td>' +
        '<td>' + (p.average || '—') + '</td><td>' + (p.games_count || '—') + '</td>' +
        '<td>' + confText + '</td>' +
        '<td><div class="parlay-row-micro"><span>EV <strong>' + (p.ev ?? '—') + '%</strong></span><span>Edge <strong>' + (p.edge ?? '—') + '%</strong></span><span>Sample <strong>' + (p.hit_count || 0) + '/' + (p.games_count || 0) + '</strong></span></div></td>' +
        '<td>' + fmtOdds(p.odds) + '</td>' +
        '<td><div class="parlay-inj-cell">' + renderInjuryCell(p) + '</div></td>' +
        '<td><div class="parlay-action-cell"><button class="parlay-track-btn" data-track-idx="' + idx + '">+ Track</button></div></td>' +
        '</tr>';
    }).join('');
    parlayAllPropsBody.querySelectorAll('tr[data-prop-idx]').forEach(function (row) {
      row.addEventListener('click', async function (e) {
        if (e.target.closest('.parlay-track-btn')) return;
        const prop = all[parseInt(row.dataset.propIdx)];
        if (!prop || !prop.player_id) return;
        try {
          await hydrateAnalyzerFromPropSelection(prop);
        } catch (e) { console.warn('Parlay row nav failed:', e); }
      });
    });
    parlayAllPropsBody.querySelectorAll('.parlay-track-btn').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
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
    const n = parseInt(parlayLegsSelect.value) || 3;
    const legs = pickLegs(cachedScoredProps, n);
    const odds = calcOdds(legs);
    renderTicket(legs, n, odds);
    renderAllProps(cachedScoredProps, legs.map(function (l) { return l.player_id; }));
    const m = cachedScrapeMeta || {};
    const ago = m.scrapedAt ? ' · cached ' + Math.round((Date.now() - m.scrapedAt) / 60000) + 'm ago' : '';
    setStatus((m.evCount || 0) + ' events · ' + (m.analyzed || 0) + ' props analyzed' + ago + ' — no credits used', false);
  }

  // ── Render injury summary panel ──────────────────────────────────────
  function renderInjurySummary(injurySummary) {
    if (!parlayInjurySummaryEl) return;
    if (!injurySummary || !injurySummary.length) {
      parlayInjurySummaryEl.style.display = 'none';
      return;
    }
    parlayInjurySummaryEl.style.display = '';
    parlayInjurySummaryEl.innerHTML =
      '<div class="parlay-inj-summary-label">🏥 Today\'s lineup context used in analysis</div>' +
      injurySummary.map(function(t) {
        return '<div class="parlay-inj-team-chip">' +
          '<span class="inj-team-name">' + escHtml(t.team_name) + '</span>' +
          '<span class="inj-player-list">' + (t.injured_player_names || []).map(escHtml).join('<br>') + '</span>' +
          '</div>';
      }).join('');
  }

  // ── Render single injury cell for all-props table ─────────────────────
  function renderInjuryCell(p) {
    if (!p) return '<span style="opacity:0.3">—</span>';
    const teamInjured = p.team_injury_player_names || [];
    const filterUsed = p.injury_filter_player_names || [];
    const boost = p.injury_boost;
    if (!teamInjured.length && !filterUsed.length) return '<span class="inj-names" style="opacity:0.55">No lineup filter used</span>';
    var parts = [];
    if (boost) {
      const diff = Math.round((p.hit_rate || 0) - (p.base_hit_rate || p.hit_rate || 0));
      parts.push('<span class="inj-boost-tag">🏥' + (diff > 0 ? ' +' + diff + '%' : '') + ' context</span>');
    }
    if (filterUsed.length) {
      parts.push('<span class="inj-names">w/o ' + filterUsed.map(escHtml).join(', ') + '</span>');
    } else if (teamInjured.length) {
      const shown = teamInjured.slice(0, 2).map(escHtml).join(', ');
      const extra = teamInjured.length > 2 ? ' <span style="opacity:0.5">+' + (teamInjured.length - 2) + '</span>' : '';
      parts.push('<span class="inj-names">' + shown + extra + '</span>');
    }
    return parts.length ? parts.join('') : '<span style="opacity:0.3">—</span>';
  }

  // ── Full scrape selected events → analyze → render ───────────────────
  async function runScrape() {
    let keyEntries = [];
    try {
      keyEntries = await getRotatingVaultKeysForFeature({
        minimumKeys: KEY_VAULT_MIN_ROTATING_KEYS,
        requiredCredits: 1,
        sourceLabel: 'Parlay Builder',
      });
    } catch (error) {
      setStatus(error.message || 'No usable rotating keys available.', true);
      return;
    }
    const keys = keyEntries.map(function (entry) { return entry.key; });
    if (selectedEventIds.size === 0) { setStatus('Select at least one event first.', true); return; }
    const legs = parseInt(parlayLegsSelect.value) || 2;
    const sport = parlaySportSelect.value;
    const oddsFormat = parlayOddsFormatSel.value;
    const lastN = parseInt(parlayLastNSelect.value) || 10;

    saveSettings();
    show(parlayTicket, false); show(parlayAllPropsWrap, false);
    show(parlayQuotaBar, false); show(parlayEmptyState, false);
    if (parlayInjurySummaryEl) parlayInjurySummaryEl.style.display = 'none';
    parlayQuotaList.innerHTML = '';
    const useInjuryAware = parlayInjuryAware && parlayInjuryAware.checked;
    const parlayEndpoint = useInjuryAware ? '/api/parlay-builder-injury-aware' : '/api/parlay-builder';
    const parlayAsyncEndpoint = useInjuryAware ? '/api/parlay-builder-injury-aware/async' : '/api/parlay-builder/async';
    const parlayStreamEndpoint = useInjuryAware ? '/api/parlay-builder-injury-aware/stream' : '/api/parlay-builder/stream';
    const totalEvents = selectedEventIds.size;
    setStatus('⏳ Step 1/3 — Fetching odds for ' + totalEvents + ' game(s)…', false);
    setParlayProgress(5, 'Tip-off: connecting to the odds board…');
    if (parlayBuildBtn) parlayBuildBtn.disabled = true;
    if (parlayRebuildBtn) parlayRebuildBtn.disabled = true;
    if (parlayRescrapeBtn) parlayRescrapeBtn.disabled = true;

    try {
      const inputPayload = {
        api_keys: keys, legs: legs, sport: sport,
        odds_format: oddsFormat, last_n: lastN,
        event_ids: Array.from(selectedEventIds),
        bookmaker: parlayBookmakerSelect ? parlayBookmakerSelect.value : 'draftkings',
      };
      const updateProgress = (msg) => {
        if (msg?.type !== 'progress') return;
        let pct = 5;
        let label = 'Processing parlay…';
        if (msg.stage === 'events_resolved') {
          pct = 12;
          label = 'Events loaded · ' + (msg.events || totalEvents) + ' games';
        } else if (msg.stage === 'scrape_progress') {
          const batch = msg.batch || 1;
          const batches = msg.batches || 1;
          pct = 15 + Math.round((batch / batches) * 30);
          label = 'Scraping odds · batch ' + batch + ' of ' + batches;
        } else if (msg.stage === 'analysis_start') {
          pct = 55;
          label = 'Analyzing props · ' + (msg.total || 0) + ' rows';
        } else if (msg.stage === 'analysis_progress') {
          const done = msg.done || 0;
          const total = msg.total || 1;
          pct = 55 + Math.round((done / total) * 25);
          label = 'Analyzing ' + done + '/' + total + ' props';
        } else if (msg.stage === 'analysis_done') {
          pct = 82;
          label = 'Analysis complete · scoring edges';
        } else if (msg.stage === 'scoring_progress') {
          const done = msg.done || 0;
          const total = msg.total || 1;
          pct = 82 + Math.round((done / total) * 16);
          label = 'Scoring ' + done + '/' + total;
        } else if (msg.stage === 'done') {
          pct = 100;
          label = 'Buzzer beater: parlay ready.';
        }
        setParlayProgress(Math.min(100, Math.max(5, pct)), label);
        setStatus(label, false);
      };
      let data = null;
      try {
        const streamResult = await streamNdjson(parlayStreamEndpoint, inputPayload, updateProgress);
        if (streamResult?.type === 'error') throw new Error(streamResult.message || 'Server error');
        data = streamResult?.payload || null;
      } catch (streamError) {
        data = await runAsyncJob(parlayAsyncEndpoint, inputPayload, (status) => {
          const label = status.status === 'running' ? 'Analyzing parlay...' : 'Queued parlay...';
          setParlayProgress(60, label);
          setStatus(label, false);
        });
      }

      cachedScoredProps = data.all_props_scored || [];
      cachedQuotaLog = data.quota_log || [];
      cachedScrapeMeta = { evCount: data.events_scraped || 0, propCount: data.props_found || 0, analyzed: data.props_analyzed || 0, scrapedAt: Date.now() };
      setParlayProgress(95, 'Fourth quarter: building your ticket…');
      renderQuota(cachedQuotaLog);
      const m = cachedScrapeMeta;
      const injBoostCount = (cachedScoredProps || []).filter(function(p){ return p.injury_boost; }).length;
      const injStatusMsg = useInjuryAware ? (' · 🏥 ' + injBoostCount + ' lineup-context edge' + (injBoostCount !== 1 ? 's' : '')) : '';
      setStatus('✓ Done — ' + m.evCount + ' event(s) scraped · ' + m.propCount + ' props found · ' + m.analyzed + ' analyzed' + injStatusMsg, false);
      // Render injury summary panel
      if (useInjuryAware && parlayInjurySummaryEl) renderInjurySummary(data.injury_summary || []);
      setParlayProgress(100, 'Buzzer beater: parlay ready.');
      setTimeout(function () { setParlayProgress(0, ''); }, 2000);

      if (!cachedScoredProps.length) {
        show(parlayEmptyState, true);
        const s = parlayEmptyState.querySelector('strong'), sp = parlayEmptyState.querySelector('span');
        if (s) s.textContent = data.message || 'No props found for selected events.';
        if (sp) sp.textContent = 'Try different events or check your API keys.';
        if (typeof hideBanner === 'function') hideBanner();
        return;
      }
      showCacheButtons(true);
      rebuildFromCache();
      if (typeof hideBanner === 'function') hideBanner();
    } catch (err) {
      setParlayProgress(0, '');
      setStatus('❌ Error: ' + (err.message || 'Unknown'), true);
      show(parlayEmptyState, true);
    } finally {
      if (parlayBuildBtn) parlayBuildBtn.disabled = false;
      if (parlayRebuildBtn) parlayRebuildBtn.disabled = false;
      if (parlayRescrapeBtn) parlayRescrapeBtn.disabled = false;
    }
  }

  // ── Step 1: Load today's events ───────────────────────────────────────
  parlayLoadEventsBtn.addEventListener('click', async function () {
    let keyEntries = [];
    try {
      keyEntries = await getRotatingVaultKeysForFeature({
        minimumKeys: KEY_VAULT_MIN_ROTATING_KEYS,
        requiredCredits: 1,
        sourceLabel: 'Parlay event loading',
      });
    } catch (error) {
      setStatus(error.message || 'No usable rotating keys available.', true);
      return;
    }
    const keys = keyEntries.map(function (entry) { return entry.key; });
    saveSettings();
    parlayLoadEventsBtn.disabled = true;
    parlayLoadEventsBtn.textContent = 'Loading…';
    show(parlayEventPickerWrap, false);
    show(parlayBuildWrap, false);

    try {
      const data = await apiFetch('/api/odds/events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ api_key: keys[0], sport: parlaySportSelect.value }),
      }, 15000);
      allEvents = (data.events || []).filter(function (e) { return e.id && e.home_team; });

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
    } catch (err) {
      setStatus('Error loading events: ' + err.message, true);
    } finally {
      parlayLoadEventsBtn.disabled = false;
      parlayLoadEventsBtn.textContent = "Load Today's Events";
    }
  });

  // Select all / none
  if (parlaySelectAllBtn) {
    parlaySelectAllBtn.addEventListener('click', function () {
      parlayEventList.querySelectorAll('.parlay-event-chip').forEach(function (c) {
        selectedEventIds.add(c.dataset.eventId);
        c.classList.add('selected');
      });
      syncLegsToEventCount();
    });
  }
  if (parlaySelectNoneBtn) {
    parlaySelectNoneBtn.addEventListener('click', function () {
      selectedEventIds.clear();
      parlayEventList.querySelectorAll('.parlay-event-chip').forEach(function (c) { c.classList.remove('selected'); });
      syncLegsToEventCount();
    });
  }

  // Scrape & Build
  if (parlayBuildBtn) parlayBuildBtn.addEventListener('click', function () { runScrape(); });

  // Reshuffle (re-pick from cache, zero credits)
  if (parlayRebuildBtn) {
    parlayRebuildBtn.addEventListener('click', function () {
      if (!cachedScoredProps || !cachedScoredProps.length) { setStatus('No cached data. Run Scrape & Build first.', true); return; }
      // Rotate the cached props so a different combination surfaces
      if (cachedScoredProps.length > 1) cachedScoredProps.push(cachedScoredProps.shift());
      rebuildFromCache();
    });
  }

  // Re-scrape (clear cache, burn credits)
  if (parlayRescrapeBtn) {
    parlayRescrapeBtn.addEventListener('click', function () {
      cachedScoredProps = null; cachedQuotaLog = null; cachedScrapeMeta = null;
      showCacheButtons(false);
      runScrape();
    });
  }

  // Leg change → instant local rebuild
  if (parlayLegsSelect) {
    parlayLegsSelect.addEventListener('change', function () {
      if (cachedScoredProps && cachedScoredProps.length) rebuildFromCache();
    });
  }

})();

/* ═══════════════════════════════════════════════════════════════════════
   PROP TRACKER
═══════════════════════════════════════════════════════════════════════ */
(function initPropTracker() {

  const trackerPlayerInput = document.getElementById('trackerPlayerInput');
  const trackerStatSelect = document.getElementById('trackerStatSelect');
  const trackerLineInput = document.getElementById('trackerLineInput');
  const trackerSideSelect = document.getElementById('trackerSideSelect');
  const trackerOddsInput = document.getElementById('trackerOddsInput');
  const trackerBookInput = document.getElementById('trackerBookInput');
  const trackerAddBtn = document.getElementById('trackerAddBtn');
  const trackerAddError = document.getElementById('trackerAddError');
  const trackerRefreshBtn = document.getElementById('trackerRefreshBtn');
  const trackerCards = document.getElementById('trackerCards');
  const trackerEmpty = document.getElementById('trackerEmpty');
  const trackerSortSelect = document.getElementById('trackerSortSelect');
  const trackerGroupSelect = document.getElementById('trackerGroupSelect');
  const trackerFilterSelect = document.getElementById('trackerFilterSelect');
  const trackerBoardSummary = document.getElementById('trackerBoardSummary');

  if (!trackerAddBtn) return;

  // ── Autocomplete dropdown for player input ────────────────────────────
  const trackerPlayerDropdown = document.getElementById('trackerPlayerDropdown');
  let _autocompleteDebounce = null;
  let _selectedPlayerName = '';
  let _selectedPlayerId = null;

  function mountTrackerDropdownToBody() {
    if (trackerPlayerDropdown && trackerPlayerDropdown.parentElement !== document.body) {
      document.body.appendChild(trackerPlayerDropdown);
    }
  }

  function positionTrackerDropdown() {
    if (!trackerPlayerInput || !trackerPlayerDropdown) return;
    const rect = trackerPlayerInput.getBoundingClientRect();
    trackerPlayerDropdown.style.position = 'fixed';
    trackerPlayerDropdown.style.top = (rect.bottom + 4) + 'px';
    trackerPlayerDropdown.style.left = rect.left + 'px';
    trackerPlayerDropdown.style.width = rect.width + 'px';
  }

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
    mountTrackerDropdownToBody();
    positionTrackerDropdown();
    trackerPlayerDropdown.innerHTML = results.map(function (p) {
      return '<li role="option" class="tracker-player-option" data-id="' + p.id + '" data-name="' + esc(p.full_name) + '" style="padding:8px 12px;cursor:pointer;list-style:none;border-bottom:1px solid var(--border,#333)">'
        + esc(p.full_name) + (p.is_active ? '' : ' <small style="opacity:.5">(inactive)</small>') + '</li>';
    }).join('');
    trackerPlayerDropdown.style.display = 'block';
    if (trackerPlayerInput) trackerPlayerInput.setAttribute('aria-expanded', 'true');

    trackerPlayerDropdown.querySelectorAll('.tracker-player-option').forEach(function (li) {
      li.addEventListener('mousedown', function (e) {
        e.preventDefault(); // prevent blur before click
        _selectedPlayerId = li.dataset.id ? Number(li.dataset.id) : null;
        _selectedPlayerName = li.dataset.name || '';
        trackerPlayerInput.value = _selectedPlayerName;
        closeDropdown();
      });
    });
  }

  if (trackerPlayerInput) {
    trackerPlayerInput.addEventListener('input', function () {
      const q = trackerPlayerInput.value.trim();
      _selectedPlayerId = null; // reset cached selection on new typing
      clearTimeout(_autocompleteDebounce);
      if (q.length < 2) { closeDropdown(); return; }
      _autocompleteDebounce = setTimeout(async function () {
        try {
          const data = await apiFetch('/api/players/search?q=' + encodeURIComponent(q), {}, 6000);
          openDropdown((data.results || []).slice(0, 8));
        } catch (e) { closeDropdown(); }
      }, 220);
    });

    trackerPlayerInput.addEventListener('blur', function () {
      // Slight delay so mousedown on option fires first
      setTimeout(closeDropdown, 200);
    });

    trackerPlayerInput.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') closeDropdown();
    });
    window.addEventListener('resize', positionTrackerDropdown);
    window.addEventListener('scroll', positionTrackerDropdown, true);
  }

  // ── State ─────────────────────────────────────────────────────────────
  // bet = { id, player_name, player_id, stat, line, side, odds, book,
  //         current_val, games_count, last_updated, status }
  let bets = [];
  let trackerPersistPromise = Promise.resolve();
  let trackerSortMode = 'closest';
  let trackerGroupMode = 'game';
  let trackerFilterMode = 'all';

  // ── Persist ───────────────────────────────────────────────────────────
  function saveBets() {
    const snapshot = Array.isArray(bets) ? bets.map(function (bet) { return { ...bet }; }) : [];
    trackerPersistPromise = trackerPersistPromise
      .catch(function () { })
      .then(async function () {
        await apiFetch('/api/tracker/props', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ entries: snapshot }),
        }, 8000);
      })
      .catch(function (error) {
        console.error('Tracker persistence failed:', error);
      });
    return trackerPersistPromise;
  }
  async function loadBets() {
    const data = await apiFetch('/api/tracker/props', {}, 8000);
    return Array.isArray(data.entries) ? data.entries : [];
  }

  // ── Helpers ───────────────────────────────────────────────────────────
  function esc(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
  function escapeHtmlT(s) { return esc(s); }
  function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2, 6); }
  function showErr(msg) {
    if (!trackerAddError) return;
    trackerAddError.textContent = msg;
    trackerAddError.style.display = msg ? '' : 'none';
  }
  function showEmpty(v) {
    if (trackerEmpty) trackerEmpty.style.display = v ? '' : 'none';
    if (trackerCards) trackerCards.style.display = v ? 'none' : '';
  }

  function isDuplicateBet(candidate, existing) {
    const idMatch = candidate.player_id && existing.player_id
      ? String(existing.player_id) === String(candidate.player_id)
      : (existing.player_name || '').toLowerCase() === String(candidate.player_name || '').toLowerCase();
    return idMatch &&
      existing.stat === candidate.stat &&
      parseFloat(existing.line) === parseFloat(candidate.line) &&
      existing.side === candidate.side;
  }

  // ── Compute status from current val vs line ────────────────────────────
  function computeStatus(bet) {
    const val = bet.current_val;
    const line = parseFloat(bet.line);
    if (val === null || val === undefined) return 'pending';
    const v = parseFloat(val);
    const isFinal = bet.source === 'FINAL' || bet.game_status === 'final';
    const isLive = bet.source === 'LIVE' || bet.game_status === 'live';
    if (isFinal) {
      if (bet.side === 'OVER') return v >= line ? 'hit' : 'busted';
      if (bet.side === 'UNDER') return v <= line ? 'hit' : 'busted';
    }
    if (bet.side === 'OVER') return v >= line ? 'hit' : v >= line * 0.85 ? 'close' : 'progress';
    if (bet.side === 'UNDER') {
      // During live games, an UNDER can't be confirmed — player can still score more.
      // Only mark HIT when the game is finished (FINAL source).
      if (isLive) return v > line ? 'busted' : v > line * 0.85 ? 'close' : 'progress';
      return v <= line ? 'hit' : v <= line * 1.15 ? 'close' : 'busted';
    }
    return 'progress';
  }

  // For UNDER, the bar fills inversely: more value = worse
  function computeBarPct(bet) {
    const val = bet.current_val;
    const line = parseFloat(bet.line);
    if (val === null || val === undefined) return 0;
    const v = parseFloat(val);
    if (bet.side === 'OVER') {
      return Math.min(100, Math.round((v / line) * 100));
    } else {
      // UNDER: bar reflects proximity to the line (closer to the line = higher %).
      if (!Number.isFinite(line) || line <= 0) return 0;
      if (v <= line) {
        return Math.min(100, Math.round((v / line) * 100));
      }
      const overPct = Math.max(0, 100 - Math.round(((v - line) / line) * 100));
      return Math.min(100, overPct);
    }
  }

  function computeTrackerDistance(bet) {
    const val = Number(bet.current_val);
    const line = Number(bet.line);
    if (!Number.isFinite(line) || line <= 0 || !Number.isFinite(val)) return null;
    return bet.side === 'OVER' ? line - val : val - line;
  }

  function getTrackerMovement(bet) {
    const history = Array.isArray(bet.history) ? bet.history : [];
    if (history.length < 2) return null;
    const prev = Number(history[history.length - 2]?.value);
    const curr = Number(history[history.length - 1]?.value);
    if (!Number.isFinite(prev) || !Number.isFinite(curr)) return null;
    return Number((curr - prev).toFixed(1));
  }

  function getTrackerHistoryLabels(bet) {
    const history = Array.isArray(bet.history) ? bet.history.slice(-5) : [];
    if (!history.length) return '';
    return history.map(point => `<span class="tracker-history-pill">${Number(point.value).toFixed(1)}</span>`).join('');
  }

  function getTrackerGroupKey(bet) {
    if (trackerGroupMode === 'status') return String(computeStatus(bet) || 'pending').toUpperCase();
    if (trackerGroupMode === 'none') return 'All tracked props';
    return bet.game_label || (bet.source === 'NO GAME TODAY' ? 'No game today' : 'Upcoming slate');
  }

  function formatTrackerGroupLabel(key) {
    if (trackerGroupMode === 'status') {
      const labels = {
        HIT: 'Hit',
        BUSTED: 'Busted',
        CLOSE: 'Close calls',
        PROGRESS: 'In progress',
        PENDING: 'Pending',
      };
      return labels[key] || key;
    }
    return key;
  }

  function passesTrackerFilter(bet) {
    const status = computeStatus(bet);
    if (trackerFilterMode === 'all') return true;
    if (trackerFilterMode === 'live') return bet.source === 'LIVE' || bet.game_status === 'live';
    if (trackerFilterMode === 'active') return status === 'progress' || status === 'close';
    return status === trackerFilterMode;
  }

  function compareTrackerBets(a, b) {
    const distA = computeTrackerDistance(a);
    const distB = computeTrackerDistance(b);
    const updatedA = Date.parse(a.last_updated || 0) || 0;
    const updatedB = Date.parse(b.last_updated || 0) || 0;
    const liveRankA = a.source === 'LIVE' ? 0 : a.source === 'UPCOMING' ? 1 : 2;
    const liveRankB = b.source === 'LIVE' ? 0 : b.source === 'UPCOMING' ? 1 : 2;
    if (trackerSortMode === 'danger') {
      return (distB ?? -9999) - (distA ?? -9999);
    }
    if (trackerSortMode === 'live') {
      return liveRankA - liveRankB || updatedB - updatedA;
    }
    if (trackerSortMode === 'recent') {
      return updatedB - updatedA;
    }
    if (trackerSortMode === 'player') {
      return String(a.player_name || '').localeCompare(String(b.player_name || ''));
    }
    return (distA ?? 9999) - (distB ?? 9999);
  }

  // ── Render a single card ───────────────────────────────────────────────
  function renderCard(bet) {
    const status = computeStatus(bet);
    const barPct = computeBarPct(bet);
    const val = bet.current_val !== null && bet.current_val !== undefined
      ? parseFloat(bet.current_val).toFixed(1) : '—';
    const line = parseFloat(bet.line).toFixed(1);
    const imgSrc = bet.player_id
      ? 'https://cdn.nba.com/headshots/nba/latest/1040x760/' + bet.player_id + '.png'
      : '';

    // Card gets extra class when injured — must be declared before cardClass uses it
    const extraCardCls = bet.is_injured ? ' injured' : '';

    // Card-level class
    const cardClass = (status === 'hit' ? 'tracker-card hit'
      : status === 'busted' ? 'tracker-card busted'
        : 'tracker-card') + extraCardCls;

    // Bar track class
    const barClass = status === 'hit' ? 'tracker-bar-track hit'
      : status === 'close' ? 'tracker-bar-track close'
        : status === 'busted' ? 'tracker-bar-track busted'
          : status === 'pending' ? 'tracker-bar-track pending'
            : 'tracker-bar-track progress';

    // Status badge
    const badgeLabel = status === 'hit' ? '✓ HIT'
      : status === 'busted' ? '✗ BUST'
        : status === 'close' ? 'CLOSE'
          : status === 'pending' ? 'PENDING'
            : 'IN PROGRESS';
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
      : src === 'FINAL' ? '<span class="tracker-source-pill final">FINAL</span>'
        : src === 'UPCOMING' ? '<span class="tracker-source-pill upcoming">UPCOMING</span>'
          : src === 'NO GAME TODAY' ? '<span class="tracker-source-pill nogame">NO GAME</span>'
            : '';

    // Period/clock
    const gameCtx = bet.game_label
      ? esc(bet.game_label) + (bet.game_status === 'live' && bet.period ? ' · ' + esc(bet.period) + (bet.clock ? ' ' + esc(bet.clock) : '') : '')
      : '';

    const sideClass = bet.side === 'OVER' ? 'over' : 'under';
    const lastUpd = bet.last_updated
      ? new Date(bet.last_updated).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
      : '—';

    // Line tick position — where the line marker sits on the bar
    const tickPct = bet.side === 'OVER' ? 100 : Math.round(((parseFloat(bet.line)) / (parseFloat(bet.line) * 1.5)) * 100);
    const distance = computeTrackerDistance(bet);
    const movement = getTrackerMovement(bet);
    const moveClass = movement > 0 ? 'up' : movement < 0 ? 'down' : 'flat';
    const moveLabel = movement === null ? '' : `${movement > 0 ? '+' : ''}${movement.toFixed(1)} last refresh`;
    const cashLabel = distance === null
      ? 'Awaiting game data'
      : bet.side === 'OVER'
        ? (distance <= 0 ? `${Math.abs(distance).toFixed(1)} over` : `${distance.toFixed(1)} away`)
        : (distance <= 0 ? `${Math.abs(distance).toFixed(1)} under` : `${distance.toFixed(1)} above`);

    const stampHtml = (status === 'hit' || status === 'busted')
      ? `<span class="tracker-stamp ${status === 'hit' ? 'hit' : 'busted'}">${status === 'hit' ? 'WIN' : 'LOSE'}</span>`
      : '';

    return `
<div class="${cardClass}" data-bet-id="${esc(bet.id)}" id="tracker-card-${esc(bet.id)}">
  <div class="tracker-card-inner">
    ${stampHtml}
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

      <div class="tracker-signal-row">
        <span class="tracker-distance-pill">${esc(cashLabel)}</span>
        ${moveLabel ? `<span class="tracker-move-pill ${moveClass}">${esc(moveLabel)}</span>` : ''}
      </div>

      <div class="tracker-card-meta">
        <div class="tracker-meta-item"><span class="ml">Odds</span><span class="mv">${parseFloat(bet.odds || 1.91).toFixed(2)}</span></div>
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
    const visibleBets = bets.filter(passesTrackerFilter).sort(compareTrackerBets);
    if (!visibleBets.length) { showEmpty(true); return; }
    showEmpty(false);
    const grouped = new Map();
    visibleBets.forEach(function (bet) {
      const key = getTrackerGroupKey(bet);
      if (!grouped.has(key)) grouped.set(key, []);
      grouped.get(key).push(bet);
    });
    if (trackerBoardSummary) {
      const liveCount = visibleBets.filter(b => b.source === 'LIVE' || b.game_status === 'live').length;
      const closeCount = visibleBets.filter(b => computeStatus(b) === 'close').length;
      trackerBoardSummary.textContent = `${visibleBets.length} tracked • ${liveCount} live • ${closeCount} close to cash`;
    }
    trackerCards.innerHTML = Array.from(grouped.entries()).map(function ([key, items]) {
      const heading = formatTrackerGroupLabel(key);
      return `
        <section class="tracker-group">
          ${trackerGroupMode === 'none' ? '' : `<div class="tracker-group-head"><strong>${esc(heading)}</strong><span>${items.length} props</span></div>`}
          <div class="tracker-group-cards">
            ${items.map(renderCard).join('')}
          </div>
        </section>
      `;
    }).join('');
    // Wire remove buttons
    trackerCards.querySelectorAll('[data-remove-id]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        const id = btn.dataset.removeId;
        bets = bets.filter(function (b) { return b.id !== id; });
        saveBets();
        renderAll();
      });
    });
    // Trigger bar animation on next frame (bars start at 0 via CSS, then widen)
    requestAnimationFrame(function () {
      trackerCards.querySelectorAll('.tracker-bar-track').forEach(function (bar) {
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
      bet.is_injured = d.is_injured || false;
      bet.injury_status = d.injury_status || '';
      if (d.game_status === 'no_game' && bet.game_status === 'final') {
        return;
      }
      const gs = d.game_status;
      if ((gs === 'live' || gs === 'final') && d.live_val !== null && d.live_val !== undefined) {
        bet.history = Array.isArray(bet.history) ? bet.history : [];
        if (!bet.history.length || Number(bet.history[bet.history.length - 1].value) !== Number(d.live_val)) {
          bet.history.push({ value: Number(d.live_val), ts: new Date().toISOString() });
          bet.history = bet.history.slice(-6);
        }
        bet.current_val = d.live_val;
        bet.game_status = gs;
        bet.game_label = d.game_label || '';
        bet.period = d.period || '';
        bet.clock = d.clock || '';
        bet.source = gs === 'live' ? 'LIVE' : 'FINAL';
        bet.last_updated = new Date().toISOString();
        return;
      }
      if (gs === 'scheduled') {
        bet.game_status = 'scheduled';
        bet.game_label = d.game_label || '';
        bet.source = 'UPCOMING';
        bet.current_val = null;
        bet.last_updated = new Date().toISOString();
        return;
      }
      bet.game_status = 'no_game';
      bet.source = 'NO GAME TODAY';
      bet.last_updated = new Date().toISOString();
    } catch (e) {
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
      const timer = setTimeout(function () { ctrl.abort(); }, 4000);
      const resp = await fetch('/api/players/search?q=' + encodeURIComponent(name), { signal: ctrl.signal });
      clearTimeout(timer);
      if (!resp.ok) return null;
      const data = await resp.json();
      const results = data.results || [];
      if (Array.isArray(results) && results.length) return results[0].id || null;
    } catch (e) { }
    return null;
  }

  // ── Add prop from parlay to tracker ────────────────────────────────────
  // Returns true if actually added, false if duplicate or invalid.
  function addPropToTracker(prop) {
    if (!prop || !prop.player_name) return false;
    const parsedLine = parseFloat(prop.line);
    if (!parsedLine || parsedLine <= 0) return false;

    const newBet = {
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
      player_name: String(prop.player_name),
      player_id: prop.player_id || null,
      stat: prop.stat || 'PTS',
      line: parsedLine,
      side: prop.side || 'OVER',
      odds: parseFloat(prop.odds) || 1.91,
      book: prop.bookmaker || prop.book || '',
      game_label: prop.game_label || '',
      current_val: null,
      game_status: null,
      source: null,
      is_injured: false,
      injury_status: '',
      last_updated: null,
      history: [],
    };

    const stored = Array.isArray(bets) ? bets.slice() : [];

    // Dupe check: prefer player_id match when both sides have one, else fall back
    // to name match — prevents null-id props from all blocking each other.
    const isDupe = stored.some(function (b) { return isDuplicateBet(newBet, b); });

    if (isDupe) {
      _showTrackerToast(prop.player_name + ' already in Tracker');
      return false;
    }

    bets = [newBet].concat(stored);
    saveBets();

    _showTrackerToast(prop.player_name + ' added to Tracker');
    return true;
  }

  // Expose so Analyzer 'Send to Tracker' button (outside this IIFE) can call it
  window._trackerAddProp = addPropToTracker;

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
    el._t = setTimeout(function () {
      el.style.opacity = '0'; el.style.transform = 'translateX(-50%) translateY(20px)';
    }, 2600);
  }

  // ── Add bet handler ────────────────────────────────────────────────────
  trackerAddBtn.addEventListener('click', async function () {
    showErr('');
    const playerName = (trackerPlayerInput.value || '').trim();
    const stat = trackerStatSelect.value;
    const line = parseFloat(trackerLineInput.value);
    const side = trackerSideSelect.value;
    const odds = parseFloat(trackerOddsInput.value) || 1.91;
    const book = (trackerBookInput.value || '').trim();

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
        history: [],
      };

      const alreadyTracked = bets.some(function (b) { return isDuplicateBet(bet, b); });
      if (alreadyTracked) {
        showErr(playerName + ' is already in Prop Tracker for this line.');
        return;
      }

      bets.unshift(bet);
      saveBets();
      try { renderAll(); } catch (e) { console.error('Tracker renderAll error:', e); }

      // Clear inputs and reset autocomplete state
      trackerPlayerInput.value = '';
      trackerLineInput.value = '';
      trackerOddsInput.value = '';
      trackerBookInput.value = '';
      _selectedPlayerId = null;
      _selectedPlayerName = '';
      closeDropdown();

      // Auto-refresh the newly added bet to get live stat
      if (playerId) {
        try {
          await refreshBet(bet);
          saveBets();
          try { renderAll(); } catch (e) { /* non-fatal */ }
        } catch (e) { /* non-fatal */ }
      }
    } catch (e) {
      console.error('Add prop error:', e);
      showErr('Failed to add prop. Please try again.');
    } finally {
      trackerAddBtn.disabled = false;
      trackerAddBtn.textContent = 'Add Prop';
    }
  });

  // ── Refresh all handler ────────────────────────────────────────────────
  trackerRefreshBtn.addEventListener('click', async function () {
    if (!bets.length) return;
    trackerRefreshBtn.classList.add('spinning');
    trackerRefreshBtn.disabled = true;

    const refreshable = bets.filter(function (b) { return b.player_id; });
    await Promise.allSettled(refreshable.map(refreshBet));
    saveBets();

    // Pulse any newly-hit cards
    renderAll();
    trackerCards.querySelectorAll('.tracker-card.hit').forEach(function (card) {
      card.classList.add('hit-new');
      setTimeout(function () { card.classList.remove('hit-new'); }, 2000);
    });

    trackerRefreshBtn.classList.remove('spinning');
    trackerRefreshBtn.disabled = false;
  });

  if (trackerSortSelect) {
    trackerSortSelect.addEventListener('change', function () {
      trackerSortMode = trackerSortSelect.value || 'closest';
      renderAll();
    });
  }
  if (trackerGroupSelect) {
    trackerGroupSelect.addEventListener('change', function () {
      trackerGroupMode = trackerGroupSelect.value || 'game';
      renderAll();
    });
  }
  if (trackerFilterSelect) {
    trackerFilterSelect.addEventListener('change', function () {
      trackerFilterMode = trackerFilterSelect.value || 'all';
      renderAll();
    });
  }

  // ── Init: restore from localStorage ───────────────────────────────────
  (async function initTrackerState() {
    try {
      bets = await loadBets();
    } catch (e) {
      console.error('Tracker load failed:', e);
      bets = [];
    }
    renderAll();
  })();

  // Auto-refresh every 30s when live/upcoming game
  setInterval(async function () {
    const active = bets.some(function (b) { return b.game_status === 'live' || b.game_status === 'scheduled'; });
    if (!bets.length || !active) return;
    await Promise.allSettled(bets.filter(function (b) { return b.player_id; }).map(refreshBet));
    saveBets(); renderAll();
  }, 30000);

  // ── Expose globally so parlay builder (different IIFE) can call in ────
  window._trackerAddProp = function (prop) {
    const added = addPropToTracker(prop);
    try { renderAll(); } catch (e) { console.error('Tracker renderAll failed:', e); }
    if (typeof switchView === 'function') switchView('tracker');
  };

})();

/* === Cross-page prop slip / insight upgrade pass === */
(function () {
  const SLIP_STORAGE_KEY = 'nba-props-slip-drawer-v1';
  let slipDrawerMounted = false;

  function safeNumber(value, fallback = null) {
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
  }

  function parsePercentLike(value) {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string') {
      const num = Number(value.replace(/[^\d.-]/g, ''));
      return Number.isFinite(num) ? num : 0;
    }
    return 0;
  }

  function loadSlipProps() {
    try {
      const parsed = JSON.parse(localStorage.getItem(SLIP_STORAGE_KEY) || '[]');
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  function saveSlipProps(items) {
    localStorage.setItem(SLIP_STORAGE_KEY, JSON.stringify(items.slice(0, 12)));
  }

  function getSlipKey(prop) {
    return [prop.player_id || prop.player_name, prop.stat, prop.line, prop.side].join(':');
  }

  function clamp(n, min, max) {
    return Math.max(min, Math.min(max, n));
  }

  function computeConfidenceScoreFromParts(parts = {}) {
    const hitRate = parsePercentLike(parts.hit_rate);
    const ev = parsePercentLike(parts.ev);
    const edge = parsePercentLike(parts.edge);
    const games = safeNumber(parts.games_count, 0) || 0;
    const h2hHitRate = parsePercentLike(parts.h2h_hit_rate);
    const h2hGames = safeNumber(parts.h2h_games, 0) || 0;
    const minutes = safeNumber(parts.minutes_last5, 0) || 0;
    let score = 42;
    score += clamp((hitRate - 50) * 0.55, -18, 25);
    score += clamp(ev * 0.55, -10, 18);
    score += clamp(edge * 0.35, -8, 12);
    score += clamp((games - 5) * 1.1, 0, 12);
    score += clamp((h2hHitRate - 50) * 0.18, -6, 8);
    score += clamp(h2hGames * 1.2, 0, 6);
    score += clamp((minutes - 26) * 0.45, -6, 8);
    return Math.round(clamp(score, 18, 97));
  }

  function getConfidenceBucket(score) {
    if (score >= 86) return { label: 'Elite', tone: 'elite' };
    if (score >= 72) return { label: 'High', tone: 'good' };
    if (score >= 56) return { label: 'Medium', tone: 'warm' };
    return { label: 'Low', tone: 'neutral' };
  }

  function makeInsightChips(source = {}) {
    const chips = [];
    const hitRate = parsePercentLike(source.hit_rate);
    const ev = parsePercentLike(source.ev);
    const edge = parsePercentLike(source.edge);
    const avg = safeNumber(source.average);
    const line = safeNumber(source.line);
    const h2hHitRate = parsePercentLike(source.h2h_hit_rate);
    const h2hGames = safeNumber(source.h2h_games, 0) || 0;
    const deltaPct = safeNumber(source.vs_position_delta_pct);
    const b2b = !!source.is_back_to_back;
    const impactCount = safeNumber(source.impact_count, 0) || 0;
    const minutesTrend = String(source.minutes_trend || '').toLowerCase();
    const volumeTrend = String(source.volume_trend || '').toLowerCase();
    const availability = String(source.availability_status || '').toLowerCase();
    const recommendation = String(source.recommended_side || source.side || '').toUpperCase();

    if (hitRate >= 75) chips.push({ label: 'Hot streak', tone: 'good' });
    else if (hitRate <= 40 && hitRate > 0) chips.push({ label: 'Cold sample', tone: 'bad' });

    if (ev >= 6) chips.push({ label: 'Positive EV', tone: 'good' });
    else if (ev <= -4) chips.push({ label: 'Negative EV', tone: 'bad' });

    if (edge >= 5) chips.push({ label: 'Strong edge', tone: 'good' });
    else if (edge <= -5) chips.push({ label: 'Thin edge', tone: 'warning' });

    if (avg !== null && line !== null) {
      const diff = avg - line;
      if (recommendation.includes('UNDER') && diff <= -2) chips.push({ label: 'Line inflated', tone: 'warning' });
      if (recommendation.includes('OVER') && diff >= 2) chips.push({ label: 'Line within reach', tone: 'good' });
    }

    if (deltaPct !== null) {
      if (deltaPct >= 8) chips.push({ label: 'Soft matchup', tone: 'good' });
      else if (deltaPct <= -8) chips.push({ label: 'Tough matchup', tone: 'bad' });
    }

    if (h2hGames >= 2 && h2hHitRate >= 65) chips.push({ label: 'Good H2H', tone: 'good' });
    else if (h2hGames >= 2 && h2hHitRate <= 35) chips.push({ label: 'Poor H2H', tone: 'warning' });

    if (minutesTrend === 'up' || volumeTrend === 'up') chips.push({ label: 'Opportunity boost', tone: 'good' });
    if (b2b) chips.push({ label: 'Fatigue spot', tone: 'warning' });
    if (impactCount >= 1) chips.push({ label: 'Injury boost', tone: 'good' });
    if (availability.includes('out') || availability.includes('questionable')) chips.push({ label: 'Status risk', tone: 'bad' });

    return chips.slice(0, 4);
  }

  function flashSlipToast(message) {
    let el = document.getElementById('_slipToast');
    if (!el) {
      el = document.createElement('div');
      el.id = '_slipToast';
      el.className = 'slip-toast';
      document.body.appendChild(el);
    }
    el.textContent = message;
    el.classList.add('show');
    clearTimeout(el._timer);
    el._timer = setTimeout(() => el.classList.remove('show'), 2200);
  }

  function buildSlipProp(input = {}) {
    const confidenceScore = safeNumber(input.confidenceScore, null) ?? computeConfidenceScoreFromParts(input.metrics || input);
    const bucket = getConfidenceBucket(confidenceScore);
    const odds = safeNumber(input.odds, 1.91) || 1.91;
    return {
      key: getSlipKey(input),
      player_id: input.player_id ? Number(input.player_id) : null,
      player_name: input.player_name || 'Unknown Player',
      team: input.team || '',
      stat: input.stat || 'PTS',
      line: safeNumber(input.line, 0) || 0,
      side: String(input.side || 'OVER').toUpperCase(),
      odds,
      source: input.source || 'manual',
      confidenceScore,
      confidenceLabel: bucket.label,
      tone: bucket.tone,
      insightChips: Array.isArray(input.insightChips) ? input.insightChips.slice(0, 4) : [],
    };
  }

  function addToSlip(input, options = {}) {
    const items = loadSlipProps();
    const prop = buildSlipProp(input);
    const idx = items.findIndex(item => item.key === prop.key);
    if (idx >= 0) {
      items.splice(idx, 1);
      saveSlipProps(items);
      renderSlipDrawer();
      if (!options.silent) flashSlipToast('Removed from slip');
      return false;
    }
    items.unshift(prop);
    saveSlipProps(items);
    renderSlipDrawer();
    document.getElementById('propSlipDrawer')?.classList.remove('minimized');
    document.getElementById('propSlipDrawer')?.classList.add('open');
    if (!options.silent) flashSlipToast('Added to slip');
    return true;
  }

  function removeFromSlip(key) {
    const items = loadSlipProps().filter(item => item.key !== key);
    saveSlipProps(items);
    renderSlipDrawer();
  }

  function clearSlip() {
    saveSlipProps([]);
    renderSlipDrawer();
  }

  function getSlipTotals(items) {
    const odds = items.reduce((acc, item) => acc * (safeNumber(item.odds, 1.91) || 1.91), 1);
    const conf = items.length ? Math.round(items.reduce((acc, item) => acc + (safeNumber(item.confidenceScore, 0) || 0), 0) / items.length) : 0;
    return { odds, conf, bucket: getConfidenceBucket(conf) };
  }

  function mountSlipDrawer() {
    if (slipDrawerMounted) return;
    slipDrawerMounted = true;
    const drawer = document.createElement('aside');
    drawer.id = 'propSlipDrawer';
    drawer.className = 'prop-slip-drawer';
    drawer.innerHTML = `
      <div class="prop-slip-shell">
        <button class="prop-slip-toggle" id="propSlipToggle" type="button" aria-label="Toggle slip drawer">
          <span class="prop-slip-toggle-count" id="propSlipToggleCount">0</span>
          <span>Slip</span>
        </button>
        <div class="prop-slip-panel" id="propSlipPanel">
          <div class="prop-slip-head">
            <div>
              <span class="section-kicker">Quick slip</span>
              <h3>Parlay staging</h3>
            </div>
            <div class="prop-slip-head-actions">
              <button class="text-btn" id="propSlipHideBtn" type="button">Hide</button>
              <button class="text-btn" id="propSlipToTrackerBtn" type="button">Send to tracker</button>
              <button class="text-btn" id="propSlipClearBtn" type="button">Clear</button>
            </div>
          </div>
          <div class="prop-slip-summary" id="propSlipSummary"></div>
          <div class="prop-slip-list" id="propSlipList"></div>
        </div>
      </div>
    `;
    document.body.appendChild(drawer);

    drawer.querySelector('#propSlipToggle')?.addEventListener('click', () => {
      drawer.classList.remove('minimized');
      drawer.classList.toggle('open');
    });
    drawer.querySelector('#propSlipHideBtn')?.addEventListener('click', () => {
      drawer.classList.remove('open');
      drawer.classList.add('minimized');
    });
    drawer.querySelector('#propSlipClearBtn')?.addEventListener('click', () => clearSlip());
    drawer.querySelector('#propSlipToTrackerBtn')?.addEventListener('click', () => {
      const items = loadSlipProps();
      if (!items.length || typeof window._trackerAddProp !== 'function') {
        flashSlipToast(items.length ? 'Tracker is not ready yet' : 'Slip is empty');
        return;
      }
      items.forEach(item => {
        window._trackerAddProp({
          player_name: item.player_name,
          player_id: item.player_id ? Number(item.player_id) : null,
          stat: item.stat,
          line: item.line,
          side: item.side,
          odds: item.odds,
          book: item.source
        });
      });
      flashSlipToast('Slip sent to tracker');
      if (typeof switchView === 'function') switchView('tracker');
    });
  }

  function renderSlipDrawer() {
    mountSlipDrawer();
    const drawer = document.getElementById('propSlipDrawer');
    const list = document.getElementById('propSlipList');
    const summary = document.getElementById('propSlipSummary');
    const count = document.getElementById('propSlipToggleCount');
    const items = loadSlipProps();
    const totals = getSlipTotals(items);
    if (count) count.textContent = String(items.length);
    drawer?.classList.toggle('has-items', items.length > 0);
    if (!items.length) drawer?.classList.remove('minimized');

    if (summary) {
      summary.innerHTML = items.length ? `
        <div class="prop-slip-summary-card">
          <span class="small-label">Legs</span>
          <strong>${items.length}</strong>
          <small>Ready for review</small>
        </div>
        <div class="prop-slip-summary-card">
          <span class="small-label">Combined odds</span>
          <strong>${totals.odds.toFixed(2)}x</strong>
          <small>Approximate decimal</small>
        </div>
        <div class="prop-slip-summary-card ${totals.bucket.tone}">
          <span class="small-label">Avg confidence</span>
          <strong>${totals.conf}</strong>
          <small>${totals.bucket.label} read</small>
        </div>
      ` : `
        <div class="empty-state-panel compact" style="grid-column:1/-1">
          <div class="empty-icon">🎟️</div>
          <strong>No props in the slip yet.</strong>
          <span>Use Add to Slip from the analyzer, market, bet finder, or parlay board.</span>
        </div>
      `;
    }

    if (list) {
      list.innerHTML = items.map(item => `
        <article class="prop-slip-item">
          <div class="prop-slip-item-top">
            <strong>${escapeHtml(item.player_name)}</strong>
            <button class="prop-slip-remove" data-slip-remove="${escapeHtml(item.key)}" type="button">✕</button>
          </div>
          <div class="prop-slip-item-line">
            <span>${escapeHtml(item.stat)} ${item.line}</span>
            <span class="finder-badge ${escapeHtml(item.tone)}">${escapeHtml(item.side)}</span>
            <span class="slip-confidence-pill ${escapeHtml(item.tone)}">${item.confidenceScore}</span>
          </div>
          <div class="prop-slip-meta">
            <span>${escapeHtml(item.team || item.source || 'Saved')}</span>
            <span>${(safeNumber(item.odds, 1.91) || 1.91).toFixed(2)}x</span>
          </div>
          <div class="prop-slip-chip-row">
            ${(item.insightChips || []).map(chip => `<span class="smart-chip tone-${escapeHtml(chip.tone || 'neutral')}">${escapeHtml(chip.label)}</span>`).join('')}
          </div>
        </article>
      `).join('');
      list.querySelectorAll('[data-slip-remove]').forEach(btn => {
        btn.addEventListener('click', () => removeFromSlip(btn.dataset.slipRemove));
      });
    }
  }

  function injectAnalyzerSlipButton() {
    const saveBtn = document.getElementById('savePropBtn');
    if (!saveBtn || document.getElementById('addToSlipBtn')) return;

    // ── Add to Slip ──────────────────────────────────────────────────────
    const slipBtn = document.createElement('button');
    slipBtn.id = 'addToSlipBtn';
    slipBtn.className = 'secondary-btn secondary-btn-glow';
    slipBtn.type = 'button';
    slipBtn.textContent = 'Add to Slip';
    saveBtn.insertAdjacentElement('afterend', slipBtn);
    slipBtn.addEventListener('click', () => {
      if (!selectedPlayer) return;
      const payload = lastPayload || {};
      const side = String(payload.recommended_side || payload.best_side || (parsePercentLike(payload.hit_rate) >= 50 ? 'OVER' : 'UNDER')).toUpperCase();
      const insightChips = makeInsightChips({
        hit_rate: payload.hit_rate,
        average: payload.average,
        line: payload.line,
        h2h_hit_rate: payload.h2h?.hit_rate,
        h2h_games: payload.h2h?.games_count,
        vs_position_delta_pct: payload.matchup?.vs_position?.delta_pct,
        is_back_to_back: payload.environment?.is_back_to_back,
        impact_count: payload.team_context?.impact_count,
        minutes_trend: payload.opportunity?.minutes_trend,
        volume_trend: payload.opportunity?.volume_trend,
        availability_status: payload.availability?.status,
        recommended_side: side
      });
      const confidenceScore = computeConfidenceScoreFromParts({
        hit_rate: payload.hit_rate,
        games_count: payload.games_count,
        h2h_hit_rate: payload.h2h?.hit_rate,
        h2h_games: payload.h2h?.games_count,
        minutes_last5: payload.opportunity?.minutes_last5
      });
      addToSlip({
        player_id: selectedPlayer.id,
        player_name: selectedPlayer.full_name,
        team: selectedPlayer.team_abbreviation || '',
        stat: selectedStat,
        line: safeNumber(lineInput?.value, 0) || payload.line || 0,
        side,
        odds: 1.91,
        source: 'Analyzer',
        confidenceScore,
        insightChips
      });
      refreshSlipButtons();
    });

    // ── Send to Tracker ──────────────────────────────────────────────────
    if (!document.getElementById('analyzerToTrackerBtn')) {
      const toTrackerBtn = document.createElement('button');
      toTrackerBtn.id = 'analyzerToTrackerBtn';
      toTrackerBtn.className = 'secondary-btn';
      toTrackerBtn.type = 'button';
      toTrackerBtn.title = 'Send this prop to Live Tracker for real-time monitoring';
      toTrackerBtn.textContent = '\u{1F4E1} Send to Tracker';
      slipBtn.insertAdjacentElement('afterend', toTrackerBtn);
      toTrackerBtn.addEventListener('click', () => {
        if (!selectedPlayer) { alert('Please select and analyze a player first.'); return; }
        const payload = lastPayload || {};
        const side = String(payload.recommended_side || payload.best_side || (parsePercentLike(payload.hit_rate) >= 50 ? 'OVER' : 'UNDER')).toUpperCase();
        const prop = {
          player_id: selectedPlayer.id,
          player_name: selectedPlayer.full_name,
          stat: selectedStat,
          line: safeNumber(lineInput?.value, 0) || payload.line || 0,
          side,
          odds: 1.91,
          source: 'Analyzer'
        };
        // addPropToTracker is exposed on window._trackerAddProp from the tracker IIFE
        const trackerFn = window._trackerAddProp;
        if (typeof trackerFn !== 'function') {
          alert('Tracker not available. Please scroll to the Live Tracker section first to initialize it.');
          return;
        }
        const added = trackerFn(prop);
        if (added !== false) {
          showAppToast('✅ Sent to Live Tracker!', 'success');
          switchView && switchView('tracker', { scroll: true });
        }
      });
    }

    // ── Send to Backtest ─────────────────────────────────────────────────
    if (!document.getElementById('analyzerToBacktestBtn')) {
      const toBacktestBtn = document.createElement('button');
      toBacktestBtn.id = 'analyzerToBacktestBtn';
      toBacktestBtn.className = 'secondary-btn';
      toBacktestBtn.type = 'button';
      toBacktestBtn.title = 'Pre-fill the Backtest log form with current analysis results';
      toBacktestBtn.textContent = '\u{1F4CA} Send to Backtest';
      const trackerBtnEl = document.getElementById('analyzerToTrackerBtn');
      (trackerBtnEl || slipBtn).insertAdjacentElement('afterend', toBacktestBtn);
      toBacktestBtn.addEventListener('click', () => {
        if (!selectedPlayer) { alert('Please select and analyze a player first.'); return; }
        if (typeof window._backtestPrefillFromPayload === 'function') {
          window._backtestPrefillFromPayload(lastPayload);
          showAppToast('✅ Sent to Backtest — form pre-filled below!', 'info');
          const btSection = document.getElementById('backtestSection');
          if (btSection) btSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
          else switchView && switchView('backtest', { scroll: true });
        } else {
          alert('Backtest section not loaded yet. Please scroll to the Backtest Performance section first.');
        }
      });
    }
  }

  function buildMarketSlipPropFromItem(item) {
    const side = String(item?.best_bet?.display_side || item?.best_bet?.side || 'OVER').toUpperCase();
    const odds = safeNumber(item?.best_bet?.best_odds, null)
      || safeNumber(item?.best_bet?.odds, null)
      || (side.includes('UNDER') ? safeNumber(item?.market?.under_odds, 1.91) : safeNumber(item?.market?.over_odds, 1.91))
      || 1.91;
    return {
      player_id: item?.player?.id,
      player_name: item?.player?.full_name,
      team: item?.player?.team || item?.player?.team_abbreviation || '',
      stat: item?.market?.stat,
      line: item?.market?.line,
      side,
      odds,
      source: 'Scanner',
      confidenceScore: safeNumber(item?.best_bet?.confidence_score, null) || computeConfidenceScoreFromParts({
        hit_rate: item?.analysis?.hit_rate,
        edge: item?.best_bet?.edge,
        ev: item?.best_bet?.ev,
        games_count: item?.analysis?.games_count,
        h2h_hit_rate: item?.analysis?.h2h?.hit_rate,
        h2h_games: item?.analysis?.h2h?.games_count,
        minutes_last5: item?.analysis?.opportunity?.minutes_last5
      }),
      insightChips: makeInsightChips({
        hit_rate: item?.analysis?.hit_rate,
        edge: item?.best_bet?.edge,
        ev: item?.best_bet?.ev,
        average: item?.analysis?.average,
        line: item?.market?.line,
        h2h_hit_rate: item?.analysis?.h2h?.hit_rate,
        h2h_games: item?.analysis?.h2h?.games_count,
        vs_position_delta_pct: item?.analysis?.matchup?.vs_position?.delta_pct,
        is_back_to_back: item?.analysis?.environment?.is_back_to_back,
        impact_count: item?.analysis?.team_context?.impact_count,
        minutes_trend: item?.analysis?.opportunity?.minutes_trend,
        volume_trend: item?.analysis?.opportunity?.volume_trend,
        availability_status: item?.availability?.status,
        recommended_side: side
      })
    };
  }

  function enhanceMarketUI() {
    const root = document.getElementById('marketResults');
    if (!root || !currentMarketResultsPayload?.results?.length) return;
    const results = [...filterMarketRows(currentMarketResultsPayload.results || [], currentMarketFilter || 'all')]
      .filter(item => passesExpertFilters(item))
      .filter(item => passesAdvancedMarketFilters(item))
      .sort((a, b) => compareMarketRows(a, b, currentMarketSort || 'best_ev', currentMarketSortDirection));

    root.querySelectorAll('.market-slip-card').forEach((card, index) => {
      const item = results[index];
      if (!item || card.querySelector('.market-slip-cta-row')) return;
      const prop = buildMarketSlipPropFromItem(item);
      const cta = document.createElement('div');
      cta.className = 'market-slip-cta-row';
      cta.innerHTML = `
        <span class="smart-confidence ${getConfidenceBucket(prop.confidenceScore).tone}">Confidence ${prop.confidenceScore}</span>
        <button class="secondary-btn compact-slip-btn" type="button">${loadSlipProps().some(x => x.key === getSlipKey(prop)) ? 'In Slip' : 'Add to Slip'}</button>
      `;
      card.querySelector('.market-slip-footer')?.insertAdjacentElement('beforebegin', cta);
      cta.querySelector('button')?.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        addToSlip(prop);
        refreshSlipButtons();
      });
    });

    root.querySelectorAll('.market-row').forEach((row, index) => {
      const item = results[index];
      if (!item || row.querySelector('.market-inline-actions')) return;
      const prop = buildMarketSlipPropFromItem(item);
      const cell = document.createElement('div');
      cell.className = 'market-inline-actions';
      cell.innerHTML = `
        <span class="smart-confidence ${getConfidenceBucket(prop.confidenceScore).tone}">${prop.confidenceScore}</span>
        <button class="text-btn compact-inline-btn" type="button">${loadSlipProps().some(x => x.key === getSlipKey(prop)) ? 'In Slip' : 'Add to Slip'}</button>
      `;
      const target = row.querySelector('.market-confidence-cell');
      target?.appendChild(cell);
      cell.querySelector('button')?.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        addToSlip(prop);
        refreshSlipButtons();
      });
    });
  }

  function enhanceBetFinderUI() {
    const root = document.getElementById('betFinderResults');
    if (!root || !root.querySelectorAll('.finder-card').length) return;
    const cards = root.querySelectorAll('.finder-card');
    cards.forEach(card => {
      if (card.querySelector('.finder-slip-row')) return;
      const hitChip = card.querySelector('.finder-chip');
      const hitRate = hitChip ? parsePercentLike(hitChip.textContent) : 0;
      const lineText = card.querySelector('.market-slip-subtext')?.textContent || '';
      const lineMatch = lineText.match(/line\s+([0-9.]+)/i);
      const line = lineMatch ? Number(lineMatch[1]) : safeNumber(lineInput?.value, 0) || 0;
      const stat = selectedStat || 'PTS';
      const side = card.querySelector('.finder-side-pill')?.textContent?.trim()?.toUpperCase() || 'OVER';
      const confidenceScore = computeConfidenceScoreFromParts({ hit_rate: hitRate, games_count: Number((card.querySelectorAll('.finder-chip')[1]?.textContent || '0/0').split('/')[1] || 0) });
      const insightChips = makeInsightChips({ hit_rate: hitRate, line, recommended_side: side, average: safeNumber(card.querySelector('.finder-ticket-stat strong')?.textContent, 0) });
      const prop = {
        player_id: card.dataset.id,
        player_name: card.dataset.name,
        team: card.dataset.teamAbbr || '',
        stat,
        line,
        side,
        odds: 1.91,
        source: 'Bet Finder',
        confidenceScore,
        insightChips
      };
      const row = document.createElement('div');
      row.className = 'finder-slip-row';
      row.innerHTML = `
        <span class="smart-confidence ${getConfidenceBucket(confidenceScore).tone}">Confidence ${confidenceScore}</span>
        <button class="secondary-btn compact-slip-btn" type="button">${loadSlipProps().some(x => x.key === getSlipKey(prop)) ? 'In Slip' : 'Add to Slip'}</button>
      `;
      card.querySelector('.finder-ticket-footer')?.insertAdjacentElement('beforebegin', row);
      row.querySelector('button')?.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        addToSlip(prop);
        refreshSlipButtons();
      });
    });
  }

  function enhanceParlayUI() {
    const ticketGrid = document.querySelector('#parlayTicket .parlay-legs-grid');
    ticketGrid?.querySelectorAll('.parlay-leg-card').forEach(card => {
      if (card.querySelector('.parlay-slip-row')) return;
      const player = card.querySelector('.parlay-leg-player')?.textContent?.trim() || 'Unknown Player';
      const stat = card.querySelector('.parlay-leg-stat')?.textContent?.trim() || 'PTS';
      const line = safeNumber(card.querySelector('.parlay-leg-line')?.textContent, 0) || 0;
      const side = card.querySelector('.parlay-leg-side')?.textContent?.trim()?.toUpperCase() || 'OVER';
      const hitRateText = card.querySelector('.parlay-leg-stat-item .sval')?.textContent || '0';
      const confidenceScore = computeConfidenceScoreFromParts({ hit_rate: parsePercentLike(hitRateText), line });
      const prop = { player_name: player, stat, line, side, odds: 1.91, source: 'Parlay Builder', confidenceScore, insightChips: makeInsightChips({ hit_rate: parsePercentLike(hitRateText), line, recommended_side: side }) };
      const row = document.createElement('div');
      row.className = 'parlay-slip-row';
      row.innerHTML = `<button class="secondary-btn compact-slip-btn" type="button">${loadSlipProps().some(x => x.key === getSlipKey(prop)) ? 'In Slip' : 'Add to Slip'}</button>`;
      card.appendChild(row);
      row.querySelector('button')?.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        addToSlip(prop);
        refreshSlipButtons();
      });
    });

    document.querySelectorAll('#parlayAllPropsBody tr[data-prop-idx]').forEach(row => {
      if (row.querySelector('.parlay-slip-inline')) return;
      const player = row.dataset.playerName || row.children[0]?.querySelector('span > span')?.textContent?.trim() || 'Unknown Player';
      const playerId = row.dataset.playerId ? Number(row.dataset.playerId) : null;
      const stat = row.dataset.stat || row.children[1]?.textContent?.trim() || 'PTS';
      const line = safeNumber(row.dataset.line || row.children[2]?.textContent, 0) || 0;
      const side = (row.dataset.side || row.children[3]?.textContent?.trim() || 'OVER').toUpperCase();
      const confidenceScore = computeConfidenceScoreFromParts({ hit_rate: parsePercentLike(row.children[4]?.textContent), line });
      const prop = { player_name: player, player_id: playerId, stat, line, side, odds: 1.91, source: 'Parlay Board', confidenceScore };
      const cell = document.createElement('div');
      cell.className = 'parlay-slip-inline';
      cell.innerHTML = `<button class="parlay-slip-btn" type="button">${loadSlipProps().some(x => x.key === getSlipKey(prop)) ? '✓ Slip' : '+ Slip'}</button>`;
      const actionCell = row.lastElementChild?.querySelector('.parlay-action-cell') || row.lastElementChild;
      actionCell?.appendChild(cell);
      cell.querySelector('button')?.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        addToSlip(prop);
        refreshSlipButtons();
      });
    });
  }

  function enhanceTrackerUI() {
    document.querySelectorAll('.tracker-card').forEach(card => {
      if (card.querySelector('.tracker-slip-row')) return;
      const player = card.querySelector('.tracker-player-name')?.textContent?.trim() || 'Unknown Player';
      const stat = card.querySelector('.tracker-stat-pill')?.textContent?.trim() || 'PTS';
      const lineLabel = card.querySelector('.tracker-line-label')?.textContent || '';
      const lineMatch = lineLabel.match(/\/\s*([0-9.]+)/);
      const line = lineMatch ? Number(lineMatch[1]) : 0;
      const side = card.querySelector('.tracker-side-pill')?.textContent?.trim()?.toUpperCase() || 'OVER';
      const currentVal = safeNumber(card.querySelector('.tracker-current-val')?.textContent, 0) || 0;
      const confidenceScore = computeConfidenceScoreFromParts({ average: currentVal, line, recommended_side: side, hit_rate: currentVal >= line ? 65 : 45 });
      const insightChips = makeInsightChips({ average: currentVal, line, recommended_side: side });
      const meta = document.createElement('div');
      meta.className = 'tracker-slip-row';
      meta.innerHTML = `
        <span class="smart-confidence ${getConfidenceBucket(confidenceScore).tone}">${confidenceScore}</span>
        <button class="text-btn compact-inline-btn" type="button">${loadSlipProps().some(x => x.key === getSlipKey({ player_name: player, stat, line, side })) ? 'In Slip' : 'Add to Slip'}</button>
      `;
      card.querySelector('.tracker-card-meta')?.appendChild(meta);
      meta.querySelector('button')?.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        addToSlip({ player_name: player, stat, line, side, odds: 1.91, source: 'Tracker', confidenceScore, insightChips });
        refreshSlipButtons();
      });
    });
  }

  function renderAnalyzerSummaryUpgrade(payload) {
    const chartArea = document.getElementById('chartChips');
    if (!chartArea) return;
    let summary = document.getElementById('smartAnalyzerSummary');
    if (!summary) {
      summary = document.createElement('div');
      summary.id = 'smartAnalyzerSummary';
      summary.className = 'smart-analyzer-summary';
      chartArea.insertAdjacentElement('afterend', summary);
    }
    const confidenceScore = computeConfidenceScoreFromParts({
      hit_rate: payload?.hit_rate,
      games_count: payload?.games_count,
      h2h_hit_rate: payload?.h2h?.hit_rate,
      h2h_games: payload?.h2h?.games_count,
      minutes_last5: payload?.opportunity?.minutes_last5
    });
    const bucket = getConfidenceBucket(confidenceScore);
    const side = String(payload?.recommended_side || (parsePercentLike(payload?.hit_rate) >= 50 ? 'OVER' : 'UNDER')).toUpperCase();
    const chips = makeInsightChips({
      hit_rate: payload?.hit_rate,
      average: payload?.average,
      line: payload?.line,
      h2h_hit_rate: payload?.h2h?.hit_rate,
      h2h_games: payload?.h2h?.games_count,
      vs_position_delta_pct: payload?.matchup?.vs_position?.delta_pct,
      is_back_to_back: payload?.environment?.is_back_to_back,
      impact_count: payload?.team_context?.impact_count,
      minutes_trend: payload?.opportunity?.minutes_trend,
      volume_trend: payload?.opportunity?.volume_trend,
      availability_status: payload?.availability?.status,
      recommended_side: side
    });

    const bullets = [
      `${side} lean on ${payload?.line} ${payload?.stat || selectedStat}`,
      `${payload?.hit_count || 0}/${payload?.games_count || 0} at the current line`,
      payload?.matchup?.next_game?.matchup_label ? `Next matchup: ${payload.matchup.next_game.matchup_label}` : 'Next matchup pending',
      payload?.environment?.is_back_to_back ? 'Back-to-back risk is active' : 'No back-to-back fatigue flag'
    ];

    summary.innerHTML = `
      <div class="smart-analyzer-header">
        <span class="smart-confidence ${bucket.tone}">Confidence ${confidenceScore}</span>
        <span class="smart-summary-side ${side.includes('UNDER') ? 'under' : 'over'}">${escapeHtml(side)}</span>
      </div>
      <div class="smart-chip-row">
        ${chips.map(chip => `<span class="smart-chip tone-${escapeHtml(chip.tone || 'neutral')}">${escapeHtml(chip.label)}</span>`).join('')}
      </div>
      <ul class="smart-summary-list">
        ${bullets.map(item => `<li>${escapeHtml(item)}</li>`).join('')}
      </ul>
    `;
  }

  function refreshSlipButtons() {
    renderSlipDrawer();
    enhanceMarketUI();
    enhanceBetFinderUI();
    enhanceParlayUI();
    enhanceTrackerUI();
  }

  const _renderMarketResults = renderMarketResults;
  renderMarketResults = function (payload) {
    _renderMarketResults(payload);
    enhanceMarketUI();
    renderSlipDrawer();
  };

  const _renderBetFinderResults = renderBetFinderResults;
  renderBetFinderResults = function (payload) {
    _renderBetFinderResults(payload);
    enhanceBetFinderUI();
    renderSlipDrawer();
  };

  const _renderSummary = renderSummary;
  renderSummary = function (payload) {
    _renderSummary(payload);
    renderAnalyzerSummaryUpgrade(payload);
    renderSlipDrawer();
  };

  const observer = new MutationObserver(() => {
    enhanceParlayUI();
    enhanceTrackerUI();
  });

  window.addEventListener('DOMContentLoaded', () => {
    mountSlipDrawer();
    renderSlipDrawer();
    injectAnalyzerSlipButton();
    observer.observe(document.body, { childList: true, subtree: true });
    setTimeout(() => {
      refreshSlipButtons();
      renderSlipDrawer();
    }, 200);
  });

  mountSlipDrawer();
  renderSlipDrawer();
  injectAnalyzerSlipButton();
  setTimeout(() => {
    refreshSlipButtons();
    renderSlipDrawer();
  }, 400);
})();

/* ═══════════════════════════════════════════════════════════════════════════
   BACKTEST PERFORMANCE DASHBOARD
   Logs predictions to the server, resolves results, shows ROI / win-rate.
   ═══════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  // ── Element refs ──────────────────────────────────────────────────────
  const btPlayerInput  = document.getElementById('btPlayerInput');
  const btStatSelect   = document.getElementById('btStatSelect');
  const btLineInput    = document.getElementById('btLineInput');
  const btSideSelect   = document.getElementById('btSideSelect');
  const btTierSelect   = document.getElementById('btTierSelect');
  const btOddsInput    = document.getElementById('btOddsInput');
  const btPlayerDropdown = document.getElementById('btPlayerDropdown');
  const btLogBtn       = document.getElementById('btLogBtn');
  const btLogError     = document.getElementById('btLogError');
  const btRefreshBtn   = document.getElementById('btRefreshBtn');
  const btExportBtn    = document.getElementById('btExportBtn');
  const btImportInput  = document.getElementById('btImportInput');
  const btClearBtn     = document.getElementById('btClearBtn');
  const backtestLog    = document.getElementById('backtestLog');
  const backtestStats  = document.getElementById('backtestStats');
  const backtestSummaryPill = document.getElementById('backtestSummaryPill');
  const btFilterSearch = document.getElementById('btFilterSearch');
  const btFilterResult = document.getElementById('btFilterResult');
  const btFilterStat   = document.getElementById('btFilterStat');
  const btFilterTier   = document.getElementById('btFilterTier');
  const btFilterSide   = document.getElementById('btFilterSide');

  if (!btLogBtn) return; // backtest section not mounted

  // ── Helpers ───────────────────────────────────────────────────────────
  function esc(s) {
    return String(s || '')
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function showBtError(msg) {
    if (!btLogError) return;
    btLogError.textContent = msg;
    btLogError.style.display = msg ? '' : 'none';
  }

  function parseBacktestDate(value) {
    if (!value) return null;
    const dt = new Date(value);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }

  function formatBacktestDate(value) {
    const dt = parseBacktestDate(value);
    if (!dt) return '—';
    return dt.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  }

  function formatBacktestPercent(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
    return `${Number(value).toFixed(1)}%`;
  }

  function populateBacktestFilterOptions(entries) {
    const populate = function (el, values, labelFormatter) {
      if (!el) return;
      const current = el.value || 'all';
      const options = ['<option value="all">All</option>'].concat(
        [...values].sort().map(value => `<option value="${esc(value)}">${esc(labelFormatter ? labelFormatter(value) : value)}</option>`)
      );
      el.innerHTML = options.join('');
      if ([...values, 'all'].includes(current)) el.value = current;
    };
    populate(btFilterStat, new Set(entries.map(e => String(e.stat || '').trim()).filter(Boolean)), value => value);
    populate(btFilterTier, new Set(entries.map(e => String(e.confidence_tier || '').trim()).filter(Boolean)), value => value);
  }

  function getFilteredBacktestEntries(entries) {
    return (entries || []).filter(function (entry) {
      const haystack = [
        entry.player,
        entry.stat,
        entry.confidence_tier,
        entry.notes,
        entry.source,
      ].join(' ').toLowerCase();
      if (backtestFilters.search && !haystack.includes(backtestFilters.search)) return false;
      if (backtestFilters.result !== 'all' && String(entry.result || '').toLowerCase() !== backtestFilters.result) return false;
      if (backtestFilters.stat !== 'all' && String(entry.stat || '') !== backtestFilters.stat) return false;
      if (backtestFilters.tier !== 'all' && String(entry.confidence_tier || '') !== backtestFilters.tier) return false;
      if (backtestFilters.side !== 'all' && String(entry.side || '').toUpperCase() !== backtestFilters.side) return false;
      return true;
    });
  }

  function renderMiniBreakdown(targetId, title, items, formatter) {
    const el = document.getElementById(targetId);
    if (!el) return;
    if (!items || !items.length) {
      el.innerHTML = '';
      return;
    }
    el.innerHTML = `
      <div class="backtest-mini-card">
        <div class="insight-summary-label">${esc(title)}</div>
        <div class="backtest-mini-list">
          ${items.map(item => `
            <div class="backtest-mini-row">
              <span>${esc(item.label)}</span>
              <strong>${esc(formatter(item))}</strong>
            </div>
          `).join('')}
        </div>
      </div>
    `;
  }

  function renderBacktestBreakdowns(stats) {
    const sideItems = Object.entries(stats.by_side || {}).map(([label, data]) => ({ label, data }));
    renderMiniBreakdown('btSideBreakdown', 'By side', sideItems, item => `${item.data.win_rate}% • ${item.data.total}`);

    const alignItems = Object.entries(stats.by_market_alignment || {}).map(([label, data]) => ({ label, data }));
    renderMiniBreakdown('btAlignmentBreakdown', 'Market alignment', alignItems, item => `${item.data.win_rate}% • ${item.data.total}`);

    const playerItems = (stats.top_players || []).map(item => ({ label: item.player, data: item }));
    renderMiniBreakdown('btTopPlayers', 'Most tracked players', playerItems, item => `${item.data.total} • ${item.data.win_rate != null ? `${item.data.win_rate}%` : 'pending'}`);

    const recentForm = document.getElementById('btRecentForm');
    if (recentForm) {
      const form = stats.recent_form || [];
      recentForm.innerHTML = form.length ? `
        <div class="backtest-mini-card">
          <div class="insight-summary-label">Recent form</div>
          <div class="backtest-form-strip">
            ${form.map(hit => `<span class="backtest-form-pill ${hit ? 'is-hit' : 'is-miss'}">${hit ? 'W' : 'L'}</span>`).join('')}
          </div>
        </div>
      ` : '';
    }
  }

  function tierColor(tier) {
    if (!tier) return 'var(--neutral,#9ca3af)';
    const t = tier.toLowerCase();
    if (t === 'elite') return 'var(--good,#4ade80)';
    if (t === 'high')  return '#60a5fa';
    if (t === 'medium') return 'var(--warning,#f59e0b)';
    return 'var(--bad,#f87171)';
  }

  let btAutocompleteDebounce = null;
  let btSelectedPlayerName = '';
  let backtestEntriesCache = [];
  let backtestStatsCache = {};
  const backtestFilters = {
    search: '',
    result: 'all',
    stat: 'all',
    tier: 'all',
    side: 'all',
  };

  function mountBtDropdownToBody() {
    if (btPlayerDropdown && btPlayerDropdown.parentElement !== document.body) {
      document.body.appendChild(btPlayerDropdown);
    }
  }

  function positionBtDropdown() {
    if (!btPlayerInput || !btPlayerDropdown) return;
    const rect = btPlayerInput.getBoundingClientRect();
    btPlayerDropdown.style.position = 'fixed';
    btPlayerDropdown.style.top = (rect.bottom + 4) + 'px';
    btPlayerDropdown.style.left = rect.left + 'px';
    btPlayerDropdown.style.width = rect.width + 'px';
  }

  function closeBtDropdown() {
    if (btPlayerDropdown) {
      btPlayerDropdown.innerHTML = '';
      btPlayerDropdown.style.display = 'none';
    }
    if (btPlayerInput) {
      btPlayerInput.setAttribute('aria-expanded', 'false');
    }
  }

  function openBtDropdown(results) {
    if (!btPlayerDropdown) return;
    if (!results.length) {
      closeBtDropdown();
      return;
    }
    mountBtDropdownToBody();
    positionBtDropdown();
    btPlayerDropdown.innerHTML = results.map(function (p) {
      return '<li role="option" class="tracker-player-option" data-name="' + esc(p.full_name) + '" style="padding:8px 12px;cursor:pointer;list-style:none;border-bottom:1px solid var(--border,#333)">' +
        esc(p.full_name) + (p.is_active ? '' : ' <small style="opacity:.5">(inactive)</small>') + '</li>';
    }).join('');
    btPlayerDropdown.style.display = 'block';
    if (btPlayerInput) btPlayerInput.setAttribute('aria-expanded', 'true');
    btPlayerDropdown.querySelectorAll('.tracker-player-option').forEach(function (li) {
      li.addEventListener('mousedown', function (e) {
        e.preventDefault();
        btSelectedPlayerName = li.dataset.name || '';
        if (btPlayerInput) btPlayerInput.value = btSelectedPlayerName;
        closeBtDropdown();
      });
    });
  }

  if (btPlayerInput) {
    btPlayerInput.addEventListener('input', function () {
      const q = btPlayerInput.value.trim();
      btSelectedPlayerName = '';
      clearTimeout(btAutocompleteDebounce);
      if (q.length < 2) {
        closeBtDropdown();
        return;
      }
      btAutocompleteDebounce = setTimeout(async function () {
        try {
          const data = await apiFetch('/api/players/search?q=' + encodeURIComponent(q), {}, 6000);
          openBtDropdown((data.results || []).slice(0, 8));
        } catch (e) {
          closeBtDropdown();
        }
      }, 220);
    });
    btPlayerInput.addEventListener('blur', function () {
      setTimeout(closeBtDropdown, 200);
    });
    btPlayerInput.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') closeBtDropdown();
    });
    window.addEventListener('resize', positionBtDropdown);
    window.addEventListener('scroll', positionBtDropdown, true);
  }

  // ── Render aggregate stats row ────────────────────────────────────────
  function renderStats(stats) {
    if (!stats || stats.total === 0) {
      if (backtestStats) backtestStats.style.display = 'none';
      if (backtestSummaryPill) {
        backtestSummaryPill.className = 'spotlight-pill neutral';
        backtestSummaryPill.textContent = 'No results yet';
      }
      return;
    }
    if (backtestStats) backtestStats.style.display = '';

    // Summary pill
    if (backtestSummaryPill) {
      const wr = stats.win_rate;
      const pillClass = wr >= 57 ? 'good' : wr >= 52 ? 'warning' : 'bad';
      backtestSummaryPill.className = `spotlight-pill ${pillClass}`;
      backtestSummaryPill.textContent = `${wr}% hit • ROI ${stats.roi_pct > 0 ? '+' : ''}${stats.roi_pct}%`;
    }

    // Stat chips
    const setChip = (id, main, sub) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.querySelector('strong').textContent = main;
      el.querySelector('small').textContent  = sub;
    };
    setChip('btStatTotal',   stats.total,              `${stats.pending} pending`);
    setChip('btStatWin',     `${stats.win_rate}%`,     `${stats.hits}/${stats.hits + stats.misses} resolved`);
    const roiSign = stats.roi_pct > 0 ? '+' : '';
    setChip('btStatROI',     `${roiSign}${stats.roi_pct}%`, 'vs -110 odds');
    setChip('btStatPending', stats.pending,            `${stats.logged_total || stats.total + stats.pending} logged`);

    // Tier breakdown table
    const tierBreakdown = document.getElementById('btTierBreakdown');
    if (tierBreakdown && stats.by_tier && Object.keys(stats.by_tier).length) {
      const rows = Object.entries(stats.by_tier)
        .sort((a, b) => b[1].win_rate - a[1].win_rate)
        .map(([tier, d]) => {
          const barW = Math.round(d.win_rate);
          const col = tierColor(tier);
          return `<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;font-size:12px">
            <span style="width:70px;color:${col};font-weight:600">${esc(tier)}</span>
            <div style="flex:1;height:8px;background:rgba(255,255,255,0.07);border-radius:4px;overflow:hidden">
              <div style="width:${barW}%;height:100%;background:${col};border-radius:4px;transition:width 0.6s ease"></div>
            </div>
            <span style="width:52px;text-align:right;opacity:0.75">${d.win_rate}% (${d.total})</span>
          </div>`;
        }).join('');
      tierBreakdown.innerHTML = `<div class="insight-summary-label" style="margin-bottom:6px">By confidence tier</div>${rows}`;
    }

    // Stat-type breakdown
    const statBreakdown = document.getElementById('btStatBreakdown');
    if (statBreakdown && stats.by_stat && Object.keys(stats.by_stat).length) {
      const pills = Object.entries(stats.by_stat)
        .sort((a, b) => b[1].win_rate - a[1].win_rate)
        .map(([st, d]) => {
          const col = d.win_rate >= 57 ? 'var(--good)' : d.win_rate >= 52 ? '#60a5fa' : d.win_rate >= 48 ? 'var(--warning)' : 'var(--bad)';
          return `<span style="display:inline-flex;align-items:center;gap:5px;padding:4px 10px;border-radius:20px;background:rgba(255,255,255,0.06);font-size:11px;margin:2px">
            <strong style="color:${col}">${esc(st)}</strong>
            <span style="opacity:0.65">${d.win_rate}% · ${d.total}g</span>
          </span>`;
        }).join('');
      statBreakdown.innerHTML = `<div class="insight-summary-label" style="margin-bottom:5px">By stat type</div><div>${pills}</div>`;
    }

    renderBacktestBreakdowns(stats);
  }

  // ── Render log table ──────────────────────────────────────────────────
  function renderLog(entries) {
    if (!backtestLog) return;
    if (!entries || entries.length === 0) {
      backtestLog.innerHTML = `<div style="opacity:0.45;font-size:13px;padding:8px 0">No predictions logged yet. Use the form above to start tracking.</div>`;
      return;
    }
    const filteredEntries = getFilteredBacktestEntries(entries);
    if (!filteredEntries.length) {
      backtestLog.innerHTML = `<div style="opacity:0.55;font-size:13px;padding:8px 0">No backtest rows match the current filters.</div>`;
      return;
    }

    const rows = filteredEntries.map(e => {
      const isPending = e.result === 'pending';
      const isHit     = e.result === 'hit';
      const resultColor = isPending ? 'var(--neutral,#9ca3af)' : isHit ? 'var(--good,#4ade80)' : 'var(--bad,#f87171)';
      const resultIcon  = isPending ? '⏳' : isHit ? '✓' : '✗';
      const resultLabel = isPending ? 'Pending' : isHit ? 'Hit' : 'Miss';
      const dateStr = e.logged_at ? new Date(e.logged_at).toLocaleDateString(undefined, { month:'short', day:'numeric' }) : '—';

      return `<tr data-bt-id="${esc(e.id)}" style="border-bottom:1px solid rgba(255,255,255,0.05)">
        <td style="padding:7px 8px;font-weight:600;font-size:12px">${esc(e.player)}</td>
        <td style="padding:7px 6px;font-size:11px;opacity:0.75">${esc(e.stat)}</td>
        <td style="padding:7px 6px;font-size:11px">${esc(String(e.line))}</td>
        <td style="padding:7px 6px">
          <span style="font-size:10px;font-weight:700;padding:2px 7px;border-radius:10px;background:${e.side==='OVER'?'rgba(74,222,128,0.15)':'rgba(248,113,113,0.15)'};color:${e.side==='OVER'?'var(--good)':'var(--bad)'}">${esc(e.side)}</span>
        </td>
        <td style="padding:7px 6px;font-size:11px;color:${tierColor(e.confidence_tier)}">${esc(e.confidence_tier||'—')}</td>
        <td style="padding:7px 6px;font-size:11px;color:${resultColor};font-weight:700">${resultIcon} ${resultLabel}${e.actual_value !== null && e.actual_value !== undefined ? ` (${Number(e.actual_value).toFixed(1)})` : ''}</td>
        <td style="padding:7px 6px;font-size:11px;opacity:0.5">${dateStr}</td>
        <td style="padding:7px 6px">
          ${isPending
            ? `<div style="display:flex;gap:4px;align-items:center">
                <input type="number" step="0.1" min="0" placeholder="Actual" class="market-api-input bt-actual-input" style="width:70px;padding:3px 6px;font-size:11px" data-bt-resolve-id="${esc(e.id)}"/>
                <button class="secondary-btn bt-resolve-btn" data-bt-resolve-id="${esc(e.id)}" style="font-size:10px;padding:3px 8px">✓</button>
              </div>`
            : `<button class="bt-delete-btn" data-bt-id="${esc(e.id)}" style="background:none;border:none;cursor:pointer;opacity:0.4;font-size:13px;padding:2px 6px" title="Remove">✕</button>`
          }
        </td>
      </tr>`;
    }).join('');

    backtestLog.innerHTML = `
      <div style="overflow-x:auto">
        <table style="width:100%;border-collapse:collapse;font-size:12px">
          <thead>
            <tr style="opacity:0.45;font-size:10px;text-transform:uppercase;letter-spacing:0.05em;border-bottom:1px solid rgba(255,255,255,0.1)">
              <th style="padding:5px 8px;text-align:left;font-weight:600">Player</th>
              <th style="padding:5px 6px;text-align:left;font-weight:600">Stat</th>
              <th style="padding:5px 6px;text-align:left;font-weight:600">Line</th>
              <th style="padding:5px 6px;text-align:left;font-weight:600">Side</th>
              <th style="padding:5px 6px;text-align:left;font-weight:600">Tier</th>
              <th style="padding:5px 6px;text-align:left;font-weight:600">Result</th>
              <th style="padding:5px 6px;text-align:left;font-weight:600">Date</th>
              <th style="padding:5px 6px;text-align:left;font-weight:600">Action</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;

    // Wire resolve buttons
    backtestLog.querySelectorAll('.bt-resolve-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        const id = btn.dataset.btResolveId;
        const inputEl = backtestLog.querySelector(`.bt-actual-input[data-bt-resolve-id="${id}"]`);
        const actualVal = inputEl ? parseFloat(inputEl.value) : NaN;
        if (!id || isNaN(actualVal)) {
          showBtError('Enter the actual stat value before resolving.');
          return;
        }
        try {
          await apiFetch('/api/backtest/resolve', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id, actual_value: actualVal }),
          }, 8000);
          showBtError('');
          await loadAndRender();
        } catch (err) {
          showBtError('Resolve failed: ' + err.message);
        }
      });
    });

    // Wire delete buttons
    backtestLog.querySelectorAll('.bt-delete-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        const id = btn.dataset.btId;
        if (!id) return;
        // Optimistically remove the row immediately for instant feedback
        const row = btn.closest('tr');
        if (row) row.remove();
        try {
          const r = await fetch(`/api/backtest/log/${encodeURIComponent(id)}`, { method: 'DELETE' });
          if (!r.ok) throw new Error(await r.text());
          // Reload stats only (log is already visually updated)
          await loadAndRender();
        } catch (err) {
          showBtError('Delete failed: ' + err.message);
          await loadAndRender(); // re-render to restore consistent state
        }
      });
    });
  }

  // ── Load from server and render ───────────────────────────────────────
  async function loadAndRender() {
    try {
      const data = await apiFetch('/api/backtest/log?limit=200', {}, 8000);
      backtestEntriesCache = data.entries || [];
      backtestStatsCache = data.stats || {};
      populateBacktestFilterOptions(backtestEntriesCache);
      renderStats(backtestStatsCache);
      renderLog(backtestEntriesCache);
    } catch (err) {
      if (backtestLog) backtestLog.innerHTML = `<div style="opacity:0.45;font-size:12px;padding:8px 0">Could not load backtest log: ${esc(err.message)}</div>`;
    }
  }

  function rerenderBacktestView() {
    renderStats(backtestStatsCache || {});
    renderLog(backtestEntriesCache || []);
  }

  function parseBacktestCsv(text) {
    const lines = String(text || '').split(/\r?\n/).filter(Boolean);
    if (lines.length < 2) return [];
    const parseLine = function (line) {
      const values = [];
      let current = '';
      let inQuotes = false;
      for (let i = 0; i < line.length; i += 1) {
        const ch = line[i];
        if (ch === '"') {
          if (inQuotes && line[i + 1] === '"') {
            current += '"';
            i += 1;
          } else {
            inQuotes = !inQuotes;
          }
        } else if (ch === ',' && !inQuotes) {
          values.push(current);
          current = '';
        } else {
          current += ch;
        }
      }
      values.push(current);
      return values.map(v => v.trim());
    };
    const headers = parseLine(lines[0]).map(h => h.toLowerCase());
    return lines.slice(1).map(line => {
      const cells = parseLine(line);
      const row = {};
      headers.forEach((header, idx) => {
        row[header] = cells[idx] ?? '';
      });
      return row;
    }).filter(row => row.player && row.stat);
  }

  function buildBacktestCsv(entries) {
    const headers = ['id', 'player', 'stat', 'line', 'side', 'confidence_tier', 'confidence_score', 'model_prob', 'odds', 'result', 'actual_value', 'logged_at', 'resolved_at', 'event_date', 'source', 'market_side', 'market_disagrees', 'notes'];
    const escCsv = value => {
      const s = String(value ?? '');
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    return [headers.join(',')].concat((entries || []).map(entry => headers.map(key => escCsv(entry[key])).join(','))).join('\n');
  }

  // ── Log prediction ────────────────────────────────────────────────────
  btLogBtn.addEventListener('click', async () => {
    const player = btPlayerInput ? btPlayerInput.value.trim() : '';
    const stat   = btStatSelect  ? btStatSelect.value   : 'PTS';
    const line   = btLineInput   ? parseFloat(btLineInput.value) : NaN;
    const side   = btSideSelect  ? btSideSelect.value   : 'OVER';
    const tier   = btTierSelect  ? btTierSelect.value   : 'Medium';
    const odds   = btOddsInput   ? parseFloat(btOddsInput.value) : 1.91;

    if (!player) { showBtError('Enter a player name.'); return; }
    if (isNaN(line) || line <= 0) { showBtError('Enter a valid line.'); return; }

    showBtError('');
    btLogBtn.disabled = true;
    btLogBtn.textContent = 'Logging…';

    try {
      const ctrl = new AbortController();
      const timer = setTimeout(function () { ctrl.abort(); }, 12000);
      const r = await fetch('/api/backtest/log', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: ctrl.signal,
        body: JSON.stringify({
          player, stat, line, side,
          confidence_tier: tier,
          confidence_score: tier === 'Elite' ? 87 : tier === 'High' ? 76 : tier === 'Medium' ? 64 : 52,
          model_prob: 0.55,
          odds: isNaN(odds) ? 1.91 : odds,
          source: 'manual',
        }),
      });
      clearTimeout(timer);
      if (!r.ok) throw new Error(await r.text());
      if (btPlayerInput) btPlayerInput.value = '';
      if (btLineInput)   btLineInput.value   = '';
      if (btOddsInput)   btOddsInput.value   = '';
      btSelectedPlayerName = '';
      closeBtDropdown();
      await loadAndRender();
    } catch (err) {
      showBtError('Log failed: ' + (err && err.name === 'AbortError' ? 'request timed out' : err.message));
    } finally {
      btLogBtn.disabled = false;
      btLogBtn.textContent = 'Log Prediction';
    }
  });

  // ── Auto-fill from analyzer payload ──────────────────────────────────
  // When the analyzer runs, expose a hook so we can pre-fill the log form
  window._backtestPrefillFromPayload = function (payload) {
    if (!payload) return;
    try {
      const playerName = payload.player?.full_name || '';
      const stat       = payload.stat || 'PTS';
      const side       = payload.recommended_side || 'OVER';
      const tier       = payload.confidence?.tier || 'Medium';
      const score      = payload.confidence?.score || 0;

      if (btPlayerInput) btPlayerInput.value = playerName;
      if (btStatSelect)  btStatSelect.value  = stat;
      if (btSideSelect)  btSideSelect.value  = side;
      if (btLineInput)   btLineInput.value   = payload.line || '';
      if (btTierSelect)  btTierSelect.value  = tier;
      btSelectedPlayerName = playerName;

      // Scroll to backtest section smoothly
      const section = document.getElementById('backtestSection');
      if (section) section.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    } catch (e) { /* silent */ }
  };

  // ── Refresh button ────────────────────────────────────────────────────
  if (btRefreshBtn) {
    btRefreshBtn.addEventListener('click', loadAndRender);
  }

  [
    [btFilterSearch, 'search', value => value.trim().toLowerCase()],
    [btFilterResult, 'result', value => value],
    [btFilterStat, 'stat', value => value],
    [btFilterTier, 'tier', value => value],
    [btFilterSide, 'side', value => value],
  ].forEach(([el, key, transform]) => {
    if (!el) return;
    const apply = () => {
      backtestFilters[key] = transform(el.value || '');
      rerenderBacktestView();
    };
    el.addEventListener('input', apply);
    el.addEventListener('change', apply);
  });

  if (btExportBtn) {
    btExportBtn.addEventListener('click', () => {
      const csv = buildBacktestCsv(getFilteredBacktestEntries(backtestEntriesCache || []));
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `backtest-log-${new Date().toISOString().slice(0, 10)}.csv`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    });
  }

  if (btImportInput) {
    btImportInput.addEventListener('change', async () => {
      const file = btImportInput.files && btImportInput.files[0];
      if (!file) return;
      try {
        const text = await file.text();
        const entries = parseBacktestCsv(text).map(row => ({
          id: row.id,
          player: row.player,
          stat: row.stat,
          line: Number(row.line || 0),
          side: String(row.side || '').toUpperCase(),
          confidence_tier: row.confidence_tier,
          confidence_score: Number(row.confidence_score || 0),
          model_prob: Number(row.model_prob || 0.5),
          odds: row.odds ? Number(row.odds) : null,
          result: row.result || 'pending',
          actual_value: row.actual_value ? Number(row.actual_value) : null,
          logged_at: row.logged_at,
          resolved_at: row.resolved_at,
          event_date: row.event_date,
          source: row.source || 'csv-import',
          market_side: row.market_side,
          market_disagrees: String(row.market_disagrees || '').toLowerCase() === 'true',
          notes: row.notes || '',
        })).filter(entry => entry.player && entry.stat && entry.side);
        if (!entries.length) throw new Error('No usable rows found in that CSV.');
        const payload = await apiFetch('/api/backtest/import', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ entries }),
        }, 15000);
        showBtError('');
        showAppToast(`Imported ${payload.added || 0} rows${payload.skipped ? ` • ${payload.skipped} skipped` : ''}.`, 'info');
        await loadAndRender();
      } catch (err) {
        showBtError('Import failed: ' + err.message);
      } finally {
        btImportInput.value = '';
      }
    });
  }

  // ── Clear all ─────────────────────────────────────────────────────────
  if (btClearBtn) {
    btClearBtn.addEventListener('click', async () => {
      if (!confirm('Clear all backtest predictions? This cannot be undone.')) return;
      try {
        await fetch('/api/backtest/log', { method: 'DELETE' });
        await loadAndRender();
      } catch (err) {
        showBtError('Clear failed: ' + err.message);
      }
    });
  }

  // ── Initial load ──────────────────────────────────────────────────────
  loadAndRender();

})();

// ══════════════════════════════════════════════════════════════════════
// ── MARKET ADVANCED FILTERS (Bookmaker + Min/Max Odds) ────────────────
// ══════════════════════════════════════════════════════════════════════

(function initMarketAdvancedFilters() {
  const bookFilter   = document.getElementById('marketBookFilter');
  const minOddsInput = document.getElementById('marketMinOdds');
  const maxOddsInput = document.getElementById('marketMaxOdds');
  const resetBtn     = document.getElementById('marketAdvFilterResetBtn');

  if (!bookFilter && !minOddsInput) return;

  function applyAndRerender() {
    currentMarketBookFilter = (bookFilter?.value || '').toLowerCase().trim();
    currentMarketMinOdds    = minOddsInput?.value ? parseFloat(minOddsInput.value) : null;
    currentMarketMaxOdds    = maxOddsInput?.value ? parseFloat(maxOddsInput.value) : null;
    if (currentMarketResultsPayload) renderMarketResults(currentMarketResultsPayload);
  }

  bookFilter?.addEventListener('change', applyAndRerender);
  minOddsInput?.addEventListener('input', applyAndRerender);
  maxOddsInput?.addEventListener('input', applyAndRerender);

  resetBtn?.addEventListener('click', () => {
    if (bookFilter) bookFilter.value = '';
    if (minOddsInput) minOddsInput.value = '';
    if (maxOddsInput) maxOddsInput.value = '';
    currentMarketBookFilter = '';
    currentMarketMinOdds = null;
    currentMarketMaxOdds = null;
    if (currentMarketResultsPayload) renderMarketResults(currentMarketResultsPayload);
  });
})();

// Called in the filter chain inside renderMarketResults
function passesAdvancedMarketFilters(item) {
  // ── Bookmaker filter ──
  if (currentMarketBookFilter) {
    const itemBook = String(
      item?.market?.bookmaker || item?.bookmaker || item?.source || ''
    ).toLowerCase();
    if (itemBook && itemBook !== currentMarketBookFilter) return false;
  }

  // ── Odds range filter — use best odds available on the winning side ──
  const side = String(item?.best_bet?.side || item?.best_bet?.display_side || '').toUpperCase();
  const bestOdds = item?.best_bet?.best_odds != null
    ? Number(item.best_bet.best_odds)
    : side.includes('UNDER')
      ? Number(item?.market?.under_odds ?? 0)
      : Number(item?.market?.over_odds ?? 0);

  if (currentMarketMinOdds != null && bestOdds > 0 && bestOdds < currentMarketMinOdds) return false;
  if (currentMarketMaxOdds != null && bestOdds > 0 && bestOdds > currentMarketMaxOdds) return false;

  return true;
}


// ══════════════════════════════════════════════════════════════════════
// ── KEY VAULT ─────────────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════

(function initKeyVault() {
  const kvKeyInput      = document.getElementById('kvKeyInput');
  const kvProviderSelect = document.getElementById('kvProviderSelect');
  const kvAddBtn        = document.getElementById('kvAddBtn');
  const kvRefreshAllBtn = document.getElementById('kvRefreshAllBtn');
  const kvKeyList       = document.getElementById('kvKeyList');
  const kvActiveLabel   = document.getElementById('kvActiveLabel');
  const kvVaultSummary  = document.getElementById('kvVaultSummary');
  const kvStatus        = document.getElementById('keyVaultStatus');
  const kvHealthHeadline = document.getElementById('kvHealthHeadline');
  const kvHealthDetail = document.getElementById('kvHealthDetail');
  const kvHealthyCount = document.getElementById('kvHealthyCount');
  const kvSoftDisabledCount = document.getElementById('kvSoftDisabledCount');
  const kvUncheckedCount = document.getElementById('kvUncheckedCount');

  if (!kvKeyList) return;

  // ── Helpers ───────────────────────────────────────────────────────────
  function loadVault() {
    return loadOddsKeyVault(null);
  }
  function saveVault(vault) {
    return saveOddsKeyVault(vault, { activeId: oddsKeyVaultActiveId });
  }
  function getActiveId() {
    return oddsKeyVaultActiveId || '';
  }
  function setActiveId(id) {
    oddsKeyVaultActiveId = String(id || '');
    return saveOddsKeyVault(loadVault(), { activeId: oddsKeyVaultActiveId });
  }
  function maskKey(key) {
    if (!key || key.length < 8) return '••••••••';
    return key.slice(0, 4) + '••••••••' + key.slice(-4);
  }
  function esc(s) { return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
  function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2, 6); }
  function nextAutoLabel(vault, provider) {
    const prefix = provider === 'odds_api' ? 'Odds API Key' : 'API Key';
    const used = new Set(vault.map(entry => String(entry.label || '')));
    let idx = vault.filter(entry => entry.provider === provider).length + 1;
    while (used.has(`${prefix} ${idx}`)) idx += 1;
    return `${prefix} ${idx}`;
  }

  // Push active key into Market Scanner + Parlay Builder inputs
  function syncActiveKeyToPages(key) {
    return key;
  }

  function setKvStatus(msg, tone = 'neutral') {
    if (!kvStatus) return;
    kvStatus.textContent = msg;
    kvStatus.dataset.tone = tone;
  }

  function formatVaultTimestamp(value) {
    if (!value) return 'never';
    const parsed = Date.parse(value);
    if (!Number.isFinite(parsed)) return 'unknown';
    return new Date(parsed).toLocaleString();
  }

  // ── Render key list ───────────────────────────────────────────────────
  function renderVault() {
    const vault = loadVault();
    const activeId = getActiveId();

    const oddsVault = vault.filter(k => k.provider === 'odds_api');
    const healthyCount = oddsVault.filter(k => getVaultHealth(k).state === 'healthy').length;
    const softReserveCount = oddsVault.filter(k => getVaultHealth(k).softDisabled && getVaultHealth(k).usable).length;
    const criticalCount = oddsVault.filter(k => getVaultHealth(k).state === 'critical').length;
    const uncheckedCount = oddsVault.filter(k => getVaultHealth(k).state === 'unknown').length;
    const usableCount = healthyCount + softReserveCount;
    const lowCount = softReserveCount + criticalCount;
    if (kvActiveLabel) {
      kvActiveLabel.textContent = usableCount >= KEY_VAULT_MIN_ROTATING_KEYS
        ? `Rotation ready: ${usableCount} usable keys`
        : `Rotation needs ${Math.max(KEY_VAULT_MIN_ROTATING_KEYS - usableCount, 0)} more usable key${Math.max(KEY_VAULT_MIN_ROTATING_KEYS - usableCount, 0) === 1 ? '' : 's'}`;
    }
    if (kvVaultSummary) {
      kvVaultSummary.textContent = `${oddsVault.length} saved Odds API key${oddsVault.length === 1 ? '' : 's'} • ${usableCount} usable • ${lowCount} low-credit • minimum rotating pool: ${KEY_VAULT_MIN_ROTATING_KEYS}`;
    }

    if (kvHealthyCount) kvHealthyCount.textContent = String(healthyCount);
    if (kvSoftDisabledCount) kvSoftDisabledCount.textContent = String(lowCount);
    if (kvUncheckedCount) kvUncheckedCount.textContent = String(uncheckedCount);
    if (kvHealthHeadline) {
      kvHealthHeadline.textContent = !oddsVault.length
        ? 'Waiting for keys'
        : healthyCount >= KEY_VAULT_MIN_ROTATING_KEYS
          ? 'Rotation healthy'
          : lowCount
            ? 'Needs credit attention'
            : 'Needs more checked keys';
    }
    if (kvHealthDetail) {
      kvHealthDetail.textContent = !oddsVault.length
        ? 'Add and check keys to see vault health.'
        : healthyCount >= KEY_VAULT_MIN_ROTATING_KEYS
          ? `${healthyCount} healthy key${healthyCount === 1 ? '' : 's'} can rotate safely right now.`
          : `${Math.max(KEY_VAULT_MIN_ROTATING_KEYS - healthyCount, 0)} more healthy key${Math.max(KEY_VAULT_MIN_ROTATING_KEYS - healthyCount, 0) === 1 ? '' : 's'} needed for a safe pool.`;
    }

    if (!vault.length) {
      kvKeyList.innerHTML = `
        <div class="bet-finder-state empty-state-panel compact" style="padding:32px 0">
          <div class="empty-icon">🔑</div>
          <strong>No keys saved yet.</strong>
          <span>Add a key above to get started.</span>
        </div>`;
      return;
    }

    kvKeyList.innerHTML = vault.map(entry => {
      const isActive = entry.id === activeId;
      const health = getVaultHealth(entry);
      return `
        <div class="kv-key-row ${isActive ? 'kv-active' : ''} health-${esc(health.state)}" data-id="${esc(entry.id)}" style="
          display:flex;align-items:center;gap:12px;padding:12px 16px;
          border-radius:14px;margin-bottom:8px;border:1px solid var(--border);
          background:${isActive ? 'rgba(var(--accent-rgb),0.10)' : 'var(--panel-strong)'};
          transition:background 0.2s,border-color 0.2s;
          ${isActive ? 'border-color:rgba(var(--accent-rgb),0.35)' : ''}
        ">
          <div style="flex:1;min-width:0">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:2px">
              <strong style="font-size:.9rem">${esc(entry.label)}</strong>
              ${isActive ? '<span class="small-badge" style="background:rgba(var(--accent-rgb),0.18);color:var(--accent)">● Active</span>' : ''}
              <span class="small-badge">${esc(entry.provider === 'odds_api' ? 'Odds API' : entry.provider)}</span>
              <span class="small-badge kv-health-badge ${esc(health.tone)}">${esc(health.label)}</span>
            </div>
            <code style="font-size:.78rem;opacity:0.55;letter-spacing:.05em">${esc(maskKey(entry.key))}</code>
          </div>
          <div style="display:flex;gap:8px;flex-shrink:0">
            <button class="secondary-btn kv-credits-btn" data-id="${esc(entry.id)}" style="padding:6px 14px;font-size:.8rem" type="button" title="Check remaining credits">💳 Credits</button>
            <button class="text-btn kv-delete-btn" data-id="${esc(entry.id)}" style="padding:6px 10px;font-size:.8rem;color:var(--bad);opacity:0.7" type="button" title="Remove key">✕</button>
          </div>
        </div>`;
    }).join('');

    kvKeyList.querySelectorAll('.kv-key-row').forEach((row, index) => {
      const entry = vault[index];
      if (!entry) return;
      const health = getVaultHealth(entry);
      row.classList.remove('kv-active');
      row.style.background = 'var(--panel-strong)';
      row.style.borderColor = 'var(--border)';
      const activateBtn = row.querySelector('.kv-activate-btn');
      if (activateBtn) activateBtn.remove();
      const badges = row.querySelector('div[style*="margin-bottom:2px"]');
      if (badges && !badges.querySelector('[data-kv-credit-badge]')) {
        const creditBadge = document.createElement('span');
        creditBadge.className = 'small-badge';
        creditBadge.dataset.kvCreditBadge = '1';
        creditBadge.textContent = Number.isFinite(Number(entry.remaining)) ? `Credits ${entry.remaining}` : 'Credits unknown';
        creditBadge.classList.add(`tone-${health.tone}`);
        badges.appendChild(creditBadge);
      }
      const legacyActiveBadge = Array.from(row.querySelectorAll('.small-badge')).find(el => /active/i.test(el.textContent || ''));
      if (legacyActiveBadge) legacyActiveBadge.remove();
      const codeEl = row.querySelector('code');
      if (codeEl && !row.querySelector('[data-kv-credit-meta]')) {
        const meta = document.createElement('div');
        meta.className = 'small-meta';
        meta.dataset.kvCreditMeta = '1';
        meta.style.marginTop = '6px';
        meta.textContent = Number.isFinite(Number(entry.remaining))
          ? `Remaining ${entry.remaining} • Used ${entry.used ?? '—'} • Checked ${entry.last_checked_at ? new Date(entry.last_checked_at).toLocaleString() : 'just now'}`
          : 'Balance not checked yet';
        codeEl.insertAdjacentElement('afterend', meta);
      }
    });

    // Bind buttons
    kvKeyList.querySelectorAll('.kv-delete-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        if (!confirm('Remove this key from the vault?')) return;
        await deleteOddsKeyVaultEntry(btn.dataset.id);
        renderVault();
      });
    });

    kvKeyList.querySelectorAll('.kv-credits-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        const vault = loadVault();
        const entry = vault.find(k => k.id === btn.dataset.id);
        if (!entry) return;
        const orig = btn.textContent;
        btn.textContent = 'Checking…';
        btn.disabled = true;
        try {
          const refreshed = await refreshOddsVaultEntryCredits(entry, {
            force: true,
            onLowCredits: async (lowEntry) => promptDeleteLowCreditVaultKey(lowEntry, 'rotation'),
          });
          const used = refreshed?.used ?? '?';
          const remaining = refreshed?.remaining ?? '?';
          setKvStatus(`"${entry.label}" — Used: ${used} • Remaining: ${remaining}`, 'good');
          renderVault();
          setTimeout(() => setKvStatus('Ready'), 5000);
        } catch (err) {
          setKvStatus('Credits check failed: ' + (err.message || 'Unknown error'), 'bad');
          setTimeout(() => setKvStatus('Ready'), 4000);
        } finally {
          btn.textContent = orig;
          btn.disabled = false;
        }
      });
    });
  }

  // ── Add key ───────────────────────────────────────────────────────────
  kvAddBtn?.addEventListener('click', async () => {
    await ensureOddsKeyVaultLoaded().catch(function () { });
    const rawKeys = (kvKeyInput?.value || '').trim();
    const provider = kvProviderSelect?.value || 'odds_api';

    if (!rawKeys) { setKvStatus('Please paste at least one API key.', 'bad'); return; }

    const parsedKeys = rawKeys
      .split(/[\s,]+/)
      .map(part => part.trim())
      .filter(Boolean);
    if (!parsedKeys.length) {
      setKvStatus('Please paste at least one valid API key.', 'bad');
      return;
    }

    const vault = loadVault();
    const seenExisting = new Set(vault.map(entry => entry.key));
    const uniqueIncoming = [...new Set(parsedKeys)];
    const addedEntries = [];
    let duplicateCount = 0;
    uniqueIncoming.forEach(key => {
      if (seenExisting.has(key)) {
        duplicateCount += 1;
        return;
      }
      const newEntry = {
        id: uid(),
        label: nextAutoLabel(vault, provider),
        key,
        provider,
        added: new Date().toISOString(),
        remaining: null,
        used: null,
        last_cost: null,
        last_checked_at: null,
      };
      vault.push(newEntry);
      seenExisting.add(key);
      addedEntries.push(newEntry);
    });

    if (!addedEntries.length) {
      setKvStatus('Those keys are already saved in the vault.', 'bad');
      return;
    }

    invalidateEligibleVaultKeyCache();
    await saveVault(vault);

    // Auto-activate if first key
    if (vault.length === addedEntries.length) {
      await setActiveId(addedEntries[0].id);
      syncActiveKeyToPages(addedEntries[0].key);
    }

    if (kvKeyInput) kvKeyInput.value = '';
    const duplicateText = duplicateCount ? ` ${duplicateCount} duplicate${duplicateCount === 1 ? '' : 's'} skipped.` : '';
    setKvStatus(`${addedEntries.length} key${addedEntries.length === 1 ? '' : 's'} saved.${duplicateText} Add at least ${KEY_VAULT_MIN_ROTATING_KEYS} usable keys for automatic rotation.`, 'good');
    setTimeout(() => setKvStatus('Ready'), 2500);
    renderVault();
  });

  kvRefreshAllBtn?.addEventListener('click', async () => {
    await ensureOddsKeyVaultLoaded().catch(function () { });
    const oddsVault = loadVault().filter(entry => entry.provider === 'odds_api');
    if (!oddsVault.length) {
      setKvStatus('Add Odds API keys first.', 'bad');
      return;
    }
    const orig = kvRefreshAllBtn.textContent;
    kvRefreshAllBtn.disabled = true;
    kvRefreshAllBtn.textContent = 'Checking…';
    setKvStatus('Checking all key balances…', 'working');
    try {
      for (const entry of oddsVault) {
        await refreshOddsVaultEntryCredits(entry, {
          force: true,
          onLowCredits: async (lowEntry) => promptDeleteLowCreditVaultKey(lowEntry, 'rotation'),
        });
      }
      renderVault();
      setKvStatus('All key balances checked.', 'good');
    } catch (error) {
      setKvStatus(error.message || 'Failed to refresh balances.', 'bad');
    } finally {
      kvRefreshAllBtn.disabled = false;
      kvRefreshAllBtn.textContent = orig;
    }
  });

  // ── Init: render vault and sync active key on load ────────────────────
  (async function bootstrapVaultUi() {
    try {
      await ensureOddsKeyVaultLoaded();
    } catch (error) {
      setKvStatus('Could not load Key Vault.', 'bad');
    }
    renderVault();
    const vault = loadVault();
    const activeId = getActiveId();
    const activeEntry = vault.find(k => k.id === activeId);
    if (activeEntry) syncActiveKeyToPages(activeEntry.key);
  })();
})();
