const teamSelect = document.getElementById('teamSelect');
const playerSearchInput = document.getElementById('playerSearch');
const searchResults = document.getElementById('searchResults');
const selectedPlayerBadge = document.getElementById('selectedPlayer');
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

const RECENT_PLAYERS_KEY = 'nba-props-recent-players';
const THEME_KEY = 'nba-props-theme';
const SIDEBAR_COLLAPSED_KEY = 'nba-props-sidebar-collapsed';
const MARKET_RESULTS_KEY = 'nba-props-latest-market-results';
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
let latestTodayGamesPayload = null;
let currentGameLogPayload = null;
let activeGameLogView = 'recent';

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
  }
};

function switchView(view) {
  if (!VIEW_META[view]) return;

  activeView = view;

  dashboardViews.forEach(section => {
    section.classList.toggle('active', section.dataset.view === view);
  });

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
  window.scrollTo({ top: 0, behavior: 'smooth' });
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

function formatStoredTime(value) {
  if (!value) return 'Latest saved board unavailable.';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'Latest saved board available.';
  return `Updated ${date.toLocaleString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })}`;
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

  const availabilityHtml = selectedPlayer.availability
    ? `<div class="selected-player-meta-row">${renderAvailabilityBadge(selectedPlayer.availability)}<small>${escapeHtml(selectedPlayer.availability.reason || selectedPlayer.availability.note || '')}</small></div>`
    : '';

  overviewCurrentCard.innerHTML = `
    <img class="overview-current-avatar-img" src="${getPlayerImage(selectedPlayer.id)}" alt="${escapeHtml(selectedPlayer.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
    <div class="overview-current-copy">
      <span class="selected-player-label">Current focus</span>
      <strong>${escapeHtml(selectedPlayer.full_name)}</strong>
      <small>${escapeHtml(subLine || (selectedPlayer.is_active ? 'Active player' : 'Player'))}</small>
      ${availabilityHtml}
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

function setStatus(text) {
  statusPill.textContent = text;
}

function setTheme(theme) {
  document.body.classList.toggle('light-theme', theme === 'light');
  localStorage.setItem(THEME_KEY, theme);
  themeToggle.querySelector('.theme-icon').textContent = theme === 'light' ? '🌙' : '☀';
  themeToggle.querySelector('.theme-text').textContent = theme === 'light' ? 'Dark' : 'Light';

  if (lastPayload) {
    renderChart(lastPayload);
  }
}

function applySavedTheme() {
  const saved = localStorage.getItem(THEME_KEY) || 'dark';
  setTheme(saved);
}

function applyTeamAccent(teamAbbreviation) {
  const palette = TEAM_COLORS[teamAbbreviation] || { accent: '#4da3ff', accent2: '#8571ff', rgb: '77, 163, 255' };
  document.documentElement.style.setProperty('--accent', palette.accent);
  document.documentElement.style.setProperty('--accent-2', palette.accent2);
  document.documentElement.style.setProperty('--accent-rgb', palette.rgb);

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
  const availabilityHtml = selectedPlayer.availability
    ? `<div class="selected-player-meta-row">${renderAvailabilityBadge(selectedPlayer.availability)}<small>${escapeHtml(selectedPlayer.availability.reason || selectedPlayer.availability.note || '')}</small></div>`
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

async function loadTeams() {
  const response = await fetch('/api/teams');
  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    teamSelect.innerHTML = '<option value="">Failed to load teams</option>';
    throw new Error(data.detail || 'Failed to load teams.');
  }

  teamSelect.innerHTML = '';
  const placeholder = document.createElement('option');
  placeholder.value = '';
  placeholder.textContent = 'Choose a team';
  teamSelect.appendChild(placeholder);

  (data.results || []).forEach(team => {
    const option = document.createElement('option');
    option.value = String(team.id);
    option.textContent = team.full_name;
    option.dataset.abbreviation = team.abbreviation;
    option.dataset.name = team.full_name;
    teamSelect.appendChild(option);
  });
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
  rosterTitle.textContent = `${team.full_name} roster`;
  rosterMeta.textContent = `${players.length} players • ${season}`;

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
  playerGrid.innerHTML = players.map(player => `
    <button class="player-card" data-id="${player.id}">
      <img src="${getPlayerImage(player.id)}" alt="${escapeHtml(player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
      <div class="player-card-head">
        <div class="player-card-name">${escapeHtml(player.full_name)}</div>
        <span class="jersey-pill">${escapeHtml(player.jersey || '--')}</span>
      </div>
      <div class="player-card-meta">${escapeHtml(player.position || 'Position N/A')}</div>
      <div class="player-card-team">${escapeHtml(player.team_abbreviation || team.abbreviation)}</div>
    </button>
  `).join('');

  playerGrid.querySelectorAll('.player-card').forEach((card, index) => {
    card.addEventListener('click', () => {
      const player = players[index];
      setSelectedPlayer(player);
    });
  });

  updateSelectedCardStyles();
  updateRosterScrollState(players, season);
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
  return `
    <article class="today-game-card ${compact ? 'compact' : ''} ${statusClass}">
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

function renderBetFinderResults(payload) {
  const results = payload.results || [];

  if (!results.length) {
    renderBetFinderEmpty('No players on this roster met the sample requirement for the current prop line.');
    return;
  }

  betFinderMeta.textContent = `${payload.team.full_name} • ${getStatLabel(payload.stat)} ${payload.line} • Last ${payload.last_n} • Min ${payload.min_games} games`;
  betFinderResults.className = 'bet-finder-grid';
  betFinderResults.innerHTML = results.map((item, index) => {
    const tone = getFinderTone(item.hit_rate);
    return `
      <button class="finder-card ${tone}" type="button"
        data-id="${item.player.id}"
        data-name="${escapeHtml(item.player.full_name)}"
        data-team-id="${item.player.team_id}"
        data-team-name="${escapeHtml(item.player.team_name || '')}"
        data-team-abbr="${escapeHtml(item.player.team_abbreviation || '')}"
        data-position="${escapeHtml(item.player.position || '')}"
        data-jersey="${escapeHtml(item.player.jersey || '')}">
        <div class="finder-rank">#${index + 1}</div>
        <div class="finder-player">
          <img src="${getPlayerImage(item.player.id)}" alt="${escapeHtml(item.player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
          <div>
            <strong>${escapeHtml(item.player.full_name)}</strong>
            <span>${escapeHtml(item.player.position || 'Position N/A')} • ${escapeHtml(item.player.team_abbreviation || '')}</span>
          </div>
        </div>
        <div class="finder-chip-row">
          <span class="finder-chip">${item.hit_rate.toFixed(1)}% hit</span>
          <span class="finder-chip">${item.hit_count}/${item.games_count}</span>
          <span class="finder-chip">Avg ${item.average.toFixed(1)}</span>
          <span class="finder-chip ${item.avg_edge >= 0 ? 'positive' : 'negative'}">Edge ${item.avg_edge >= 0 ? '+' : ''}${item.avg_edge.toFixed(1)}</span>
        </div>
        <div class="finder-footer">
          <span class="finder-badge ${tone}">${getFinderLabel(item.hit_rate)}</span>
          <small>Last ${item.last_value.toFixed(1)} • Over streak ${item.hit_streak}</small>
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
    ? `${nextGame.matchup_label} • ${nextGame.game_date || 'Date TBA'}${nextGame.game_time ? ` • ${nextGame.game_time}` : ''}`
    : 'Upcoming game unavailable';

  const venueLabel = nextGame
    ? (nextGame.is_home ? 'Home game' : 'Away game')
    : 'Venue unavailable';

  let summaryText = 'Defense-vs-position data unavailable for this player and stat.';
  if (vsPosition) {
    summaryText = `${nextGame?.opponent_name || 'This opponent'} allows ${vsPosition.opponent_value.toFixed(2)} ${getStatLabel(vsPosition.stat).toLowerCase()} per player-game to ${vsPosition.position_label.toLowerCase()}, versus a league baseline of ${vsPosition.league_average.toFixed(2)} (${formatDelta(vsPosition.delta_pct)}%).`;
  }

  const bodyHtml = `
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
    badge.className = `spotlight-pill ${leanTone}`;
    badge.textContent = leanText;
    body.className = 'matchup-body';
    body.innerHTML = bodyHtml;
  });
}

function renderInterpretationPanels(payload) {
  const interpretation = payload?.interpretation || {};
  const opportunity = payload?.opportunity || {};
  const teamContext = payload?.team_context || {};
  const toneMap = { good: 'good', warning: 'warning', bad: 'bad', neutral: 'neutral' };
  const interpretationToneClass = toneMap[interpretation.tone] || 'neutral';
  const opportunityToneClass = opportunity.minutes_trend === 'up' || opportunity.volume_trend === 'up'
    ? 'good'
    : (teamContext.impact_count ? 'warning' : 'neutral');

  if (interpretationTone) {
    interpretationTone.className = `spotlight-pill ${interpretationToneClass}`;
    interpretationTone.textContent = interpretation.headline || 'Quick read';
  }
  if (opportunityTone) {
    opportunityTone.className = `spotlight-pill ${opportunityToneClass}`;
    opportunityTone.textContent = opportunity.minutes_label || 'Opportunity';
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
    const listedPlayers = (teamContext.players || []).map(item => `${item.name} (${item.status})`);
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
  const labels = payload.games.map(game => game.matchup);
  const values = payload.games.map(game => game.value);
  const hits = payload.games.map(game => game.hit);
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
            label(context) {
              const value = context.raw;
              const verdict = value >= payload.line ? 'Over' : 'Under';
              return `${getStatLabel(payload.stat)}: ${value} (${verdict})`;
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
              return labels[index].replace(/\s+vs\.|\s+@/g, ' ');
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
    plugins: [linePlugin]
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

function buildGameLogRowsMarkup(games, emptyTitle, emptySubtitle) {
  if (!games || !games.length) {
    return `
      <tr>
        <td colspan="8">
          <div class="empty-state-panel compact">
            <div class="empty-icon">📊</div>
            <strong>${escapeHtml(emptyTitle)}</strong>
            <span>${escapeHtml(emptySubtitle)}</span>
          </div>
        </td>
      </tr>
    `;
  }

  return games.slice().reverse().map(game => `
    <tr>
      <td>${game.game_date}</td>
      <td>${game.matchup}</td>
      <td class="${game.hit ? 'hit-value' : 'miss-value'}">${game.value}</td>
      <td>${Number(game.minutes || 0).toFixed(1)}</td>
      <td>${Number(game.fga || 0).toFixed(1)}</td>
      <td>${Number(game.fg3a || 0).toFixed(1)}</td>
      <td>${Number(game.fta || 0).toFixed(1)}</td>
      <td><span class="result-badge ${game.hit ? 'hit' : 'miss'}">${game.hit ? 'Hit' : 'Miss'}</span></td>
    </tr>
  `).join('');
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
    gamesTableBody.innerHTML = buildGameLogRowsMarkup([], 'No data yet.', 'Analyze a player prop to fill the game log.');
    return;
  }

  if (view === 'h2h') {
    const h2h = currentGameLogPayload.h2h || {};
    const oppLabel = h2h.opponent_abbreviation || h2h.opponent_name || 'opponent';
    const games = h2h.games || [];
    if (games.length) {
      gameLogMeta.textContent = `${h2h.hit_count}/${h2h.games_count} overs • Avg ${Number(h2h.average || 0).toFixed(1)} vs ${oppLabel} • Minutes and attempts included`;
      gamesTableBody.innerHTML = buildGameLogRowsMarkup(games, `No H2H games vs ${oppLabel} yet.`, 'No current-season meetings found for this next opponent.');
    } else {
      gameLogMeta.textContent = `H2H vs ${oppLabel}`;
      gamesTableBody.innerHTML = buildGameLogRowsMarkup([], `No H2H games vs ${oppLabel} yet.`, 'No current-season meetings found for this next opponent.');
    }
    return;
  }

  gameLogMeta.textContent = `Last ${currentGameLogPayload.last_n} games • Value, minutes, and attempts`;
  gamesTableBody.innerHTML = buildGameLogRowsMarkup(currentGameLogPayload.games || [], 'No data yet.', 'Analyze a player prop to fill the game log.');
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
    await loadRoster(teamId);
  }

  setSelectedPlayer({
    id: item.player.id,
    full_name: item.player.full_name,
    is_active: true,
    team_abbreviation: item.player.team_abbreviation || '',
    team_name: item.player.team_name || '',
    team_id: item.player.team_id || null,
    position: item.player.position || '',
    jersey: item.player.jersey || ''
  });

  setActiveProp(item.market.stat);
  lineInput.value = item.market.line;
  switchView('analyzer');
  await analyzePlayerProp();
}

function renderMarketResults(payload) {
  const results = payload.results || [];

  if (!results.length) {
    renderMarketEmpty('No rows produced a usable result. Check names and try again.');
    return;
  }

  marketMeta.textContent = `${results.length} props scanned • Sorted by best edge and EV`;
  marketResults.className = 'market-results-shell';
  marketResults.innerHTML = `
    <div class="market-results-table-wrap">
      <table class="market-results-table">
        <thead>
          <tr>
            <th>Player</th>
            <th>Availability</th>
            <th>Prop</th>
            <th>Best side</th>
            <th>Edge</th>
            <th>EV</th>
            <th>Model %</th>
            <th>Implied %</th>
            <th>Last ${payload.last_n}</th>
            <th>Average</th>
            <th>Matchup</th>
          </tr>
        </thead>
        <tbody>
          ${results.map((item, index) => {
    const tone = getConfidenceTone(item.best_bet.confidence);
    const availability = item.analysis?.availability || item.availability || { status: 'Unknown', tone: 'neutral', reason: 'No report found', note: '' };
    const matchupData = item.analysis?.matchup || item.matchup || {};
    const matchupLean = matchupData?.vs_position?.lean || 'No matchup';
    const matchupDetail = matchupData?.next_game?.matchup_label || 'No next opponent';
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

  marketResults.querySelectorAll('.market-row').forEach(row => {
    row.addEventListener('click', async () => {
      const item = results[Number(row.dataset.index)];
      if (!item) return;
      try {
        setStatus('Loading market pick');
        await focusMarketPlayer(item);
        setStatus('Ready');
      } catch (error) {
        console.error(error);
        alert(error.message || 'Failed to open that market pick.');
        setStatus('Error');
      }
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

  if (chart) {
    chart.destroy();
    chart = null;
  }
  lastPayload = null;
}

async function analyzePlayerProp() {
  switchView('analyzer');
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

    const response = await fetch(`/api/player-prop?${params.toString()}`);
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.detail || 'Failed to analyze player prop.');
    }

    selectedPlayer = {
      ...selectedPlayer,
      team_id: payload.player.team_id || selectedPlayer.team_id,
      position: payload.player.position || selectedPlayer.position,
      availability: payload.availability || null
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
clearRecentBtn.addEventListener('click', () => clearRecentPlayersState({ resetCurrent: true }));
marketTemplateBtn.addEventListener('click', () => { marketTextarea.value = getMarketTemplate(); });
marketClearBtn.addEventListener('click', () => { marketTextarea.value = ''; renderMarketEmpty(); });
marketScanBtn.addEventListener('click', runMarketScan);
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

document.addEventListener('click', (event) => {
  if (!playerSearchInput.contains(event.target) && !searchResults.contains(event.target)) {
    searchResults.classList.add('hidden');
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