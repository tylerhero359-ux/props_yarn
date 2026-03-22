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
const propButtonsWrap = document.getElementById('propButtons');
const themeToggle = document.getElementById('themeToggle');
const matchupPanel = document.getElementById('matchupPanel');
const betFinderMeta = document.getElementById('betFinderMeta');
const betFinderResults = document.getElementById('betFinderResults');
const matchupLeanBadge = document.getElementById('matchupLeanBadge');
const matchupBody = document.getElementById('matchupBody');

const RECENT_PLAYERS_KEY = 'nba-props-recent-players';
const THEME_KEY = 'nba-props-theme';
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
    return;
  }

  const subLine = [
    selectedPlayer.team_name || selectedPlayer.team_abbreviation || '',
    selectedPlayer.position || '',
    selectedPlayer.jersey ? `#${selectedPlayer.jersey}` : ''
  ].filter(Boolean).join(' • ');

  selectedPlayerBadge.className = 'selected-player';
  selectedPlayerBadge.innerHTML = `
    <img src="${getPlayerImage(selectedPlayer.id)}" alt="${escapeHtml(selectedPlayer.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
    <div class="selected-player-copy">
      <span class="selected-player-label">Selected player</span>
      <strong>${escapeHtml(selectedPlayer.full_name)}</strong>
      <small>${escapeHtml(subLine || (selectedPlayer.is_active ? 'Active player' : 'Player'))}</small>
    </div>
  `;
}

function setSelectedPlayer(player) {
  selectedPlayer = player;
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
  playerGrid.innerHTML = Array.from({ length: 8 }).map(() => '<div class="skeleton-card"></div>').join('');
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
  const teamId = teamSelect.value;
  if (!teamId) {
    alert('Please choose a team first.');
    return;
  }

  betFinderBtn.disabled = true;
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

function formatDelta(value) {
  return value > 0 ? `+${value}` : `${value}`;
}

function renderMatchup(payload) {
  const nextGame = payload?.matchup?.next_game;
  const vsPosition = payload?.matchup?.vs_position;

  if (!nextGame && !vsPosition) {
    matchupLeanBadge.className = 'spotlight-pill neutral';
    matchupLeanBadge.textContent = 'No matchup data';
    matchupBody.className = 'empty-state-panel compact matchup-empty';
    matchupBody.innerHTML = `
      <div class="empty-icon">🛡️</div>
      <strong>Matchup context unavailable.</strong>
      <span>We could not resolve the next opponent or position split for this player.</span>
    `;
    return;
  }

  const leanTone = getLeanClass(vsPosition?.lean_tone);
  matchupLeanBadge.className = `spotlight-pill ${leanTone}`;
  matchupLeanBadge.textContent = vsPosition?.lean || (nextGame ? 'Upcoming game found' : 'Partial matchup');

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

  matchupBody.className = 'matchup-body';
  matchupBody.innerHTML = `
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
  `;
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
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: document.body.classList.contains('light-theme') ? 'rgba(255,255,255,0.96)' : 'rgba(10,16,31,0.95)',
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
            maxRotation: 0,
            autoSkip: false,
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
            color: document.body.classList.contains('light-theme') ? 'rgba(17,33,63,0.08)' : 'rgba(255,255,255,0.08)'
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
    ${nextGame ? `<span class="chart-chip">Next ${escapeHtml(nextGame.matchup_label)}</span>` : ''}
    ${vsPosition ? `<span class="chart-chip">Vs ${escapeHtml(vsPosition.position_label)} ${formatDelta(vsPosition.delta_pct)}%</span>` : ''}
  `;

  renderMatchup(payload);
}

function renderTable(payload) {
  gamesTableBody.innerHTML = payload.games.slice().reverse().map(game => `
    <tr>
      <td>${game.game_date}</td>
      <td>${game.matchup}</td>
      <td class="${game.hit ? 'hit-value' : 'miss-value'}">${game.value}</td>
      <td><span class="result-badge ${game.hit ? 'hit' : 'miss'}">${game.hit ? 'Hit' : 'Miss'}</span></td>
    </tr>
  `).join('');
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
  gamesTableBody.innerHTML = `
    <tr>
      <td colspan="4">
        <div class="empty-state-panel compact">
          <div class="empty-icon">📊</div>
          <strong>No data yet.</strong>
          <span>Analyze a player prop to fill the game log.</span>
        </div>
      </td>
    </tr>
  `;
  matchupLeanBadge.className = 'spotlight-pill neutral';
  matchupLeanBadge.textContent = 'Waiting for analysis';
  matchupBody.className = 'empty-state-panel compact matchup-empty';
  matchupBody.innerHTML = `
    <div class="empty-icon">🛡️</div>
    <strong>No matchup loaded yet.</strong>
    <span>Analyze a player prop to load the next opponent and defense-vs-position read.</span>
  `;

  if (chart) {
    chart.destroy();
    chart = null;
  }
  lastPayload = null;
}

async function analyzePlayerProp() {
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
    applyTeamAccent();
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
    return;
  }

  applyTeamAccent(selectedOption.dataset.abbreviation);
  setStatus('Loading roster');
  renderRosterSkeleton();

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

analyzeBtn.addEventListener('click', analyzePlayerProp);
betFinderBtn.addEventListener('click', runBetFinder);
clearRecentBtn.addEventListener('click', () => clearRecentPlayersState({ resetCurrent: true }));

themeToggle.addEventListener('click', () => {
  const nextTheme = document.body.classList.contains('light-theme') ? 'dark' : 'light';
  setTheme(nextTheme);
});

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
  resetDashboardForNoSelection();
  renderBetFinderEmpty();
  setActiveProp(selectedStat);
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
