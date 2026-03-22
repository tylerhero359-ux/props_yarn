const teamSelect = document.getElementById('teamSelect');
const playerSearchInput = document.getElementById('playerSearch');
const searchResults = document.getElementById('searchResults');
const selectedPlayerBadge = document.getElementById('selectedPlayer');
const playerGrid = document.getElementById('playerGrid');
const rosterTitle = document.getElementById('rosterTitle');
const rosterMeta = document.getElementById('rosterMeta');
const recentPlayersSection = document.getElementById('recentPlayersSection');
const recentPlayersContainer = document.getElementById('recentPlayers');
const analyzeBtn = document.getElementById('analyzeBtn');
const statSelect = document.getElementById('statSelect');
const lineInput = document.getElementById('lineInput');
const gamesSelect = document.getElementById('gamesSelect');
const seasonInput = document.getElementById('seasonInput');
const chartTitle = document.getElementById('chartTitle');
const chartSubtitle = document.getElementById('chartSubtitle');
const statusPill = document.getElementById('statusPill');
const avgValue = document.getElementById('avgValue');
const hitRateValue = document.getElementById('hitRateValue');
const hitCountValue = document.getElementById('hitCountValue');
const seasonValue = document.getElementById('seasonValue');
const gamesTableBody = document.getElementById('gamesTableBody');

const RECENT_PLAYERS_KEY = 'nba-props-recent-players';
const FALLBACK_HEADSHOT = encodeURIComponent(`
  <svg xmlns="http://www.w3.org/2000/svg" width="240" height="240" viewBox="0 0 240 240">
    <defs>
      <linearGradient id="g" x1="0" x2="1" y1="0" y2="1">
        <stop offset="0%" stop-color="#243452" />
        <stop offset="100%" stop-color="#121b31" />
      </linearGradient>
    </defs>
    <rect width="240" height="240" rx="120" fill="url(#g)" />
    <circle cx="120" cy="92" r="44" fill="#5f9bff" fill-opacity="0.92" />
    <path d="M54 202c11-32 39-52 66-52s55 20 66 52" fill="#5f9bff" fill-opacity="0.92" />
  </svg>
`);

let selectedPlayer = null;
let chart = null;
let searchTimeout = null;
let rosterPlayers = [];

function getFallbackHeadshot() {
  return `data:image/svg+xml;charset=UTF-8,${FALLBACK_HEADSHOT}`;
}

function getPlayerImage(playerId) {
  return `https://cdn.nba.com/headshots/nba/latest/1040x760/${playerId}.png`;
}

function showStatus(text) {
  statusPill.textContent = text;
}

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
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
      team_name: player.team_name || ''
    },
    ...existing
  ].slice(0, 6);

  localStorage.setItem(RECENT_PLAYERS_KEY, JSON.stringify(trimmed));
  renderRecentPlayers();
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
    <button class="mini-player-card" data-id="${player.id}" data-name="${escapeHtml(player.full_name)}" data-active="${player.is_active}" data-team-abbr="${escapeHtml(player.team_abbreviation || '')}" data-team-name="${escapeHtml(player.team_name || '')}">
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
        team_name: card.dataset.teamName
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

function setSelectedPlayer(player) {
  selectedPlayer = player;
  selectedPlayerBadge.classList.remove('selected-player-empty');
  selectedPlayerBadge.innerHTML = `
    <img src="${getPlayerImage(player.id)}" alt="${escapeHtml(player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
    <div class="selected-player-copy">
      <span class="selected-player-label">Selected player</span>
      <strong>${escapeHtml(player.full_name)}</strong>
      <small>${escapeHtml(player.team_name || player.team_abbreviation || (player.is_active ? 'Active player' : 'Player'))}</small>
    </div>
  `;

  playerSearchInput.value = player.full_name;
  searchResults.classList.add('hidden');
  updateSelectedCardStyles();
  saveRecentPlayer(player);
}

async function loadTeams() {
  const response = await fetch('/api/teams');
  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    teamSelect.innerHTML = '<option value="">Failed to load teams</option>';
    throw new Error(data.detail || 'Failed to load teams. Make sure main.py was updated and the server was restarted.');
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
    teamSelect.appendChild(option);
  });

  teamSelect.selectedIndex = 0;
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
  renderRoster(payload.team, payload.season, rosterPlayers);
}

function renderRoster(team, season, players) {
  rosterTitle.textContent = `${team.full_name} roster`;
  rosterMeta.textContent = `${players.length} players • ${season}`;

  if (!players.length) {
    playerGrid.classList.add('empty-grid');
    playerGrid.innerHTML = '<div class="empty-roster-state">No players found for this team and season.</div>';
    return;
  }

  playerGrid.classList.remove('empty-grid');
  playerGrid.innerHTML = players.map(player => `
    <button class="player-card" data-id="${player.id}">
      <img src="${getPlayerImage(player.id)}" alt="${escapeHtml(player.full_name)}" onerror="this.onerror=null;this.src='${getFallbackHeadshot()}'">
      <div class="player-card-name">${escapeHtml(player.full_name)}</div>
      <div class="player-card-meta">${escapeHtml(player.jersey || '--')} ${escapeHtml(player.position || '')}</div>
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
    searchResults.innerHTML = '<div class="search-item"><small>No players found.</small></div>';
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
        is_active: item.dataset.active === 'true'
      });
    });
  });
}

function renderChart(payload) {
  const labels = payload.games.map(game => game.matchup);
  const values = payload.games.map(game => game.value);
  const hits = payload.games.map(game => game.hit);

  const linePlugin = {
    id: 'linePlugin',
    afterDatasetsDraw(chartInstance) {
      const { ctx, scales: { y } } = chartInstance;
      const yValue = y.getPixelForValue(payload.line);

      ctx.save();
      ctx.beginPath();
      ctx.moveTo(chartInstance.chartArea.left, yValue);
      ctx.lineTo(chartInstance.chartArea.right, yValue);
      ctx.lineWidth = 2;
      ctx.strokeStyle = '#ffd166';
      ctx.setLineDash([6, 6]);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#ffd166';
      ctx.fillText(`Line: ${payload.line}`, chartInstance.chartArea.left + 8, yValue - 8);
      ctx.restore();
    }
  };

  if (chart) chart.destroy();

  chart = new Chart(document.getElementById('propsChart'), {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: payload.stat,
        data: values,
        backgroundColor: hits.map(hit => hit ? 'rgba(44, 207, 143, 0.78)' : 'rgba(255, 107, 107, 0.78)'),
        borderRadius: 10,
        borderSkipped: false
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (context) => `${payload.stat}: ${context.raw}`
          }
        }
      },
      scales: {
        x: {
          ticks: { color: '#dce7ff' },
          grid: { display: false }
        },
        y: {
          beginAtZero: true,
          ticks: { color: '#dce7ff' },
          grid: { color: 'rgba(255,255,255,0.08)' }
        }
      }
    },
    plugins: [linePlugin]
  });
}

function renderSummary(payload) {
  avgValue.textContent = payload.average;
  hitRateValue.textContent = `${payload.hit_rate}%`;
  hitCountValue.textContent = `${payload.hit_count}/${payload.games_count}`;
  seasonValue.textContent = payload.season;

  chartTitle.textContent = `${payload.player.full_name} • ${payload.stat} vs ${payload.line}`;
  chartSubtitle.textContent = `${payload.season_type} • Last ${payload.last_n} games`;
}

function renderTable(payload) {
  gamesTableBody.innerHTML = payload.games.map(game => `
    <tr>
      <td>${game.game_date}</td>
      <td>${game.matchup}</td>
      <td>${game.value}</td>
      <td class="${game.hit ? 'hit' : 'miss'}">${game.hit ? 'Hit' : 'Miss'}</td>
    </tr>
  `).join('');
}

async function analyzePlayerProp() {
  if (!selectedPlayer) {
    alert('Please select a player first.');
    return;
  }

  showStatus('Loading');
  analyzeBtn.disabled = true;

  try {
    const params = new URLSearchParams({
      player_id: selectedPlayer.id,
      stat: statSelect.value,
      line: lineInput.value,
      last_n: gamesSelect.value
    });

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
    showStatus('Ready');
  } catch (error) {
    console.error(error);
    alert(error.message);
    showStatus('Error');
  } finally {
    analyzeBtn.disabled = false;
  }
}

teamSelect.addEventListener('change', async () => {
  const teamId = teamSelect.value;

  if (!teamId) {
    rosterPlayers = [];
    rosterTitle.textContent = 'Team roster';
    rosterMeta.textContent = 'Choose a team to load players.';
    playerGrid.classList.add('empty-grid');
    playerGrid.innerHTML = '<div class="empty-roster-state">Choose a team to see player cards with headshots.</div>';
    return;
  }

  showStatus('Loading roster');
  try {
    await loadRoster(teamId);
    showStatus('Roster ready');
  } catch (error) {
    console.error(error);
    alert(error.message);
    showStatus('Error');
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
    }
  }, 250);
});

document.addEventListener('click', (event) => {
  if (!searchResults.contains(event.target) && event.target !== playerSearchInput) {
    searchResults.classList.add('hidden');
  }
});

analyzeBtn.addEventListener('click', analyzePlayerProp);

(async function init() {
  renderRecentPlayers();
  try {
    await loadTeams();
    showStatus('Teams ready');
  } catch (error) {
    console.error(error);
    rosterMeta.textContent = 'Teams could not load. Update main.py and restart the backend.';
    alert(error.message || 'Failed to initialize teams list.');
    showStatus('Error');
  }
})();
