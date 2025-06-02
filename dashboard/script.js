document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', function () {
        const tabId = this.dataset.tab;

        // Désactiver tous les onglets et contenus
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

        // Activer l'onglet et contenu sélectionnés
        this.classList.add('active');
        document.getElementById(tabId).classList.add('active');

        // Initialiser les graphiques si nécessaire
        initCharts(tabId);
    });
});

Chart.defaults.font.family = "'Segoe UI', sans-serif";
Chart.defaults.color = '#64748b';

let charts = {};

function initCharts(tabId) {
    if (tabId === 'overview' && !charts.overview) {
        const ctx = document.getElementById('overviewChart').getContext('2d');
        charts.overview = new Chart(ctx, {
            type: 'line',
            data: {
                labels: ['Pas de données'],
                datasets: [{
                    label: 'Température',
                    data: [0],
                    borderColor: '#4fc3f7',
                    backgroundColor: 'rgba(79, 195, 247, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { y: { ticks: { callback: value => value + '°C' } } }
            }
        });
    }

    if (tabId === 'analytics' && !charts.extremes) {
        const extremesCtx = document.getElementById('extremesChart').getContext('2d');
        charts.extremes = new Chart(extremesCtx, {
            type: 'bar',
            data: {
                labels: ['Lun', 'Mar', 'Mer', 'Jeu', 'Ven', 'Sam', 'Dim'],
                datasets: [
                    { label: 'Min', data: [null, null, null, null, null, null, null], backgroundColor: '#4fc3f7' },
                    { label: 'Max', data: [null, null, null, null, null, null, null], backgroundColor: '#ef4444' }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: { y: { ticks: { callback: value => value + '°C' } } }
            }
        });

        const windCtx = document.getElementById('windChart').getContext('2d');
        charts.wind = new Chart(windCtx, {
            type: 'radar',
            data: {
                labels: ['N', 'NE', 'E', 'SE', 'S', 'SO', 'O', 'NO'],
                datasets: [{
                    label: 'Vitesse du vent (km/h)',
                    data: [0, 0, 0, 0, 0, 0, 0, 0],
                    backgroundColor: 'rgba(79, 195, 247, 0.2)',
                    borderColor: '#4fc3f7',
                    pointBackgroundColor: '#4fc3f7',
                    pointBorderColor: '#fff',
                    pointHoverRadius: 5
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: { r: { angleLines: { display: true }, suggestedMin: 0 } }
            }
        });
    }

    if (tabId === 'monitoring' && !charts.system) {
        const sysCtx = document.getElementById('systemChart').getContext('2d');
        charts.system = new Chart(sysCtx, {
            type: 'line',
            data: {
                labels: ['10:00', '11:00', '12:00', '13:00', '14:00'],
                datasets: [
                    { label: 'CPU %', data: [45, 52, 48, 65, 58], borderColor: '#ef4444', tension: 0.4 },
                    { label: 'Mémoire %', data: [32, 35, 38, 42, 40], borderColor: '#4fc3f7', tension: 0.4 }
                ]
            },
            options: { responsive: true, maintainAspectRatio: false }
        });
    }
}
// Initialiser les graphiques de la vue d'ensemble au chargement
initCharts('overview');
initCharts('analytics');

// Gestion des contrôles temporels
document.querySelectorAll('.time-btn').forEach(btn => {
    btn.addEventListener('click', function () {
        document.querySelectorAll('.time-btn').forEach(b => b.classList.remove('active'));
        this.classList.add('active');

        // Mise à jour des données du graphique
        const period = this.dataset.period;
        if (charts.overview) {
            const data = {
                '24h': [18.5, 17.2, 21.8, 24.5, 23.2, 20.1],
                '7j': [20.1, 21.5, 22.8, 23.2, 24.1, 22.9],
                '30j': [19.2, 20.8, 22.1, 23.5, 21.9, 20.5]
            };
            charts.overview.data.datasets[0].data = data[period];
            charts.overview.update();
        }
    });
});

// Simulation de mise à jour temps réel
// setInterval(() => {
//     const temp = (Math.random() * 6 + 19).toFixed(1);
//     document.querySelector('.metric-value').textContent = temp + '°C';
// }, 5000);

// Rafraîchir les valeurs actuelles toutes les 10 min
// ============ FONCTIONS UTILITAIRES SÉCURISÉES ============

// ============ FONCTIONS UTILITAIRES SÉCURISÉES ============

// Affichage sécurisé des valeurs numériques
function safeDisplay(value, decimals = 1, unit = '') {
    if (value === null || value === undefined || isNaN(value)) {
        return 'N/A';
    }
    return `${value.toFixed(decimals)}${unit}`;
}

// Afficher message "pas de prédictions"
function displayNoPredictionsMessage() {
    const forecastItems = document.querySelectorAll('.forecast-item');

    forecastItems.forEach((item, index) => {
        const span = item.querySelector('span');
        const strong = item.querySelector('strong');

        if (span) span.textContent = `+${index + 1}h`;
        if (strong) strong.textContent = '--°C';

        item.style.opacity = '0.6';
        item.style.borderLeftColor = '#ff6b6b';
    });
}

// Afficher message "pas de données" dans toutes les cartes
function displayNoDataMessage() {
    const cards = [
        { selector: '.metric-card:nth-child(1)', value: '--°C', change: 'Données indisponibles' },
        { selector: '.metric-card:nth-child(2)', value: '--%', change: null },
        { selector: '.metric-card:nth-child(3)', value: '-- km/h', change: 'Direction: --' },
        { selector: '.metric-card:nth-child(4)', value: '-- hPa', change: null },
        { selector: '.metric-card:nth-child(5)', value: 'Indisponible', change: 'Système hors ligne' }
    ];

    cards.forEach(cardInfo => {
        const card = document.querySelector(cardInfo.selector);
        if (card) {
            const valueEl = card.querySelector('.metric-value');
            const changeEl = card.querySelector('.metric-change');

            if (valueEl) valueEl.textContent = cardInfo.value;
            if (changeEl && cardInfo.change) changeEl.textContent = cardInfo.change;

            // Style discret pour indiquer l'indisponibilité
            card.style.opacity = '0.6';
        }
    });
}

// Vérifier la fraîcheur des données selon le type
function isDataFresh(timestamp, dataType = 'raw') {
    if (!timestamp) return false;

    const now = new Date();
    const dataTime = new Date(timestamp);
    const ageMinutes = (now - dataTime) / (1000 * 60);

    switch (dataType) {
        case 'raw': return ageMinutes <= 20;        // Données 10min : max 20min
        case 'hourly': return ageMinutes <= 75;     // Moyennes horaires : max 1h15
        case 'predictions': return ageMinutes <= 240; // Prédictions : max 4h
        case 'daily': return ageMinutes <= 1440;    // Données journalières : max 24h
        default: return ageMinutes <= 30;
    }
}

// Convertir degrés en direction
function degToDir(deg) {
    if (deg === null || deg === undefined || isNaN(deg)) return 'N/A';
    const dirs = ['N', 'NE', 'E', 'SE', 'S', 'SO', 'O', 'NO'];
    return dirs[Math.round(deg / 45) % 8];
}

// Mettre à jour l'indicateur de statut
function updateStatusIndicator(isFresh, lastUpdate = null) {
    const statusDot = document.querySelector('.status-dot');
    const statusText = document.querySelector('.status-indicator span');

    if (statusDot) {
        if (isFresh) {
            statusDot.style.background = '#4caf50';
            statusDot.style.boxShadow = '0 0 10px #4caf50';
        } else {
            statusDot.style.background = '#ff6b6b';
            statusDot.style.boxShadow = '0 0 10px #ff6b6b';
        }
    }

    if (statusText) {
        if (isFresh && lastUpdate) {
            const updateTime = new Date(lastUpdate).toLocaleTimeString('fr-FR', {
                hour: '2-digit', minute: '2-digit'
            });
            statusText.textContent = `EPT - DIC2 GIT (${updateTime})`;
        } else if (!isFresh) {
            statusText.textContent = 'EPT - DIC2 GIT (DONNÉES OBSOLÈTES)';
        }
    }
}
// ============ MISE À JOUR DES MÉTRIQUES ACTUELLES ============
async function fetchAndUpdateCurrent() {
    try {
        const res = await fetch('http://127.0.0.1:5000/api/latest-raw');

        if (!res.ok) {
            throw new Error(`HTTP ${res.status}`);
        }

        const data = await res.json();
        console.log("Données reçues:", data);

        // Vérifier si on a des données
        if (!data || Object.keys(data).length === 0) {
            console.warn("Aucune donnée reçue de l'API");
            updateStatusIndicator(false);
            return;
        }

        // Vérifier la fraîcheur AVANT d'afficher
        const isFresh = isDataFresh(data.timestamp, 'raw');
        updateStatusIndicator(isFresh, data.timestamp);

        // Si données pas fraîches (>20min), afficher "--" au lieu des valeurs
        if (!isFresh) {
            console.warn(`Données trop anciennes: ${data.data_age_minutes}min`);
            displayNoDataMessage();
            return;
        }

        // Mise à jour sécurisée - SÉLECTEURS SPÉCIFIQUES pour éviter les conflits

        // Température (première carte)
        const tempCard = document.querySelector('.metric-card:nth-child(1)');
        if (tempCard) {
            const tempValue = tempCard.querySelector('.metric-value');
            const tempChange = tempCard.querySelector('.metric-change');
            if (tempValue) tempValue.textContent = safeDisplay(data.temperature, 1, '°C');
            if (tempChange) tempChange.textContent = `Temp ressentie : ${safeDisplay(data.feels_like, 1, '°C')}`;
        }

        // Humidité (deuxième carte)
        const humidCard = document.querySelector('.metric-card:nth-child(2)');
        if (humidCard) {
            const humidValue = humidCard.querySelector('.metric-value');
            if (humidValue) humidValue.textContent = safeDisplay(data.humidity, 0, '%');
        }

        // Vent (troisième carte)
        const windCard = document.querySelector('.metric-card:nth-child(3)');
        if (windCard) {
            const windValue = windCard.querySelector('.metric-value');
            const windChange = windCard.querySelector('.metric-change');
            if (windValue) windValue.textContent = safeDisplay(data.wind_speed, 1, ' km/h');
            if (windChange) windChange.textContent = `Direction: ${degToDir(data.wind_deg)}`;
        }

        // Pression (quatrième carte)
        const pressCard = document.querySelector('.metric-card:nth-child(4)');
        if (pressCard) {
            const pressValue = pressCard.querySelector('.metric-value');
            if (pressValue) pressValue.textContent = safeDisplay(data.pressure, 0, ' hPa');
        }

        // Conditions météo (cinquième carte)
        const weatherCard = document.querySelector('.metric-card:nth-child(5)');
        if (weatherCard) {
            const weatherValue = weatherCard.querySelector('.metric-value');
            const weatherChange = weatherCard.querySelector('.metric-change');
            if (weatherValue) weatherValue.textContent = data.weather_main || 'N/A';
            if (weatherChange) weatherChange.textContent = data.weather_description || 'N/A';
        }

        // Si données obsolètes, indication visuelle discrète (optionnel)
        const metricCards = document.querySelectorAll('.metric-card');
        metricCards.forEach(card => {
            if (!isFresh) {
                card.style.opacity = '0.8';
            } else {
                card.style.opacity = '1';
            }
        });

    } catch (error) {
        console.error("Erreur lors de la récupération des données actuelles:", error);
        updateStatusIndicator(false);

        // Afficher un message d'erreur discret
        const firstMetric = document.querySelector('.metric-value');
        if (firstMetric) {
            firstMetric.textContent = 'Erreur API';
        }
    }
}

// ============ MISE À JOUR DES PRÉDICTIONS ============
async function fetchAndUpdatePredictions() {
    try {
        const res = await fetch('http://127.0.0.1:5000/api/predictions');

        if (!res.ok) {
            console.warn("Pas de prédictions disponibles");
            return;
        }

        const data = await res.json();

        if (!data || Object.keys(data).length === 0) {
            console.warn("Données de prédiction vides");
            return;
        }

        // Vérifier la fraîcheur des prédictions
        const predictionsFresh = isDataFresh(data.prediction_time, 'predictions');

        // Si prédictions trop anciennes, afficher "--"
        if (!predictionsFresh) {
            console.warn(`Prédictions trop anciennes: ${data.age_minutes}min`);
            displayNoPredictionsMessage();
            return;
        }

        const forecastItems = document.querySelectorAll('.forecast-item');

        for (let i = 0; i < 4 && i < forecastItems.length; i++) {
            const item = forecastItems[i];
            const span = item.querySelector('span');
            const strong = item.querySelector('strong');

            if (span && strong) {
                // ✅ CALCULER À PARTIR DE L'HEURE ACTUELLE
                const now = new Date();
                const nextHour = new Date(now);
                nextHour.setHours(now.getHours() + (i + 1));
                nextHour.setMinutes(0, 0, 0); // Mettre à l'heure pile

                // Afficher l'heure future
                span.textContent = nextHour.getHours().toString().padStart(2, '0') + 'h';

                const predValue = data[`h_plus_${i + 1}`];
                strong.textContent = safeDisplay(predValue, 1, '°C');
                // Indication visuelle si prédictions obsolètes
                if (!predictionsFresh) {
                    item.style.opacity = '0.6';
                    item.style.borderLeftColor = '#ff6b6b';
                } else {
                    item.style.opacity = '1';
                    item.style.borderLeftColor = '#4fc3f7';
                }
            }
        }

        // Mise à jour du graphique principal avec gestion d'erreurs
        await updateMainChart(data);

    } catch (error) {
        console.error("Erreur lors de la récupération des prédictions:", error);
    }
}

// ============ MISE À JOUR DU GRAPHIQUE PRINCIPAL ============
async function updateMainChart(predictionData) {
    try {
        if (!charts.overview) return;

        // Récupérer les températures horaires (h-2, h-1, maintenant)
        const tempRes = await fetch('http://127.0.0.1:5000/api/hourly-temperature');
        if (!tempRes.ok) {
            console.warn("API hourly-temperature indisponible");
            displayEmptyChart();
            return;
        }

        const tempData = await tempRes.json();

        if (!tempData || tempData.length === 0) {
            console.warn("Pas de données horaires pour le graphique");
            displayEmptyChart();
            return;
        }

        // Vérifier fraîcheur des données horaires
        const latestHourly = tempData[tempData.length - 1];
        const hourlyFresh = isDataFresh(latestHourly.timestamp, 'hourly');

        if (!hourlyFresh) {
            console.warn("Données horaires trop anciennes");
            displayEmptyChart();
            return;
        }

        // Préparer les données passées avec les VRAIES heures
        const pastLabels = [];
        const pastTemps = [];

        // Prendre les 3 dernières heures maximum
        const recentData = tempData.slice(-2);

        recentData.forEach(item => {
            try {
                const date = new Date(item.timestamp);
                pastLabels.push(date.getHours() + 'h');
                const temp = item.temperature_avg;
                pastTemps.push((temp !== null && temp !== undefined && !isNaN(temp)) ? temp : null);
            } catch {
                pastLabels.push('N/A');
                pastTemps.push(null);
            }
        });


        // Ajouter "Maintenant" comme point de référence
        const now = new Date();
        pastLabels.push('Maintenant');
        // // Prendre la dernière température comme "maintenant"
        pastTemps.push(pastTemps[pastTemps.length - 1]);



        // Préparer les prédictions avec les VRAIES heures futures
        const predictionTemps = [];
        const predictionLabels = [];

        // Vérifier cohérence des prédictions avec l'heure actuelle
        const predictionTime = predictionData.prediction_time ? new Date(predictionData.prediction_time) : new Date();
        const predictionAge = (new Date() - predictionTime) / (1000 * 60); // en minutes

        if (predictionAge > 240) { // Plus de 4h
            console.warn(`Prédictions trop anciennes: ${predictionAge.toFixed(0)}min`);
            // Afficher des prédictions vides
            for (let i = 1; i <= 4; i++) {
                predictionLabels.push(`H+${i}`);
                predictionTemps.push(null);
            }
        } else {
            // Prédictions valides - calculer les vraies heures
            for (let i = 1; i <= 4; i++) {
                const futureHour = new Date(now.getTime() + i * 60 * 60 * 1000);
                predictionLabels.push(futureHour.getHours() + 'h');

                const predValue = predictionData[`h_plus_${i}`];
                predictionTemps.push((predValue !== null && predValue !== undefined && !isNaN(predValue)) ? predValue : null);
            }
        }

        // Mise à jour du graphique avec vraies données
        charts.overview.data.labels = [...pastLabels, ...predictionLabels];
        charts.overview.data.datasets[0].data = [...pastTemps, ...predictionTemps];
        charts.overview.update('none');

        console.log("Graphique principal mis à jour avec données réelles");

    } catch (error) {
        console.error("Erreur mise à jour graphique principal:", error);
        displayEmptyChart();
    }
}

function displayEmptyChart() {
    if (charts.overview) {
        charts.overview.data.labels = ['Pas de données'];
        charts.overview.data.datasets[0].data = [0];
        charts.overview.update('none');
    }
}

// ============ MISE À JOUR DES ANALYSES (EXTRÊMES) ============
async function fetchAndUpdateAnalytics() {
    try {
        const res = await fetch('http://127.0.0.1:5000/api/daily-extremes');

        if (!res.ok) {
            console.warn("API daily-extremes indisponible");
            displayEmptyExtremesChart();
            return;
        }

        const data = await res.json();

        if (!data || data.length === 0) {
            console.warn("Pas de données d'extrêmes");
            displayEmptyExtremesChart();
            return;
        }

        // Vérifier qu'on a au moins quelques données récentes
        const hasRecentData = data.some(item => {
            const dayAge = (new Date() - new Date(item.timestamp)) / (1000 * 60 * 60 * 24);
            return dayAge <= 7; // Données des 7 derniers jours
        });

        if (!hasRecentData) {
            console.warn("Données d'extrêmes trop anciennes");
            displayEmptyExtremesChart();
            return;
        }

        if (charts.extremes) {
            const labels = data.map(e => {
                try {
                    return new Date(e.timestamp).toLocaleDateString('fr-FR', { weekday: 'short' });
                } catch {
                    return 'N/A';
                }
            });

            const minTemps = data.map(e => {
                const temp = e.temperature_min;
                return (temp !== null && temp !== undefined && !isNaN(temp)) ? temp : null;
            });

            const maxTemps = data.map(e => {
                const temp = e.temperature_max;
                return (temp !== null && temp !== undefined && !isNaN(temp)) ? temp : null;
            });

            charts.extremes.data.labels = labels;
            charts.extremes.data.datasets[0].data = minTemps;
            charts.extremes.data.datasets[1].data = maxTemps;
            charts.extremes.update('none');

            console.log("Graphique extrêmes mis à jour avec données réelles");
        }

    } catch (error) {
        console.error("Erreur lors de la récupération des extrêmes:", error);
        displayEmptyExtremesChart();
    }
}

function displayEmptyExtremesChart() {
    if (charts.extremes) {
        charts.extremes.data.labels = ['Lun', 'Mar', 'Mer', 'Jeu', 'Ven', 'Sam', 'Dim'];
        charts.extremes.data.datasets[0].data = [null, null, null, null, null, null, null];
        charts.extremes.data.datasets[1].data = [null, null, null, null, null, null, null];
        charts.extremes.update('none');
        console.log("Graphique extrêmes: pas de données récentes");
    }
}

// ============ MISE À JOUR ROSE DES VENTS ============
async function fetchAndUpdateWind() {
    try {
        const res = await fetch('http://127.0.0.1:5000/api/wind-distribution');

        if (!res.ok) return;

        const data = await res.json();

        if (!data) return;

        if (charts.wind) {
            const directions = ['N', 'NE', 'E', 'SE', 'S', 'SO', 'O', 'NO'];
            const windData = directions.map(d => {
                const value = data[d];
                return (value !== null && value !== undefined && !isNaN(value)) ? value : 0;
            });

            charts.wind.data.datasets[0].data = windData;
            charts.wind.update('none');
        }

    } catch (error) {
        console.error("Erreur lors de la récupération des données de vent:", error);
    }
}

// ============ SYSTÈME DE MONITORING ALERTES SEULEMENT ============
async function loadMonitoringData() {
    try {
        const alertsRes = await fetch('http://127.0.0.1:5000/api/alerts');
        const alerts = await alertsRes.json();

        console.log("🔍 Alertes reçues:", alerts);
        console.log("🔍 Sévérités:", alerts.map(a => a.severity));

        displaySystemAlerts(alerts);
        updateAlertStats(alerts);
        updateComponentsStatus(alerts);

    } catch (error) {
        console.error("Erreur monitoring:", error);
        showMonitoringError();
    }
}

function displaySystemAlerts(alerts) {
    const container = document.getElementById('system-alerts');
    if (!container) return;

    if (!alerts || alerts.length === 0) {
        container.innerHTML = `
            <div class="alert-item alert-low">
                <span>✅</span>
                <div>
                    <strong>Aucune alerte système</strong><br>
                    <small>Tous les composants fonctionnent normalement</small>
                </div>
            </div>
        `;
        return;
    }

    container.innerHTML = alerts.slice(0, 8).map(alert => `
        <div class="real-alert-item alert-${alert.severity}">
            <span style="font-size: 1.2rem;">${getSeverityIcon(alert.severity)}</span>
            <div style="flex: 1;">
                <strong>${alert.message}</strong><br>
                <small style="color: #64748b;">
                    <strong>${alert.component}</strong> • ${getTimeAgo(alert.age_minutes)}
                    ${alert.type ? ` • ${alert.type}` : ''}
                </small>
            </div>
        </div>
    `).join('');

    updateErrorLogs(alerts);
}

function updateErrorLogs(alerts) {
    console.log("🔍 Debug alertes:", alerts);
    console.log("🔍 Erreurs trouvées:", alerts.filter(a => a.severity === 'critical' || a.severity === 'high'));

    const container = document.getElementById('error-logs');
    if (!container) return;

    const errorAlerts = alerts.filter(a => a.severity === 'critical' || a.severity === 'high');

    if (errorAlerts.length === 0) {
        container.innerHTML = `
            <div style="color: #10b981; text-align: center; padding: 2rem;">
                ✅ Aucune erreur critique détectée
            </div>
        `;
        return;
    }

    const logs = errorAlerts.slice(0, 10).map(alert => {
        const mins = parseFloat(alert.age_minutes) || 0;
        const timestamp = new Date(Date.now() - mins * 60000).toLocaleTimeString();
        const level = alert.severity.toUpperCase();
        return `
            <div class="log-entry log-${alert.severity}">
                [${timestamp}] ${level} - ${alert.component}: ${alert.message}
            </div>
        `;
    }).join('');

    container.innerHTML = logs;
    container.scrollTop = container.scrollHeight;
}

function updateAlertStats(alerts) {
    const stats = {
        critical: alerts.filter(a => a.severity === 'critical').length,
        high: alerts.filter(a => a.severity === 'high').length,
        medium: alerts.filter(a => a.severity === 'medium').length,
        low: alerts.filter(a => a.severity === 'low').length
    };

    Object.entries(stats).forEach(([level, count]) => {
        const el = document.getElementById(`${level}-count`);
        if (el) el.textContent = count;
    });
}

function updateComponentsStatus(alerts) {
    const container = document.getElementById('components-status');
    if (!container) return;

    const components = {
        'Pipeline Spark': getComponentStatus(alerts, ['spark', 'system']),
        'API Météo': getComponentStatus(alerts, ['api']),
        'Capteur Local': getComponentStatus(alerts, ['local']),
        'Base PostgreSQL': getComponentStatus(alerts, ['raw_aggregation', 'hourly_aggregation']),
        'Prédictions ML': getComponentStatus(alerts, ['predictions'])
    };

    container.innerHTML = Object.entries(components).map(([name, status]) => `
        <div class="component-item component-${status.level}">
            <span><strong>${name}</strong></span>
            <span class="component-status status-${status.level}">${status.text}</span>
        </div>
    `).join('');
}

function getComponentStatus(alerts, componentNames) {
    const componentAlerts = alerts.filter(a =>
        componentNames.some(name => a.component.includes(name))
    );

    if (componentAlerts.length === 0) {
        return { level: 'ok', text: 'Opérationnel' };
    }

    const hasCritical = componentAlerts.some(a => a.severity === 'critical');
    const hasHigh = componentAlerts.some(a => a.severity === 'high');

    if (hasCritical) return { level: 'error', text: 'Erreur' };
    if (hasHigh) return { level: 'warning', text: 'Attention' };
    return { level: 'warning', text: 'Avertissement' };
}

function showMonitoringError() {
    document.getElementById('system-alerts').innerHTML = `
        <div class="real-alert-item alert-critical">
            <span></span>
            <div>
                <strong>Erreur de connexion</strong><br>
                <small>Impossible de récupérer les alertes système</small>
            </div>
        </div>
    `;
}

function getSeverityIcon(severity) {
    return {
        'low': '🟢',
        'medium': '🟡',
        'high': '🟠',
        'critical': '🔴'
    }[severity] || '⚪';
}

function getTimeAgo(minutes) {
    const mins = parseFloat(minutes) || 0;
    if (mins < 1) return 'À l\'instant';
    if (mins < 60) return `${Math.round(mins)}min`;
    if (mins < 1440) return `${Math.round(mins / 60)}h`;
    return `${Math.round(mins / 1440)}j`;
}

// Auto-refresh monitoring seulement
setInterval(loadMonitoringData, 15000);

// Charger quand on ouvre l'onglet monitoring
document.addEventListener('DOMContentLoaded', function () {
    const monitoringTab = document.querySelector('.tab-btn[data-tab="monitoring"]');
    if (monitoringTab) {
        monitoringTab.addEventListener('click', () => {
            setTimeout(loadMonitoringData, 300);
        });
    }
});
// ============ INITIALISATION ============
console.log("Initialisation du dashboard temps réel...");
// Appels initiaux
fetchAndUpdateCurrent();
fetchAndUpdatePredictions();
fetchAndUpdateAnalytics();
fetchAndUpdateWind();

// Programmation des mises à jour automatiques
setInterval(fetchAndUpdateCurrent, 2 * 60 * 1000);
setInterval(fetchAndUpdatePredictions, 5 * 60 * 1000);
setInterval(fetchAndUpdateWind, 5 * 60 * 1000);
setInterval(fetchAndUpdateAnalytics, 15 * 60 * 1000);