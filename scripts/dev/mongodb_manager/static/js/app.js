/* ============================================
   MongoDB Manager — Frontend Controller
   ============================================ */
const App = {
    socket: null,
    state: {
        mode: 'export',
        theme: 'dark',
        connected: false,
        databases: [],
        selectedDbs: new Set(),
        selectedCollections: {},
        sourceX509Path: null,
        destX509Path: null,
        outputDir: null,
        running: false,
        result: null
    },
    init() {
        this.socket = io();
        this.loadTheme();
        this.enforceUriDefaults();
        this.bindEvents();
        this.bindSocketEvents();
        this.updateModeUI();
    },
    enforceUriDefaults() {
        const DEFAULT_URI = "mongodb://localhost:27017/";
        const src = document.getElementById('sourceUri');
        const dst = document.getElementById('destUri');
        if (src && (!src.value || !src.value.trim())) src.value = DEFAULT_URI;
        if (dst && (!dst.value || !dst.value.trim())) dst.value = DEFAULT_URI;
    },
    buildConnPayload(type) {
        const isDest = type === 'dest';
        const uriEl = document.getElementById(isDest ? 'destUri' : 'sourceUri');
        let uri = (uriEl && uriEl.value) ? uriEl.value.trim() : '';
        const DEFAULT_URI = "mongodb://localhost:27017/";
        if (!uri) {
            uri = DEFAULT_URI;
            if (uriEl) uriEl.value = uri;
        }
        const timeoutMsEl = document.getElementById('timeoutMs');
        let timeoutMs = 3600000;
        if (timeoutMsEl && timeoutMsEl.value) {
            timeoutMs = parseInt(timeoutMsEl.value, 10);
            if (!timeoutMs || timeoutMs < 1000) timeoutMs = 3600000;
        }
        const x509Path = isDest ? this.state.destX509Path : this.state.sourceX509Path;
        return { uri, timeout_ms: timeoutMs, x509_path: x509Path };
    },
    validateUri(uri) {
        if (!uri || !uri.trim()) return { ok: false, reason: "La URI MongoDB está vacía" };
        const v = uri.trim();
        if (!v.startsWith("mongodb://") && !v.startsWith("mongodb+srv://")) {
            return { ok: false, reason: "La URI debe empezar con mongodb:// o mongodb+srv://" };
        }
        return { ok: true };
    },
    loadTheme() {
        const saved = localStorage.getItem('ui-theme');
        if (saved === 'light' || (!saved && window.matchMedia('(prefers-color-scheme: light)').matches)) {
            this.applyTheme('light');
        } else {
            this.applyTheme('dark');
        }
    },
    applyTheme(theme) {
        this.state.theme = theme;
        const root = document.documentElement;
        if (theme === 'light') {
            root.setAttribute('data-theme', 'light');
        } else {
            root.removeAttribute('data-theme');
        }
        const btn = document.getElementById('themeToggle');
        if (btn) btn.textContent = theme === 'light' ? '☀️' : '🌙';
        localStorage.setItem('ui-theme', theme);
    },
    toggleTheme() {
        this.applyTheme(this.state.theme === 'light' ? 'dark' : 'light');
    },
    bindEvents() {
        // Theme toggle
        const themeBtn = document.getElementById('themeToggle');
        if (themeBtn) themeBtn.addEventListener('click', () => this.toggleTheme());
        // Navigation tabs
        document.querySelectorAll('.nav-tab').forEach(tab => {
            tab.addEventListener('click', () => this.switchTab(tab.dataset.tab));
        });
        // Mode switcher
        document.querySelectorAll('input[name="mode"]').forEach(radio => {
            radio.addEventListener('change', (e) => {
                this.state.mode = e.target.value;
                this.updateModeUI();
            });
        });
        // X.509 toggles
        document.getElementById('sourceX509').addEventListener('change', (e) => {
            document.getElementById('sourceX509Group').style.display = e.target.checked ? 'block' : 'none';
        });
        document.getElementById('destX509').addEventListener('change', (e) => {
            document.getElementById('destX509Group').style.display = e.target.checked ? 'block' : 'none';
        });
        // Test connections
        document.getElementById('btnTestSource').addEventListener('click', () => this.testConnection('source'));
        document.getElementById('btnTestDest').addEventListener('click', () => this.testConnection('dest'));
        // Run operation
        document.getElementById('btnRun').addEventListener('click', () => this.runOperation());
        // Clear logs
        document.getElementById('btnClearLogs').addEventListener('click', () => {
            document.getElementById('logsBody').innerHTML = '';
        });
        // Download ZIP
        document.getElementById('btnDownloadZip').addEventListener('click', () => this.downloadZip());
        // X.509 uploads
        document.getElementById('sourceX509File').addEventListener('change', (e) => this.uploadX509(e, 'source'));
        document.getElementById('destX509File').addEventListener('change', (e) => this.uploadX509(e, 'dest'));
    },
    bindSocketEvents() {
        this.socket.on('connect', () => {
            console.log('Socket connected');
        });
        this.socket.on('log', (data) => {
            this.appendLog(data.message);
        });
        this.socket.on('progress', (data) => {
            this.updateProgress(data.current, data.total, data.message);
        });
        this.socket.on('complete', (data) => {
            try {
                console.log('[socketio complete]', data);
                this.state.running = false;
                this.state.result = data;
                this.state.outputDir = data.output_dir;
                this.updateProgress(1, 1, 'Completado');
                document.getElementById('progressBar').style.width = '100%';
                document.getElementById('progressPercent').textContent = '100%';
                this.showResults(data);
                this.switchTab('results');
            } catch (err) {
                console.error('[complete handler error]', err);
                try {
                    this.updateProgress(1, 1, 'Completado con error en render');
                    const statusEl = document.getElementById('resultStatus');
                    if (statusEl) {
                        statusEl.className = 'result-status error';
                        statusEl.innerHTML = '❌ Error al renderizar resultados. Revisa la consola del navegador: ' + err.message;
                    }
                    this.switchTab('results');
                } catch (_) {}
            }
        });
    },
    switchTab(tabId) {
        document.querySelectorAll('.nav-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tabId));
        document.querySelectorAll('.tab-panel').forEach(p => p.classList.toggle('active', p.id === `tab-${tabId}`));
        const titles = {
            connect: 'Conectar a MongoDB',
            select: 'Seleccionar bases de datos y colecciones',
            run: 'Ejecutar operación',
            results: 'Resultados'
        };
        document.getElementById('pageTitle').textContent = titles[tabId];
    },
    updateModeUI() {
        const isCopy = this.state.mode === 'copy';
        document.getElementById('destConnectionCard').style.display = isCopy ? 'block' : 'none';
        document.getElementById('btnTestDest').style.display = isCopy ? 'inline-flex' : 'none';
        document.getElementById('outputDir').value = isCopy ? './mongo_copy' : './mongo_export';
    },
    async uploadX509(event, type) {
        const file = event.target.files[0];
        if (!file) return;
        const formData = new FormData();
        formData.append('file', file);
        try {
            const res = await fetch('/api/upload-x509', { method: 'POST', body: formData });
            const data = await res.json();
            if (data.success) {
                if (type === 'source') this.state.sourceX509Path = data.path;
                else this.state.destX509Path = data.path;
                this.toast('Certificado X.509 subido correctamente', 'success');
            } else {
                this.toast(data.error || 'Error al subir certificado', 'error');
            }
        } catch (err) {
            this.toast('Error de red al subir certificado', 'error');
        }
    },
    async testConnection(type) {
        const isDest = type === 'dest';
        const conn = this.buildConnPayload(type);
        const validation = this.validateUri(conn.uri);
        if (!validation.ok) {
            this.toast('⚠️ ' + validation.reason, 'error');
            return;
        }
        const btn = document.getElementById(isDest ? 'btnTestDest' : 'btnTestSource');
        const originalText = btn.innerHTML;
        btn.innerHTML = '<span class="btn-icon">⏳</span>Conectando...';
        btn.disabled = true;
        try {
            const res = await fetch('/api/test-connection', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(conn)
            });
            const data = await res.json();
            if (data.success) {
                this.toast(`✅ Conectado. ${data.databases.length} base(s) encontrada(s).`, 'success');
                if (!isDest) {
                    this.state.connected = true;
                    this.state.databases = data.databases;
                    this.renderDbSelector();
                    this.updateConnectionStatus(true);
                }
            } else {
                const reason = (data && data.error) ? data.error : 'Revisa la URI y credenciales.';
                this.toast('❌ No se pudo conectar. ' + reason, 'error');
            }
        } catch (err) {
            this.toast('Error de red: ' + err.message, 'error');
        } finally {
            btn.innerHTML = originalText;
            btn.disabled = false;
        }
    },
    updateConnectionStatus(connected) {
        const dot = document.querySelector('#connectionStatus .status-dot');
        const text = document.querySelector('#connectionStatus .status-text');
        dot.classList.toggle('online', connected);
        dot.classList.toggle('offline', !connected);
        text.textContent = connected ? 'Conectado' : 'Sin conexión';
    },
    renderDbSelector() {
        const container = document.getElementById('dbSelector');
        container.innerHTML = '';
        this.state.selectedDbs.clear();
        this.state.selectedCollections = {};
        this.state.databases.forEach(db => {
            const chip = document.createElement('div');
            chip.className = 'db-chip';
            chip.innerHTML = `
                <span class="db-check"></span>
                <span>${db}</span>
            `;
            chip.addEventListener('click', () => this.toggleDb(db, chip));
            container.appendChild(chip);
        });
        document.getElementById('dbCountBadge').textContent = this.state.databases.length;
        document.getElementById('selectEmptyState').style.display = 'none';
        document.getElementById('selectContent').style.display = 'block';
        this.updateSummary();
    },
    async toggleDb(dbName, chipEl) {
        const isSelected = chipEl.classList.toggle('selected');
        if (isSelected) {
            this.state.selectedDbs.add(dbName);
            await this.loadCollections(dbName);
        } else {
            this.state.selectedDbs.delete(dbName);
            delete this.state.selectedCollections[dbName];
            const group = document.getElementById(`colGroup-${dbName}`);
            if (group) group.remove();
        }
        this.updateSummary();
        this.updateRunTab();
    },
    async loadCollections(dbName) {
        const conn = this.buildConnPayload('source');
        const validation = this.validateUri(conn.uri);
        if (!validation.ok) {
            this.toast('⚠️ No se pueden cargar colecciones: ' + validation.reason, 'error');
            this.state.selectedCollections[dbName] = new Set();
            this.renderCollectionsGroup(dbName, []);
            return;
        }
        const includeSystem = document.getElementById('includeSystem').checked;
        try {
            const res = await fetch('/api/collections', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ uri: conn.uri, db: dbName, timeout_ms: conn.timeout_ms, x509_path: conn.x509_path, include_system: includeSystem })
            });
            const data = await res.json();
            if (data.success) {
                this.state.selectedCollections[dbName] = new Set(data.collections);
                this.renderCollectionsGroup(dbName, data.collections);
            } else {
                const err = (data && data.error) ? data.error : '';
                this.toast('⚠️ No se pudieron listar colecciones' + (err ? ': ' + err : ''), 'error');
                this.state.selectedCollections[dbName] = new Set();
                this.renderCollectionsGroup(dbName, []);
            }
        } catch (err) {
            console.error('Error loading collections:', err);
            this.toast('Error de red al cargar colecciones: ' + err.message, 'error');
            this.state.selectedCollections[dbName] = new Set();
            this.renderCollectionsGroup(dbName, []);
        }
    },
    renderCollectionsGroup(dbName, collections) {
        const container = document.getElementById('collectionsContainer');
        const group = document.createElement('div');
        group.className = 'collections-group';
        group.id = `colGroup-${dbName}`;
        group.innerHTML = `
            <div class="collections-group-header" onclick="App.toggleGroup(this)">
                <span class="toggle-icon">▼</span>
                <h4>🗄️ ${dbName}</h4>
                <span class="badge">${collections.length}</span>
            </div>
            <div class="collections-list">
                ${collections.map(col => `
                    <div class="col-chip selected" onclick="App.toggleCollection('${dbName}', '${col}', this)">
                        <input type="checkbox" checked onchange="event.stopPropagation()">
                        <span>${col}</span>
                    </div>
                `).join('')}
            </div>
        `;
        container.appendChild(group);
    },
    toggleGroup(header) {
        header.parentElement.classList.toggle('collapsed');
    },
    toggleCollection(dbName, colName, chipEl) {
        const checkbox = chipEl.querySelector('input');
        checkbox.checked = !checkbox.checked;
        chipEl.classList.toggle('selected', checkbox.checked);
        if (checkbox.checked) {
            this.state.selectedCollections[dbName].add(colName);
        } else {
            this.state.selectedCollections[dbName].delete(colName);
        }
        this.updateSummary();
        this.updateRunTab();
    },
    updateSummary() {
        document.getElementById('summaryDbs').textContent = this.state.selectedDbs.size;
        let totalCols = 0;
        Object.values(this.state.selectedCollections).forEach(set => totalCols += set.size);
        document.getElementById('summaryCols').textContent = totalCols;
    },
    updateRunTab() {
        const hasSelection = this.state.selectedDbs.size > 0;
        document.getElementById('runEmptyState').style.display = hasSelection ? 'none' : 'flex';
        document.getElementById('runContent').style.display = hasSelection ? 'block' : 'none';
        if (hasSelection) {
            const summaryEl = document.getElementById('runSummaryContent');
            summaryEl.innerHTML = '';
            Array.from(this.state.selectedDbs).forEach(db => {
                const cols = this.state.selectedCollections[db];
                const colsArray = cols ? Array.from(cols) : [];
                const div = document.createElement('div');
                div.style.marginBottom = '12px';
                div.innerHTML = `
                    <strong style="color: var(--accent-primary)">${db}</strong>
                    <div style="margin-top:4px; font-size:13px; color: var(--text-secondary)">
                        ${colsArray.length > 0 ? colsArray.join(', ') : '<em>(ninguna seleccionada)</em>'}
                    </div>
                `;
                summaryEl.appendChild(div);
            });
        }
    },
    runOperation() {
        if (this.state.running) return;
        const selectedDbs = Array.from(this.state.selectedDbs);
        const selectedCollections = {};
        selectedDbs.forEach(db => {
            const cols = this.state.selectedCollections[db];
            selectedCollections[db] = cols ? Array.from(cols) : [];
        });
        const srcConn = this.buildConnPayload('source');
        const validateSrc = this.validateUri(srcConn.uri);
        if (!validateSrc.ok) {
            this.toast('⚠️ URI origen inválida: ' + validateSrc.reason, 'error');
            return;
        }
        let destConn = null;
        if (this.state.mode === 'copy') {
            destConn = this.buildConnPayload('dest');
            const validateDst = this.validateUri(destConn.uri);
            if (!validateDst.ok) {
                this.toast('⚠️ URI destino inválida: ' + validateDst.reason, 'error');
                return;
            }
        }
        if (selectedDbs.length === 0) {
            this.toast('⚠️ Selecciona al menos una base de datos', 'error');
            return;
        }
        const payload = {
            uri: srcConn.uri,
            timeout_ms: srcConn.timeout_ms,
            x509_path: srcConn.x509_path,
            include_system: document.getElementById('includeSystem').checked,
            selected_dbs: selectedDbs,
            selected_collections: selectedCollections,
            output_dir: document.getElementById('outputDir').value
        };
        this.state.running = true;
        this.state.result = null;
        document.getElementById('progressContainer').style.display = 'block';
        document.getElementById('logsContainer').style.display = 'block';
        document.getElementById('logsBody').innerHTML = '';
        document.getElementById('progressBar').style.width = '0%';
        document.getElementById('progressPercent').textContent = '0%';
        document.getElementById('progressMessage').textContent = 'Iniciando...';
        document.getElementById('btnRun').disabled = true;
        if (this.state.mode === 'copy') {
            payload.dest_uri = destConn.uri;
            payload.dest_x509 = destConn.x509_path;
            this.socket.emit('run_copy', payload);
        } else {
            this.socket.emit('run_export', payload);
        }
    },
    updateProgress(current, total, message) {
        const pct = total > 0 ? Math.round((current / total) * 100) : 0;
        document.getElementById('progressBar').style.width = pct + '%';
        document.getElementById('progressPercent').textContent = pct + '%';
        document.getElementById('progressMessage').textContent = message || `Procesando... ${current}/${total}`;
    },
    appendLog(message) {
        const logsBody = document.getElementById('logsBody');
        const entry = document.createElement('div');
        entry.className = 'log-entry';
        if (message.includes('✗') || message.includes('Error') || message.includes('error')) {
            entry.classList.add('error');
        } else if (message.includes('⚠') || message.includes('Warning')) {
            entry.classList.add('warning');
        } else if (message.includes('✓') || message.includes('exitosamente')) {
            entry.classList.add('success');
        }
        entry.textContent = message;
        logsBody.appendChild(entry);
        logsBody.scrollTop = logsBody.scrollHeight;
    },
    showResults(data) {
        console.log('[showResults] payload entrante:', data);
        try {
            document.getElementById('resultsEmptyState').style.display = 'none';
            document.getElementById('resultsContent').style.display = 'block';
        } catch (e) {
            console.warn('No se pudo mostrar resultsContent:', e);
        }
        document.getElementById('btnRun').disabled = false;
        const stats = (data && data.stats) ? data.stats : {};
        const errors = Array.isArray(stats.errors) ? stats.errors.slice() : [];
        const warnings = [];
        if (Array.isArray(data.warnings)) warnings.push(...data.warnings);
        if (Array.isArray(stats.warnings)) warnings.push(...stats.warnings);
        if (data && data.error && typeof data.error === 'string' && data.error.trim()) {
            errors.push(`Server error: ${data.error.trim()}`);
        }
        if (Array.isArray(data.errors) && data.errors.length > 0) {
            data.errors.forEach(group => {
                if (!group) return;
                if (typeof group === 'string') { errors.push(group); return; }
                const dbName = group.db || '';
                (group.errors || []).forEach(e => {
                    if (!e) return;
                    if (typeof e === 'string') {
                        errors.push(dbName ? `${dbName}: ${e}` : e);
                    } else {
                        const colName = e.collection || '?';
                        const msg = e.error || JSON.stringify(e);
                        errors.push(dbName ? `${dbName}.${colName}: ${msg}` : `${colName}: ${msg}`);
                    }
                });
            });
        }
        const selected = Array.isArray(data.selected) ? data.selected
            : (stats && Array.isArray(stats.selected) ? stats.selected : []);
        const imported = Array.isArray(data.imported) ? data.imported : [];
        let dbs = Number(stats.databases || 0);
        let cols = Number(stats.collections || 0);
        let docs = Number(stats.documents || 0);
        const computeFromEntries = (entries) => {
            let d = 0, c = 0, n = 0;
            const seenDbs = new Set();
            entries.forEach(entry => {
                if (!entry || typeof entry !== 'object') return;
                const dbName = entry.db || '';
                if (dbName && !seenDbs.has(dbName)) { d += 1; seenDbs.add(dbName); }
                const collections = Array.isArray(entry.collections) ? entry.collections : [];
                c += collections.filter(x => x && String(x).trim()).length;
                const imports = Array.isArray(entry.imported) ? entry.imported : [];
                imports.forEach(imp => {
                    if (!imp || typeof imp !== 'object') return;
                    const inserted = Number(imp.inserted || 0);
                    n += inserted;
                    if (!seenDbs.has(dbName) && dbName && imp.collection) {
                        seenDbs.add(dbName);
                    }
                });
            });
            return { d, c, n };
        };
        if (selected.length > 0) {
            const derived = computeFromEntries(selected);
            dbs = Math.max(dbs, derived.d);
            cols = Math.max(cols, derived.c);
            docs = Math.max(docs, derived.n);
        }
        if (imported.length > 0 && (dbs === 0 || docs === 0)) {
            const derived = computeFromEntries(imported);
            dbs = Math.max(dbs, derived.d);
            cols = Math.max(cols, derived.c);
            docs = Math.max(docs, derived.n);
        }
        const dedup = arr => Array.from(new Set(arr.filter(x => x && String(x).trim())));
        const uniqueErrors = dedup(errors);
        const uniqueWarnings = dedup(warnings);
        const hasErrors = uniqueErrors.length > 0;
        const hasWarnings = uniqueWarnings.length > 0;
        const producedSomething = dbs > 0 || cols > 0 || docs > 0;
        let successFlag = data.success !== undefined ? !!data.success : !hasErrors;
        let bannerMode = 'success';
        let bannerText = '✅ Operación completada exitosamente';
        if (hasErrors) {
            bannerMode = 'error';
            bannerText = '❌ La operación terminó con errores';
            successFlag = false;
        } else if (hasWarnings && producedSomething) {
            bannerMode = 'warning';
            bannerText = '⚠️ Operación completada con advertencias';
            successFlag = true;
        } else if (!successFlag && producedSomething) {
            bannerMode = 'warning';
            bannerText = '⚠️ Operación completada con advertencias';
            successFlag = true;
        } else if (!successFlag) {
            bannerMode = 'error';
            bannerText = '❌ La operación terminó con errores';
        }
        console.log('[showResults] derivados:', { dbs, cols, docs, successFlag, bannerMode, hasErrors, hasWarnings, selectedLen: selected.length, importedLen: imported.length });
        const statusEl = document.getElementById('resultStatus');
        if (statusEl) {
            statusEl.className = 'result-status ' + bannerMode;
            statusEl.innerHTML = bannerText;
        }
        const dbEl = document.getElementById('metricDbs');
        if (dbEl) dbEl.textContent = dbs;
        const colEl = document.getElementById('metricCols');
        if (colEl) colEl.textContent = cols;
        const docEl = document.getElementById('metricDocs');
        if (docEl) docEl.textContent = docs.toLocaleString();
        const durationText = (data && data.duration) ? data.duration : (stats && stats.duration ? stats.duration : '-');
        const durEl = document.getElementById('metricDuration');
        if (durEl) durEl.textContent = durationText;
        const errorsCard = document.getElementById('errorsCard');
        const warningsCard = document.getElementById('warningsCard');
        const warningsListEl = document.getElementById('warningsList');
        const errorsListEl = document.getElementById('errorsList');
        const fmtList = (arr, cls) => arr.map(e =>
            `<div class="log-entry ${cls}" style="padding:8px 0">${String(e).replace(/[<>]/g, c => c === '<' ? '&lt;' : '&gt;')}</div>`
        ).join('');
        if (uniqueErrors.length > 0) {
            if (errorsCard) errorsCard.style.display = 'block';
            if (errorsListEl) errorsListEl.innerHTML = fmtList(uniqueErrors, 'error');
        } else {
            if (errorsCard) errorsCard.style.display = 'none';
            if (errorsListEl) errorsListEl.innerHTML = '';
        }
        if (uniqueWarnings.length > 0) {
            if (warningsCard) warningsCard.style.display = 'block';
            if (warningsListEl) warningsListEl.innerHTML = fmtList(uniqueWarnings, 'warning');
        } else {
            if (warningsCard) warningsCard.style.display = 'none';
            if (warningsListEl) warningsListEl.innerHTML = '';
        }
        try { this.loadFilesList(data.output_dir); } catch (e) { console.warn(e); }
        if (data.output_dir) {
            const el = document.getElementById('downloadActions');
            if (el) el.style.display = 'flex';
        }
    },
    async loadFilesList(outputDir) {
        if (!outputDir) return;
        // We can't list server files from client easily without an endpoint,
        // so we'll show the output dir and let them download the zip
        const filesCard = document.getElementById('filesCard');
        filesCard.style.display = 'block';
        document.getElementById('filesList').innerHTML = `
            <div class="file-item">
                <span class="file-item-name">📁 ${outputDir}</span>
                <span class="file-item-size">Directorio de salida</span>
            </div>
        `;
    },
    downloadZip() {
        if (!this.state.outputDir) return;
        window.location.href = `/api/download-zip?out_dir=${encodeURIComponent(this.state.outputDir)}`;
    },
    toast(message, type = 'info') {
        const container = document.getElementById('toastContainer');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `
            <span>${type === 'success' ? '✓' : type === 'error' ? '✗' : 'ℹ'}</span>
            <span>${message}</span>
        `;
        container.appendChild(toast);
        setTimeout(() => toast.remove(), 4700);
    }
};
// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => App.init());
