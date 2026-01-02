
import supabase from './supabase.js';

document.addEventListener('DOMContentLoaded', () => {
    // --- Auth & Navigation Elements ---
    const loginView = document.getElementById('login-view');
    const appLayout = document.getElementById('app-layout');
    const googleLoginBtn = document.getElementById('google-login-btn');
    const loginError = document.getElementById('login-error');
    const logoutBtn = document.getElementById('logout-btn');
    const logoutBtnPendente = document.getElementById('logout-btn-pendente');

    // Sidebar
    const sideMenu = document.getElementById('side-menu');
    const openSidebarMobileBtn = document.getElementById('open-sidebar-mobile');
    const closeSidebarMobileBtn = document.getElementById('close-sidebar-mobile');
    const navDashboardBtn = document.getElementById('nav-dashboard');
    const navUploaderBtn = document.getElementById('nav-uploader');

    // Views
    const dashboardContainer = document.getElementById('dashboard-container');
    const uploaderModal = document.getElementById('uploader-modal');
    const closeUploaderBtn = document.getElementById('close-uploader-btn');

    // Dashboard Internal Views
    const mainDashboardContent = document.getElementById('main-dashboard-content');
    const cityView = document.getElementById('city-view');

    // Buttons in Dashboard
    const clearFiltersBtn = document.getElementById('clear-filters-btn');
    const showCityBtn = document.getElementById('show-city-btn');
    const backToMainBtn = document.getElementById('back-to-main-btn');

    // Uploader Elements
    const salesPrevYearInput = document.getElementById('sales-prev-year-input');
    const salesCurrYearInput = document.getElementById('sales-curr-year-input');
    const salesCurrMonthInput = document.getElementById('sales-curr-month-input');
    const clientsFileInput = document.getElementById('clients-file-input');
    const productsFileInput = document.getElementById('products-file-input');
    const generateBtn = document.getElementById('generate-btn');
    const statusContainer = document.getElementById('status-container');
    const statusText = document.getElementById('status-text');
    const progressBar = document.getElementById('progress-bar');

    // --- Auth Logic ---
    const telaLoading = document.getElementById('tela-loading');
    const telaPendente = document.getElementById('tela-pendente');

    // UI Functions
    const showScreen = (screenId) => {
        // Hide all auth/app screens first
        [loginView, telaLoading, telaPendente, appLayout].forEach(el => el?.classList.add('hidden'));
        if (screenId) {
            const screen = document.getElementById(screenId);
            screen?.classList.remove('hidden');
        }
    };

    // --- Cache (IndexedDB) Logic ---
    const DB_NAME = 'PrimeDashboardDB';
    const STORE_NAME = 'data_store';
    const DB_VERSION = 1;

    const initDB = () => {
        return idb.openDB(DB_NAME, DB_VERSION, {
            upgrade(db) {
                if (!db.objectStoreNames.contains(STORE_NAME)) {
                    db.createObjectStore(STORE_NAME);
                }
            },
        });
    };

    const getFromCache = async (key) => {
        try {
            const db = await initDB();
            return await db.get(STORE_NAME, key);
        } catch (e) {
            console.warn('Erro ao ler cache:', e);
            return null;
        }
    };

    const saveToCache = async (key, value) => {
        try {
            const db = await initDB();
            await db.put(STORE_NAME, value, key);
        } catch (e) {
            console.warn('Erro ao salvar cache:', e);
        }
    };

    let checkProfileLock = false;
    let isAppReady = false;

    // --- Visibility & Reconnection Logic ---
    document.addEventListener('visibilitychange', async () => {
        if (document.visibilityState === 'visible') {
            console.log('Tab visible. Checking connection status...');
            const { data } = await supabase.auth.getSession();
            if (data && data.session) {
                if (!isAppReady) {
                     console.log('Session active but app not ready. Retrying profile check...');
                     checkProfileStatus(data.session.user);
                }
            } else {
                // If no session and we thought we were logged in, reload might be needed or just let onAuthStateChange handle it
                if (isAppReady) {
                     console.warn('Session lost while backgrounded. Reloading...');
                     window.location.reload();
                }
            }
        }
    });

    async function checkSession() {
        console.log('Iniciando verificação de sessão...');
        showScreen('tela-loading');

        supabase.auth.onAuthStateChange(async (event, session) => {
            console.log('Auth Event:', event);
            if (event === 'SIGNED_OUT') {
                isAppReady = false;
                showScreen('login-view');
                return;
            }

            if (session) {
                console.log('Sessão encontrada para usuário:', session.user.email);
                
                if (isAppReady) {
                    console.log('App já inicializado. Ignorando re-verificação.');
                    return;
                }

                // Debounce/Lock to prevent overlapping checks causing disconnects
                if (!checkProfileLock) {
                    await checkProfileStatus(session.user);
                } else {
                    console.log('Verificação de perfil já em andamento, ignorando evento duplicado.');
                }
            } else {
                console.log('Nenhuma sessão ativa.');
                showScreen('login-view');
            }
        });
    }

    async function checkProfileStatus(user) {
        if (isAppReady) return;

        checkProfileLock = true;
        console.log('Verificando perfil para ID:', user.id);
        
        try {
            // Check Profile with Timeout - 10s (Modified per user request)
            const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('Tempo limite de conexão excedido. Verifique sua internet.')), 10000));
            const profileQuery = supabase.from('profiles').select('status, role').eq('id', user.id).single();

            const { data: profile, error } = await Promise.race([profileQuery, timeout]);

            if (error) {
                if (error.code !== 'PGRST116') {
                    throw error;
                }
            }

            // Status & Role handling
            const status = profile?.status || 'pendente';
            if (profile?.role) {
                window.userRole = profile.role;
            }
            console.log('Status do perfil:', status);

            if (status === 'aprovado') {
                const currentScreen = document.getElementById('app-layout');
                if (currentScreen.classList.contains('hidden')) {
                    console.log('Acesso aprovado. Carregando dashboard...');
                    isAppReady = true;
                    showScreen('app-layout');
                    initDashboard();
                } else {
                    console.log('Acesso já aprovado e dashboard visível.');
                    isAppReady = true;
                }
            } else {
                console.log('Acesso pendente ou bloqueado. Redirecionando para tela de espera.');
                showScreen('tela-pendente');
                
                if (status === 'bloqueado') {
                        const statusMsg = document.getElementById('status-text-pendente'); 
                        if(statusMsg) statusMsg.textContent = "Acesso Bloqueado";
                }

                startStatusListener(user.id);
            }
        } catch (err) {
            console.error('Error checking profile:', err);
            checkProfileLock = false;
            
            // Only show error if app is not ready (initial load)
            if (!isAppReady) {
                // Suppress timeout error as per user request
                if (err.message !== 'Tempo limite de conexão excedido. Verifique sua internet.') {
                    alert("Erro de conexão: " + (err.message || 'Erro desconhecido'));
                    showScreen('login-view');
                } else {
                    console.warn('Timeout de conexão suprimido. Aguardando recuperação...');
                }
            }
        } finally {
            checkProfileLock = false;
        }
    }

    let statusListener = null;
    function startStatusListener(userId) {
        if (statusListener) return; // Already listening

        statusListener = supabase
            .channel(`public:profiles:id=eq.${userId}`)
            .on('postgres_changes', {
                event: 'UPDATE',
                schema: 'public',
                table: 'profiles',
                filter: `id=eq.${userId}`
            }, (payload) => {
                if (payload.new && payload.new.status === 'aprovado') {
                    supabase.removeChannel(statusListener);
                    statusListener = null;
                    showScreen('app-layout');
                    initDashboard();
                }
            })
            .subscribe();
    }

    googleLoginBtn.addEventListener('click', async () => {
        loginError.classList.add('hidden');

        const { data, error } = await supabase.auth.signInWithOAuth({
            provider: 'google',
            options: {
                redirectTo: window.location.origin + window.location.pathname
            }
        });

        if (error) {
            loginError.textContent = 'Erro ao iniciar login: ' + error.message;
            loginError.classList.remove('hidden');
        }
    });

    const handleLogout = async () => {
        if(statusListener) {
            supabase.removeChannel(statusListener);
            statusListener = null;
        }
        await supabase.auth.signOut();
        // onAuthStateChange handles the UI update
    };

    logoutBtn.addEventListener('click', handleLogout);
    if(logoutBtnPendente) logoutBtnPendente.addEventListener('click', handleLogout);

    // Check session on start
    checkSession();

    // --- Navigation Logic ---

    function toggleSidebar() {
        sideMenu.classList.toggle('-translate-x-full');
    }

    openSidebarMobileBtn.addEventListener('click', toggleSidebar);
    closeSidebarMobileBtn.addEventListener('click', toggleSidebar);

    navDashboardBtn.addEventListener('click', () => {
        dashboardContainer.classList.remove('hidden');
        uploaderModal.classList.add('hidden');
        // Reset to main dashboard view
        mainDashboardContent.classList.remove('hidden');
        cityView.classList.add('hidden');
        if (window.innerWidth < 768) toggleSidebar();
    });

    navUploaderBtn.addEventListener('click', () => {
        if (window.userRole !== 'adm') {
            alert('Acesso negado: Apenas administradores podem acessar o uploader.');
            return;
        }
        uploaderModal.classList.remove('hidden');
        if (window.innerWidth < 768) toggleSidebar();
    });

    closeUploaderBtn.addEventListener('click', () => {
        uploaderModal.classList.add('hidden');
    });

    // --- Dashboard Internal Navigation ---
    clearFiltersBtn.addEventListener('click', async () => {
        // Immediately reset UI for better responsiveness
        const resetSelect = (el) => {
            el.innerHTML = '<option value="">Todos</option>';
            el.value = '';
        };
        resetSelect(supervisorFilter);
        resetSelect(vendedorFilter);
        resetSelect(fornecedorFilter);
        resetSelect(cidadeFilter);
        resetSelect(filialFilter);
        
        anoFilter.innerHTML = '<option value="todos">Todos</option>';
        anoFilter.value = 'todos';
        mesFilter.value = '';

        // Reload filters to reset dropdown options to full lists
        await loadFilters(getCurrentFilters());
        loadMainDashboardData();
    });

    showCityBtn.addEventListener('click', () => {
        mainDashboardContent.classList.add('hidden');
        cityView.classList.remove('hidden');
        loadCityView();
    });

    backToMainBtn.addEventListener('click', () => {
        cityView.classList.add('hidden');
        mainDashboardContent.classList.remove('hidden');
    });


    // --- Uploader Logic ---
    let files = {};

    const checkFiles = () => {
        // No longer need credentials check, they are handled by auth session
        const hasFiles = files.salesPrevYearFile && files.salesCurrYearFile && files.salesCurrMonthFile && files.clientsFile && files.productsFile;
        generateBtn.disabled = !hasFiles;
    };

    if(salesPrevYearInput) salesPrevYearInput.addEventListener('change', (e) => { files.salesPrevYearFile = e.target.files[0]; checkFiles(); });
    if(salesCurrYearInput) salesCurrYearInput.addEventListener('change', (e) => { files.salesCurrYearFile = e.target.files[0]; checkFiles(); });
    if(salesCurrMonthInput) salesCurrMonthInput.addEventListener('change', (e) => { files.salesCurrMonthFile = e.target.files[0]; checkFiles(); });
    if(clientsFileInput) clientsFileInput.addEventListener('change', (e) => { files.clientsFile = e.target.files[0]; checkFiles(); });
    if(productsFileInput) productsFileInput.addEventListener('change', (e) => { files.productsFile = e.target.files[0]; checkFiles(); });

    if(generateBtn) generateBtn.addEventListener('click', () => {
        if (!files.salesPrevYearFile || !files.salesCurrYearFile || !files.salesCurrMonthFile || !files.clientsFile || !files.productsFile) return;

        // Credentials are no longer passed to worker. 
        // Authentication is handled via session token in enviarDadosParaSupabase.

        generateBtn.disabled = true;
        statusContainer.classList.remove('hidden');
        statusText.textContent = 'Iniciando processamento...';
        progressBar.style.width = '0%';

        const worker = new Worker('src/js/worker.js');

        worker.postMessage({
            salesPrevYearFile: files.salesPrevYearFile,
            salesCurrYearFile: files.salesCurrYearFile,
            salesCurrMonthFile: files.salesCurrMonthFile,
            clientsFile: files.clientsFile,
            productsFile: files.productsFile
        });

        worker.onmessage = async (event) => {
            const { type, data, status, percentage, message } = event.data;
            if (type === 'progress') {
                statusText.textContent = status;
                progressBar.style.width = `${percentage}%`;
            } else if (type === 'result') {
                statusText.textContent = 'Processamento concluído. Iniciando upload...';
                try {
                    await enviarDadosParaSupabase(data);
                    
                    statusText.textContent = 'Dados atualizados com sucesso!';
                    progressBar.style.width = '100%';
                    setTimeout(() => {
                        uploaderModal.classList.add('hidden');
                        statusContainer.classList.add('hidden');
                        generateBtn.disabled = false;
                        initDashboard(); // Reload data
                    }, 1500);
                } catch (e) {
                    console.error(e);
                    statusText.innerHTML = `<span class="text-red-500">Erro no upload: ${e.message}</span>`;
                    generateBtn.disabled = false;
                }
            } else if (type === 'error') {
                statusText.innerHTML = `<span class="text-red-500">Erro: ${message}</span>`;
                generateBtn.disabled = false;
            }
        };
    });

    async function enviarDadosParaSupabase(data) {
        const updateStatus = (msg, percent) => {
            statusText.textContent = msg;
            progressBar.style.width = `${percent}%`;
        };

        const performUpsert = async (table, batch) => {
            const { error } = await supabase.from(table).insert(batch); // Use insert as per original logic, or upsert if needed
            if (error) {
                throw new Error(`Erro Supabase em ${table}: ${error.message}`);
            }
        };

        const clearTable = async (table) => {
            // Use RPC for safe truncation (checks is_admin)
            const { error } = await supabase.rpc('truncate_table', { table_name: table });
            if (error) {
                throw new Error(`Erro ao limpar tabela ${table}: ${error.message}`);
            }
        };

        const BATCH_SIZE = 1000;
        const uploadBatch = async (table, items) => {
            for (let i = 0; i < items.length; i += BATCH_SIZE) {
                const batch = items.slice(i, i + BATCH_SIZE);
                await performUpsert(table, batch);
                const progress = Math.round((i / items.length) * 100);
                updateStatus(`Enviando ${table}... ${progress}%`, progress);
            }
        };

        try {
            if (data.history && data.history.length > 0) {
                updateStatus('Limpando histórico...', 10);
                await clearTable('data_history');
                await uploadBatch('data_history', data.history);
            }
            if (data.detailed && data.detailed.length > 0) {
                updateStatus('Limpando detalhado...', 40);
                await clearTable('data_detailed');
                await uploadBatch('data_detailed', data.detailed);
            }
            if (data.clients && data.clients.length > 0) {
                updateStatus('Limpando clientes...', 70);
                await clearTable('data_clients');
                await uploadBatch('data_clients', data.clients);
            }

            // Refresh Cache (Important for performance)
            updateStatus('Atualizando cache de filtros...', 90);
            const { error: cacheError } = await supabase.rpc('refresh_dashboard_cache');
            if (cacheError) {
                console.error("Cache refresh failed:", cacheError);
                // Non-fatal, but good to know
            }

        } catch (error) {
            console.error("Upload error:", error);
            throw error;
        }
    }

    // --- Dashboard Data Logic ---

    // Filter Elements
    const supervisorFilter = document.getElementById('supervisor-filter');
    const vendedorFilter = document.getElementById('vendedor-filter');
    const fornecedorFilter = document.getElementById('fornecedor-filter');
    const cidadeFilter = document.getElementById('cidade-filter');
    const filialFilter = document.getElementById('filial-filter');
    const anoFilter = document.getElementById('ano-filter');
    const mesFilter = document.getElementById('mes-filter');

    let currentCharts = {};

    async function initDashboard() {
        const filters = getCurrentFilters();
        await loadFilters(filters);
        await loadMainDashboardData();
    }

    function getSelectedValues(selectElement) {
        if (!selectElement) return [];
        return Array.from(selectElement.selectedOptions).map(option => option.value).filter(v => v !== '');
    }

    function getCurrentFilters() {
        return {
            p_filial: getSelectedValues(filialFilter),
            p_cidade: getSelectedValues(cidadeFilter),
            p_supervisor: getSelectedValues(supervisorFilter),
            p_vendedor: getSelectedValues(vendedorFilter),
            p_fornecedor: getSelectedValues(fornecedorFilter),
            p_ano: anoFilter.value,
            p_mes: mesFilter.value,
            p_tipovenda: getSelectedValues(document.getElementById('tipovenda-filter'))
        };
    }

    async function loadFilters(currentFilters, retryCount = 0) {
        // With dependent filters, caching is complex because every combination is unique.
        // For now, we skip cache for filters or cache by key. Given the number of combinations, simplified to fetch fresh.
        // If performance is an issue, we can cache specific common combinations.
        
        const { data, error } = await supabase.rpc('get_dashboard_filters', currentFilters);
        if (error) {
            console.error('Error loading filters:', error);
            // Enhanced error feedback & Retry Logic
            if (error.code === '57014' || error.message.includes('timeout')) {
                 console.error('Filter query timed out.');
                 if (retryCount < 1) {
                     console.log('Retrying filters load...');
                     await new Promise(r => setTimeout(r, 1000));
                     return loadFilters(currentFilters, retryCount + 1);
                 }
            }
            return;
        }

        applyFiltersData(data);
    }

    function applyFiltersData(data) {
        // Helper to preserve selection
        const updateSelect = (element, items, isObject = false) => {
            // Determine if element is multiple select
            const isMultiple = element.hasAttribute('multiple');
            let currentValues = [];
            if (isMultiple) {
                currentValues = getSelectedValues(element);
            } else {
                currentValues = [element.value];
            }

            element.innerHTML = '';
            
            const allOpt = document.createElement('option');
            allOpt.value = (element.id === 'ano-filter') ? 'todos' : '';
            allOpt.textContent = 'Todos';
            element.appendChild(allOpt);

            if (items) {
                items.forEach(item => {
                    const opt = document.createElement('option');
                    if (isObject) {
                        opt.value = item.cod;
                        opt.textContent = item.name;
                    } else {
                        opt.value = item;
                        opt.textContent = item;
                    }
                    element.appendChild(opt);
                });
            }
            
            // Restore selections
            if (currentValues.length > 0) {
                let hasSelection = false;
                Array.from(element.options).forEach(opt => {
                    if (currentValues.includes(opt.value)) {
                        opt.selected = true;
                        hasSelection = true;
                    }
                });
                // If previously "todos" was selected explicitly (like in Ano), ensure it stays if valid
                if (!hasSelection && currentValues.includes('todos')) {
                     if (element.options.length > 0) element.options[0].selected = true;
                }
            }
        };

        updateSelect(supervisorFilter, data.supervisors);
        updateSelect(vendedorFilter, data.vendedores);
        updateSelect(cidadeFilter, data.cidades);
        updateSelect(filialFilter, data.filiais);
        updateSelect(anoFilter, data.anos);
        updateSelect(fornecedorFilter, data.fornecedores, true);
        const tipovendaFilter = document.getElementById('tipovenda-filter');
        updateSelect(tipovendaFilter, data.tipos_venda);

        // Meses (Static - No changes needed unless we want to filter months dynamically)
        if (mesFilter.options.length <= 1) { 
            mesFilter.innerHTML = '<option value="">Todos</option>';
            const meses = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"];
            meses.forEach((m, i) => {
                const opt = document.createElement('option');
                opt.value = i;
                opt.textContent = m;
                mesFilter.appendChild(opt);
            });
        }
    }

    // Unified Change Handler
    const handleFilterChange = async () => {
        const filters = getCurrentFilters();
        // 1. Update Filters (Dropdowns) based on new selection
        await loadFilters(filters);
        // 2. Load Data based on new selection (which might be slightly adjusted by loadFilters if values became invalid)
        await loadMainDashboardData();
    };

    // Event Listeners
    [supervisorFilter, vendedorFilter, fornecedorFilter, cidadeFilter, filialFilter, anoFilter, mesFilter].forEach(el => {
        el.onchange = handleFilterChange;
    });

    // PopulateSelect Removed (merged into updateSelect inside applyFiltersData)
    /* 
    function populateSelect(element, items) { ... } 
    */

    async function loadMainDashboardData() {
        const filters = getCurrentFilters();

        // Cache Key based on filters (simple stringify)
        const cacheKey = `dashboard_data_${JSON.stringify(filters)}`;

        // Try Cache First
        const cachedData = await getFromCache(cacheKey);
        if (cachedData) {
            renderDashboard(cachedData);
        }

        const { data, error } = await supabase.rpc('get_main_dashboard_data', filters);

        if (error) {
            console.error('Error fetching dashboard data:', error);
            return;
        }

        // Save fresh data
        await saveToCache(cacheKey, data);
        renderDashboard(data);
    }

    function renderDashboard(data) {
        // KPIs
        document.getElementById('kpi-clients-attended').textContent = data.kpi_clients_attended.toLocaleString('pt-BR');
        const baseEl = document.getElementById('kpi-clients-base');
        if (data.kpi_clients_base > 0) {
            baseEl.textContent = `de ${data.kpi_clients_base.toLocaleString('pt-BR')} na base`;
            baseEl.classList.remove('hidden');
        } else {
            baseEl.classList.add('hidden');
        }

        let currentData = data.monthly_data_current || [];
        let previousData = data.monthly_data_previous || [];

        // Apply Month Filter to Chart and Table Data
        if (mesFilter.value !== '') {
            const selectedMonthIndex = parseInt(mesFilter.value);
            currentData = currentData.filter(d => d.month_index === selectedMonthIndex);
            previousData = previousData.filter(d => d.month_index === selectedMonthIndex);
        }

        const targetIndex = data.target_month_index;

        const currMonthData = currentData.find(d => d.month_index === targetIndex) || { faturamento: 0, peso: 0 };
        const prevMonthData = previousData.find(d => d.month_index === targetIndex) || { faturamento: 0, peso: 0 };

        const calcEvo = (curr, prev) => prev > 0 ? ((curr / prev) - 1) * 100 : (curr > 0 ? 100 : 0);

        const fatEvo = calcEvo(currMonthData.faturamento, prevMonthData.faturamento);
        const pesoEvo = calcEvo(currMonthData.peso, prevMonthData.peso);

        updateKpi('kpi-evo-vs-ano-fat', fatEvo);
        updateKpi('kpi-evo-vs-ano-kg', pesoEvo);

        // Trimestral
        let triSumFat = 0, triSumPeso = 0, triCount = 0;
        for (let i = 1; i <= 3; i++) {
            const idx = targetIndex - i;
            const mData = currentData.find(d => d.month_index === idx);
            if (mData) {
                triSumFat += mData.faturamento;
                triSumPeso += mData.peso;
                triCount++;
            }
        }
        const triAvgFat = triCount > 0 ? triSumFat / triCount : 0;
        const triAvgPeso = triCount > 0 ? triSumPeso / triCount : 0;

        const fatEvoTri = calcEvo(currMonthData.faturamento, triAvgFat);
        const pesoEvoTri = calcEvo(currMonthData.peso, triAvgPeso);

        updateKpi('kpi-evo-vs-tri-fat', fatEvoTri);
        updateKpi('kpi-evo-vs-tri-kg', pesoEvoTri);

        // Titles
        const monthNames = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"];
        const mName = monthNames[targetIndex]?.toUpperCase() || "";
        document.getElementById('kpi-title-evo-ano-fat').textContent = `FAT ${mName} vs Ano Ant.`;
        document.getElementById('kpi-title-evo-ano-kg').textContent = `TON ${mName} vs Ano Ant.`;

        // Chart
        const chartLabels = monthNames;
        const mapTo12 = (arr) => {
            const res = new Array(12).fill(0);
            arr.forEach(d => res[d.month_index] = d.faturamento);
            return res;
        };

        createChart('main-chart', 'bar', chartLabels, [
            { label: `Ano ${data.previous_year}`, data: mapTo12(previousData) },
            { label: `Ano ${data.current_year}`, data: mapTo12(currentData) }
        ]);

        // Table
        updateTable(currentData, previousData, data.current_year, data.previous_year);
    }

    function updateKpi(id, value) {
        const el = document.getElementById(id);
        el.textContent = `${value.toFixed(1)}%`;
        el.className = `text-2xl font-bold ${value >= 0 ? 'text-green-400' : 'text-red-400'}`;
    }

    function createChart(canvasId, type, labels, datasetsData) {
        const container = document.getElementById(canvasId + 'Container');
        if (!container) return;

        container.innerHTML = '';
        const newCanvas = document.createElement('canvas');
        newCanvas.id = canvasId;
        container.appendChild(newCanvas);

        const ctx = newCanvas.getContext('2d');
        const professionalPalette = { 'current': '#06b6d4', 'previous': '#f97316' };

        const datasets = datasetsData.map((d, i) => ({
            label: d.label,
            data: d.data,
            backgroundColor: i === 1 ? professionalPalette.current : professionalPalette.previous,
            borderColor: i === 1 ? professionalPalette.current : professionalPalette.previous,
            borderWidth: 1
        }));

        if (currentCharts[canvasId]) currentCharts[canvasId].destroy();

        currentCharts[canvasId] = new Chart(ctx, {
            type: type,
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { labels: { color: '#cbd5e1' } },
                    datalabels: {
                        display: true,
                        anchor: 'end',
                        align: 'top',
                        offset: 4,
                        color: '#cbd5e1',
                        font: { size: 9, weight: 'bold' },
                        formatter: (v) => (v > 1000 ? (v/1000).toFixed(0) + 'k' : v.toFixed(0))
                    }
                },
                scales: {
                    y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255, 255, 255, 0.05)' } },
                    x: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255, 255, 255, 0.05)' } }
                }
            },
            plugins: [ChartDataLabels]
        });
    }

    function updateTable(currData, prevData, currYear, prevYear) {
        const tableBody = document.getElementById('monthly-summary-table-body');
        const tableHead = document.querySelector('#monthly-summary-table thead tr');
        tableBody.innerHTML = '';

        const monthNames = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"];
        let headerHTML = '<th class="px-2 py-2 text-left">INDICADOR</th>';
        monthNames.forEach(m => headerHTML += `<th class="px-2 py-2 text-center">${m}</th>`);
        tableHead.innerHTML = headerHTML;

        const indicators = [
            { name: 'POSITIVAÇÃO', key: 'positivacao', fmt: v => v.toLocaleString('pt-BR') },
            { name: 'FATURAMENTO', key: 'faturamento', fmt: v => v.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'}) },
            { name: 'BONIFICAÇÃO', key: 'bonificacao', fmt: v => v.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'}) },
            { name: 'DEVOLUÇÃO', key: 'devolucao', fmt: v => `<span class="text-red-400">${v.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'})}</span>` },
            { name: 'TON VENDIDA', key: 'peso', fmt: v => `${(v/1000).toFixed(2)} Kg` }
        ];

        indicators.forEach(ind => {
            let rowHTML = `<tr class="table-row"><td class="font-bold p-2 text-left">${ind.name}</td>`;
            for(let i=0; i<12; i++) {
                const d = currData.find(x => x.month_index === i) || { [ind.key]: 0 };
                const val = d[ind.key] || 0;
                rowHTML += `<td class="px-2 py-1.5 text-center">${ind.fmt(val)}</td>`;
            }
            rowHTML += '</tr>';
            tableBody.innerHTML += rowHTML;
        });
    }

    async function loadCityView() {
        const filters = getCurrentFilters();

        const { data, error } = await supabase.rpc('get_city_view_data', filters);
        if(error) { console.error(error); return; }

        const activeTableBody = document.getElementById('city-active-detail-table-body');
        activeTableBody.innerHTML = data.active_clients.map(c => `
            <tr class="table-row">
                <td class="p-2">${c['Código']}</td>
                <td class="p-2">${c.fantasia || c.razaoSocial}</td>
                <td class="p-2 text-right">${(c.totalFaturamento || 0).toLocaleString('pt-BR', {style:'currency', currency: 'BRL'})}</td>
                <td class="p-2">${c.cidade}</td>
                <td class="p-2">${c.bairro}</td>
                <td class="p-2">${c.rca1 || '-'}</td>
                <td class="p-2">${c.rca2 || '-'}</td>
            </tr>
        `).join('');

        const inactiveTableBody = document.getElementById('city-inactive-detail-table-body');
        inactiveTableBody.innerHTML = data.inactive_clients.map(c => `
            <tr class="table-row">
                <td class="p-2">${c['Código']}</td>
                <td class="p-2">${c.fantasia || c.razaoSocial}</td>
                <td class="p-2">${c.cidade}</td>
                <td class="p-2">${c.bairro}</td>
                <td class="p-2 text-center">${c.ultimaCompra ? new Date(c.ultimaCompra).toLocaleDateString('pt-BR') : '-'}</td>
                <td class="p-2">${c.rca1 || '-'}</td>
                <td class="p-2">${c.rca2 || '-'}</td>
            </tr>
        `).join('');
    }

});
