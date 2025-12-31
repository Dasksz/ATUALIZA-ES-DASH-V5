
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
            // Check Profile with Timeout - 1s (Modified per user request)
            const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('Tempo limite de conexão excedido. Verifique sua internet.')), 1000));
            const profileQuery = supabase.from('profiles').select('status').eq('id', user.id).single();

            const { data: profile, error } = await Promise.race([profileQuery, timeout]);

            if (error) {
                if (error.code !== 'PGRST116') {
                    throw error;
                }
            }

            // Status handling
            const status = profile?.status || 'pendente';
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
        uploaderModal.classList.remove('hidden');
        if (window.innerWidth < 768) toggleSidebar();
    });

    closeUploaderBtn.addEventListener('click', () => {
        uploaderModal.classList.add('hidden');
    });

    // --- Dashboard Internal Navigation ---
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

    // Elements for Credentials
    const supabaseUrlInput = document.getElementById('supabase-url-input');
    const supabaseKeyInput = document.getElementById('supabase-key-input');

    const checkFiles = () => {
        const hasCredentials = supabaseUrlInput.value.trim() !== '' && supabaseKeyInput.value.trim() !== '';
        const hasFiles = files.salesPrevYearFile && files.salesCurrYearFile && files.salesCurrMonthFile && files.clientsFile && files.productsFile;
        generateBtn.disabled = !(hasFiles && hasCredentials);
    };

    if(supabaseUrlInput) supabaseUrlInput.addEventListener('input', checkFiles);
    if(supabaseKeyInput) supabaseKeyInput.addEventListener('input', checkFiles);

    if(salesPrevYearInput) salesPrevYearInput.addEventListener('change', (e) => { files.salesPrevYearFile = e.target.files[0]; checkFiles(); });
    if(salesCurrYearInput) salesCurrYearInput.addEventListener('change', (e) => { files.salesCurrYearFile = e.target.files[0]; checkFiles(); });
    if(salesCurrMonthInput) salesCurrMonthInput.addEventListener('change', (e) => { files.salesCurrMonthFile = e.target.files[0]; checkFiles(); });
    if(clientsFileInput) clientsFileInput.addEventListener('change', (e) => { files.clientsFile = e.target.files[0]; checkFiles(); });
    if(productsFileInput) productsFileInput.addEventListener('change', (e) => { files.productsFile = e.target.files[0]; checkFiles(); });

    if(generateBtn) generateBtn.addEventListener('click', () => {
        if (!files.salesPrevYearFile || !files.salesCurrYearFile || !files.salesCurrMonthFile || !files.clientsFile || !files.productsFile) return;

        const supabaseUrl = supabaseUrlInput.value.trim();
        const supabaseKey = supabaseKeyInput.value.trim();

        if (!supabaseUrl || !supabaseKey) {
            alert('Por favor, informe a URL e a Chave Secreta do Supabase.');
            return;
        }

        generateBtn.disabled = true;
        statusContainer.classList.remove('hidden');
        statusText.textContent = 'Iniciando processamento e upload...';
        progressBar.style.width = '0%';

        const worker = new Worker('src/js/worker.js');

        worker.postMessage({
            salesPrevYearFile: files.salesPrevYearFile,
            salesCurrYearFile: files.salesCurrYearFile,
            salesCurrMonthFile: files.salesCurrMonthFile,
            clientsFile: files.clientsFile,
            productsFile: files.productsFile,
            supabaseCredentials: {
                url: supabaseUrl,
                key: supabaseKey
            }
        });

        worker.onmessage = (event) => {
            const { type, status, percentage, message } = event.data;
            if (type === 'progress') {
                statusText.textContent = status;
                progressBar.style.width = `${percentage}%`;
            } else if (type === 'result') {
                statusText.textContent = 'Dados atualizados com sucesso!';
                progressBar.style.width = '100%';
                setTimeout(() => {
                    uploaderModal.classList.add('hidden');
                    statusContainer.classList.add('hidden');
                    generateBtn.disabled = false;
                    initDashboard(); // Reload data
                }, 1500);
            } else if (type === 'error') {
                statusText.innerHTML = `<span class="text-red-500">Erro: ${message}</span>`;
                generateBtn.disabled = false;
            }
        };
    });

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
        await loadFilters();
        await loadMainDashboardData();
    }

    async function loadFilters() {
        // Try Cache First
        const cachedFilters = await getFromCache('dashboard_filters');
        if (cachedFilters) {
            console.log('Loading filters from cache...');
            applyFiltersData(cachedFilters);
        }

        // Fetch Fresh Data
        const { data, error } = await supabase.rpc('get_dashboard_filters');
        if (error) {
            console.error('Error loading filters:', error);
            return;
        }

        // Update Cache and UI if different (or just update always)
        await saveToCache('dashboard_filters', data);
        applyFiltersData(data);
    }

    function applyFiltersData(data) {
        // Populate Selects
        populateSelect(supervisorFilter, data.supervisors);
        populateSelect(vendedorFilter, data.vendedores);
        populateSelect(cidadeFilter, data.cidades);
        populateSelect(filialFilter, data.filiais);
        populateSelect(anoFilter, data.anos);

        // Fornecedores (Object array)
        fornecedorFilter.innerHTML = '<option value="">Todos</option>';
        if(data.fornecedores) {
            data.fornecedores.forEach(f => {
                const opt = document.createElement('option');
                opt.value = f.cod;
                opt.textContent = f.name;
                fornecedorFilter.appendChild(opt);
            });
        }

        // Meses (Static)
        // Check if options already exist to avoid duplication on re-render from cache then net
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

        // Event Listeners (remove old ones if re-init? simplified here)
        [supervisorFilter, vendedorFilter, fornecedorFilter, cidadeFilter, filialFilter, anoFilter, mesFilter].forEach(el => {
            el.onchange = loadMainDashboardData; // Replaces previous listener
        });
    }

    function populateSelect(element, items) {
        element.innerHTML = '<option value="">Todos</option>';
        if (element.id === 'ano-filter') element.options[0].value = 'todos';

        if (items) {
            items.forEach(item => {
                const opt = document.createElement('option');
                opt.value = item;
                opt.textContent = item;
                element.appendChild(opt);
            });
        }
    }

    async function loadMainDashboardData() {
        const filters = {
            p_filial: filialFilter.value,
            p_cidade: cidadeFilter.value,
            p_supervisor: supervisorFilter.value,
            p_vendedor: vendedorFilter.value,
            p_fornecedor: fornecedorFilter.value,
            p_ano: anoFilter.value,
            p_mes: mesFilter.value
        };

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

        const currentData = data.monthly_data_current || [];
        const previousData = data.monthly_data_previous || [];

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
        const filters = {
            p_filial: filialFilter.value,
            p_cidade: cidadeFilter.value,
            p_supervisor: supervisorFilter.value,
            p_vendedor: vendedorFilter.value,
            p_fornecedor: fornecedorFilter.value,
            p_ano: anoFilter.value,
            p_mes: mesFilter.value
        };

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
