
import supabase from './supabase.js';

document.addEventListener('DOMContentLoaded', () => {
    const mainDashboard = document.getElementById('main-dashboard');
    const cityView = document.getElementById('city-view');
    const uploadScreen = document.getElementById('upload-screen');
    const dashboardContainer = document.getElementById('dashboard-container');

    // Check if data exists? Or just show dashboard if no upload pending.
    // For now, let's keep the Upload Screen as default, but add a "View Dashboard" button if user just wants to view.
    // However, the prompt implies "Upload -> Process -> View".

    // Filter Elements
    const supervisorFilter = document.getElementById('supervisor-filter');
    const vendedorFilter = document.getElementById('vendedor-filter');
    const fornecedorFilter = document.getElementById('fornecedor-filter');
    const cidadeFilter = document.getElementById('cidade-filter');
    const filialFilter = document.getElementById('filial-filter');
    const anoFilter = document.getElementById('ano-filter');
    const mesFilter = document.getElementById('mes-filter');

    const showCityBtn = document.getElementById('show-city-btn');
    const backToMainBtn = document.getElementById('back-to-main-btn');
    const refreshBtn = document.getElementById('refresh-btn');

    let currentCharts = {};

    // --- Upload Logic ---
    const salesPrevYearInput = document.getElementById('sales-prev-year-input');
    const salesCurrYearInput = document.getElementById('sales-curr-year-input');
    const salesCurrMonthInput = document.getElementById('sales-curr-month-input');
    const clientsFileInput = document.getElementById('clients-file-input');
    const productsFileInput = document.getElementById('products-file-input');
    const generateBtn = document.getElementById('generate-btn');
    const viewDashBtn = document.getElementById('view-dash-btn'); // New button

    const statusContainer = document.getElementById('status-container');
    const statusText = document.getElementById('status-text');
    const progressBar = document.getElementById('progress-bar');

    let files = {};

    const checkFiles = () => {
        generateBtn.disabled = !(files.salesPrevYearFile && files.salesCurrYearFile && files.salesCurrMonthFile && files.clientsFile && files.productsFile);
    };

    if(salesPrevYearInput) salesPrevYearInput.addEventListener('change', (e) => { files.salesPrevYearFile = e.target.files[0]; checkFiles(); });
    if(salesCurrYearInput) salesCurrYearInput.addEventListener('change', (e) => { files.salesCurrYearFile = e.target.files[0]; checkFiles(); });
    if(salesCurrMonthInput) salesCurrMonthInput.addEventListener('change', (e) => { files.salesCurrMonthFile = e.target.files[0]; checkFiles(); });
    if(clientsFileInput) clientsFileInput.addEventListener('change', (e) => { files.clientsFile = e.target.files[0]; checkFiles(); });
    if(productsFileInput) productsFileInput.addEventListener('change', (e) => { files.productsFile = e.target.files[0]; checkFiles(); });

    if(generateBtn) generateBtn.addEventListener('click', () => {
        if (!files.salesPrevYearFile || !files.salesCurrYearFile || !files.salesCurrMonthFile || !files.clientsFile || !files.productsFile) return;

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
            productsFile: files.productsFile
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
                    uploadScreen.classList.add('hidden');
                    dashboardContainer.classList.remove('hidden');
                    initDashboard();
                }, 1000);
            } else if (type === 'error') {
                statusText.innerHTML = `<span class="text-red-500">Erro: ${message}</span>`;
                generateBtn.disabled = false;
            }
        };
    });

    if(viewDashBtn) viewDashBtn.addEventListener('click', () => {
        uploadScreen.classList.add('hidden');
        dashboardContainer.classList.remove('hidden');
        initDashboard();
    });

    // --- Dashboard Logic ---

    async function initDashboard() {
        await loadFilters();
        await loadMainDashboardData();
    }

    async function loadFilters() {
        const { data, error } = await supabase.rpc('get_dashboard_filters');
        if (error) {
            console.error('Error loading filters:', error);
            return;
        }

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
        mesFilter.innerHTML = '<option value="">Todos</option>';
        const meses = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"];
        meses.forEach((m, i) => {
            const opt = document.createElement('option');
            opt.value = i;
            opt.textContent = m;
            mesFilter.appendChild(opt);
        });

        // Event Listeners
        [supervisorFilter, vendedorFilter, fornecedorFilter, cidadeFilter, filialFilter, anoFilter, mesFilter].forEach(el => {
            el.addEventListener('change', loadMainDashboardData);
        });
    }

    function populateSelect(element, items) {
        element.innerHTML = '<option value="">Todos</option>'; // Or 'todos' for ano
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

        const { data, error } = await supabase.rpc('get_main_dashboard_data', filters);

        if (error) {
            console.error('Error fetching dashboard data:', error);
            return;
        }

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

        // Monthly Data Processing for Chart & Table
        const currentData = data.monthly_data_current || [];
        const previousData = data.monthly_data_previous || [];

        // KPI Evolution Logic (Simplified matching index.html logic)
        // Need to find "Current Month" in data
        const targetIndex = data.target_month_index;

        const currMonthData = currentData.find(d => d.month_index === targetIndex) || { faturamento: 0, peso: 0 };
        const prevMonthData = previousData.find(d => d.month_index === targetIndex) || { faturamento: 0, peso: 0 };

        const calcEvo = (curr, prev) => prev > 0 ? ((curr / prev) - 1) * 100 : (curr > 0 ? 100 : 0);

        const fatEvo = calcEvo(currMonthData.faturamento, prevMonthData.faturamento);
        const pesoEvo = calcEvo(currMonthData.peso, prevMonthData.peso);

        updateKpi('kpi-evo-vs-ano-fat', fatEvo);
        updateKpi('kpi-evo-vs-ano-kg', pesoEvo);

        // Trimestral (Last 3 months including target)
        // Logic: Sum last 3 months
        let currTriFat = 0, currTriPeso = 0, prevTriFat = 0, prevTriPeso = 0; // "Prev Tri" usually means Prev Year Same Tri? Or Prev 3 Months of Current Year?
        // Index.html: "FAT vs Trim. Ant." usually compares Current Month vs Average of Previous 3 Months (Sequential).
        // Let's check index.html logic:
        // `avgFatTri = monthCount > 0 ? totalFatTri / monthCount : 0;` (Average of previous 3 months)
        // `fatEvoVsTri = avgFatTri > 0 ? ((currentMonthFat / avgFatTri) - 1) * 100`
        // So it compares Current Month vs Average of Last 3 Months.

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

        // Update Titles
        const monthNames = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"];
        const mName = monthNames[targetIndex]?.toUpperCase() || "";
        document.getElementById('kpi-title-evo-ano-fat').textContent = `FAT ${mName} vs Ano Ant.`;
        document.getElementById('kpi-title-evo-ano-kg').textContent = `TON ${mName} vs Ano Ant.`;

        // Chart
        const chartLabels = monthNames;
        // Map data to 12 months array
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

        // Header
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

        // We show CURRENT YEAR data in the table by default, or maybe handle comparison?
        // Index.html logic: Shows 12 months columns.
        // It shows EITHER Current OR Previous year based on filters.
        // Let's stick to Current Year for now as default.

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

    // --- City View ---
    showCityBtn.addEventListener('click', () => {
        mainDashboard.classList.add('hidden');
        cityView.classList.remove('hidden');
        loadCityView();
    });

    backToMainBtn.addEventListener('click', () => {
        cityView.classList.add('hidden');
        mainDashboard.classList.remove('hidden');
    });

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
