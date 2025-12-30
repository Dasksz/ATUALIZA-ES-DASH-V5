self.importScripts('https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js');
self.importScripts('https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.39.7/dist/umd/supabase.min.js');

const SUPABASE_URL = 'https://vawrdqreibhlfsfvxbpv.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZhd3JkcXJlaWJobGZzZnZ4YnB2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjcwNzg1MTAsImV4cCI6MjA4MjY1NDUxMH0.-mAobZK_dc3QOwey3Z8NbrtybWPoPRfBqW_IN0gehl8';
const supabase = self.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);

function parseDate(dateString) {
    if (!dateString) return null;
    if (dateString instanceof Date) return !isNaN(dateString.getTime()) ? dateString : null;
    if (typeof dateString === 'number') return new Date(Math.round((dateString - 25569) * 86400 * 1000));
    if (typeof dateString !== 'string') return null;

    const parts = dateString.split('/');
    if (parts.length === 3) {
        const [day, month, year] = parts;
        if (day.length === 2 && month.length === 2 && year.length === 4) return new Date(`${year}-${month}-${day}T00:00:00`);
    }

    const isoDate = new Date(dateString);
    return !isNaN(isoDate.getTime()) ? isoDate : null;
}

function parseBrazilianNumber(value) {
    if (typeof value === 'number') return value;
    if (typeof value !== 'string' || !value) return 0;
    const cleaned = String(value).replace(/R\$\s?/g, '').trim();
    const lastComma = cleaned.lastIndexOf(',');
    const lastDot = cleaned.lastIndexOf('.');
    let numberString;
    if (lastComma > lastDot) {
        numberString = cleaned.replace(/\./g, '').replace(',', '.');
    } else if (lastDot > lastComma) {
        numberString = cleaned.replace(/,/g, '');
    } else {
        numberString = cleaned.replace(',', '.');
    }
    const number = parseFloat(numberString);
    return isNaN(number) ? 0 : number;
}

const readFile = (file) => {
    return new Promise((resolve, reject) => {
        if (!file) {
            resolve([]);
            return;
        }
        const reader = new FileReader();
        reader.onload = (event) => {
            try {
                let jsonData;
                const data = event.target.result;
                if (file.name.endsWith('.csv')) {
                    let decodedData;
                    try {
                        decodedData = new TextDecoder('utf-8', { fatal: true }).decode(new Uint8Array(data));
                    } catch (e) {
                        decodedData = new TextDecoder('iso-8859-1').decode(new Uint8Array(data));
                    }

                    const lines = decodedData.split(/\r?\n/).filter(line => line.trim() !== '');
                    if (lines.length < 1) {
                        resolve([]);
                        return;
                    };

                    const firstLine = lines[0];
                    const delimiter = firstLine.includes(';') ? ';' : ',';
                    const headers = lines.shift().trim().split(delimiter).map(h => h.replace(/"/g, '').trim().replace(/^\uFEFF/, ''));

                    jsonData = lines.map(line => {
                        const values = line.split(delimiter).map(v => v.replace(/"/g, ''));
                        let row = {};
                        headers.forEach((header, index) => {
                            row[header] = values[index] || null;
                        });
                        return row;
                    });
                } else {
                    const workbook = XLSX.read(new Uint8Array(data), {type: 'array'});
                    const firstSheetName = workbook.SheetNames[0];
                    const worksheet = workbook.Sheets[firstSheetName];
                    jsonData = XLSX.utils.sheet_to_json(worksheet, { raw: false, cellDates: true });
                }
                resolve(jsonData);
            } catch (error) {
                reject(error);
            }
        };
        reader.onerror = () => reject(new Error(`Erro ao ler o arquivo '${file.name}'.`));
        reader.readAsArrayBuffer(file);
    });
};

const processSalesData = (rawData, clientMap, productMasterMap) => {
    return rawData.map(rawRow => {
        const clientInfo = clientMap.get(String(rawRow['CODCLI']).trim()) || {};
        let vendorName = String(rawRow['NOME'] || '');
        let supervisorName = String(rawRow['SUPERV'] || '');
        let codUsur = String(rawRow['CODUSUR'] || '');
        const pedido = String(rawRow['PEDIDO'] || '');
        if (supervisorName.trim().toUpperCase() === 'OSÉAS SANTOS OL') supervisorName = 'OSVALDO NUNES O';

        const supervisorUpper = (supervisorName || '').trim().toUpperCase();
        if (supervisorUpper === 'BALCAO' || supervisorUpper === 'BALCÃO') supervisorName = 'BALCAO';

        if (pedido.startsWith('120')) {
            vendorName = 'VD HIAGO';
            supervisorName = 'HIAGO ASSUNCAO';
            codUsur = '1002';
        }

        let dtPed = rawRow['DTPED'];
        const dtSaida = rawRow['DTSAIDA'];
        const parsedDtPed = parseDate(dtPed);
        const parsedDtSaida = parseDate(dtSaida);
        if (parsedDtPed && parsedDtSaida && (parsedDtPed.getFullYear() < parsedDtSaida.getFullYear() || (parsedDtPed.getFullYear() === parsedDtSaida.getFullYear() && parsedDtPed.getMonth() < parsedDtSaida.getMonth()))) {
            dtPed = dtSaida;
        }
        const productCode = String(rawRow['PRODUTO'] || '').trim();
        const qtdeMaster = productMasterMap.get(productCode) || 1;
        const qtVenda = parseInt(String(rawRow['QTVENDA'] || '0').trim(), 10);

        let filialValue = String(rawRow['FILIAL'] || '').trim();
        if (filialValue === '5') filialValue = '05';
        if (filialValue === '8') filialValue = '08';

        return {
            pedido: pedido,
            nome: vendorName,
            superv: supervisorName,
            produto: productCode,
            descricao: String(rawRow['DESCRICAO'] || ''),
            fornecedor: String(rawRow['FORNECEDOR'] || ''),
            observacaofor: String(rawRow['OBSERVACAOFOR'] || '').trim(),
            codfor: String(rawRow['CODFOR'] || '').trim(),
            codusur: codUsur,
            codcli: String(rawRow['CODCLI'] || '').trim(),
            cliente_nome: clientInfo.nomeCliente || 'N/A',
            cidade: clientInfo.cidade || 'N/A',
            bairro: clientInfo.bairro || 'N/A',
            qtvenda: qtVenda,
            vlvenda: parseBrazilianNumber(rawRow['VLVENDA']),
            vlbonific: parseBrazilianNumber(rawRow['VLBONIFIC']),
            vldevolucao: parseBrazilianNumber(rawRow['VLDEVOLUCAO']),
            totpesoliq: parseBrazilianNumber(rawRow['TOTPESOLIQ']),
            dtped: parsedDtPed ? parsedDtPed.toISOString() : null,
            dtsaida: parsedDtSaida ? parsedDtSaida.toISOString() : null,
            posicao: String(rawRow['POSICAO'] || ''),
            filial: filialValue,
            codsupervisor: String(rawRow['CODSUPERVISOR'] || '').trim(),
            estoqueunit: parseBrazilianNumber(rawRow['ESTOQUEUNIT']),
            qtvenda_embalagem_master: isNaN(qtdeMaster) || qtdeMaster === 0 ? 0 : qtVenda / qtdeMaster
        };
    });
};

const BATCH_SIZE = 1000;

async function batchInsert(table, data) {
    for (let i = 0; i < data.length; i += BATCH_SIZE) {
        const batch = data.slice(i, i + BATCH_SIZE);
        const { error } = await supabase.from(table).insert(batch);
        if (error) throw error;
        self.postMessage({ type: 'progress', status: `Inserindo ${table}...`, percentage: 80 + Math.round((i / data.length) * 15) });
    }
}

self.onmessage = async (event) => {
    const { salesPrevYearFile, salesCurrYearFile, salesCurrMonthFile, clientsFile, productsFile } = event.data;

    try {
        // Step 0: Clear Data (Assume user wants full refresh)
        self.postMessage({ type: 'progress', status: 'Limpando banco de dados...', percentage: 0 });
        const { error: rpcError } = await supabase.rpc('clear_all_data');
        if (rpcError) throw new Error('Erro ao limpar dados: ' + rpcError.message);

        self.postMessage({ type: 'progress', status: 'Lendo arquivos...', percentage: 5 });
        let [salesPrevYearDataRaw, salesCurrYearHistDataRaw, salesCurrMonthDataRaw, clientsDataRaw, productsDataRaw] = await Promise.all([
            readFile(salesPrevYearFile),
            readFile(salesCurrYearFile),
            readFile(salesCurrMonthFile),
            readFile(clientsFile),
            readFile(productsFile)
        ]);

        self.postMessage({ type: 'progress', status: 'Filtrando vendas Pepsico...', percentage: 15 });
        const pepsicoFilter = (sale) => String(sale['OBSERVACAOFOR'] || '').trim().toUpperCase() === 'PEPSICO';

        salesPrevYearDataRaw = salesPrevYearDataRaw.filter(pepsicoFilter);
        salesCurrYearHistDataRaw = salesCurrYearHistDataRaw.filter(pepsicoFilter);
        salesCurrMonthDataRaw = salesCurrMonthDataRaw.filter(pepsicoFilter);

        // Upload Clients First (Dependency for maps)
        self.postMessage({ type: 'progress', status: 'Processando clientes...', percentage: 20 });
        const clientMap = new Map();
        const clientsToInsert = [];

        clientsDataRaw.forEach(client => {
            const codCli = String(client['Código'] || '').trim();
            if (!codCli) return;

            const rca1 = String(client['RCA 1'] || '');
            const rca2 = String(client['RCA 2'] || '');
            const ultimaCompraRaw = client['Data da Última Compra'];
            const ultimaCompra = parseDate(ultimaCompraRaw);

            const clientData = {
                codigo_cliente: codCli,
                rca1: rca1,
                rca2: rca2,
                cidade: String(client['Nome da Cidade'] || 'N/A'),
                nomecliente: String(client['Fantasia'] || client['Cliente'] || 'N/A'),
                bairro: String(client['Bairro'] || 'N/A'),
                razaosocial: String(client['Cliente'] || 'N/A'),
                fantasia: String(client['Fantasia'] || 'N/A'),
                ramo: String(client['Descricao'] || 'N/A'),
                ultimacompra: ultimaCompra ? ultimaCompra.toISOString() : null,
                bloqueio: String(client['Bloqueio'] || '').trim().toUpperCase(),
            };

            clientMap.set(codCli, {
                nomeCliente: clientData.nomecliente,
                cidade: clientData.cidade,
                bairro: clientData.bairro,
                rca1: rca1
            });
            clientsToInsert.push(clientData);
        });

        await batchInsert('data_clients', clientsToInsert);

        self.postMessage({ type: 'progress', status: 'Mapeando produtos...', percentage: 30 });
        const productMasterMap = new Map();
        productsDataRaw.forEach(prod => {
            const productCode = String(prod['Código'] || '').trim();
            if (!productCode) return;
            let qtdeMaster = parseInt(prod['Qtde embalagem master(Compra)'], 10);
            if (isNaN(qtdeMaster) || qtdeMaster <= 0) qtdeMaster = 1;
            productMasterMap.set(productCode, qtdeMaster);
        });

        // Combine Sales for Map Logic
        const allSalesRaw = [...salesPrevYearDataRaw, ...salesCurrYearHistDataRaw, ...salesCurrMonthDataRaw];

        self.postMessage({ type: 'progress', status: 'Criando mapa mestre de vendedores...', percentage: 40 });
        const rcaInfoMap = new Map();
        allSalesRaw.sort((a, b) => {
            const dateA = parseDate(a.DTPED) || new Date(0);
            const dateB = parseDate(b.DTPED) || new Date(0);
            return dateA - dateB;
        });

        for (const row of allSalesRaw) {
            const codusur = String(row['CODUSUR'] || '').trim();
            if (!codusur) continue;
            let supervisor = String(row['SUPERV'] || '').trim();
            const nome = String(row['NOME'] || '').trim();
            if (supervisor.trim().toUpperCase() === 'OSÉAS SANTOS OL') supervisor = 'OSVALDO NUNES O';
            const supervisorUpper = (supervisor || '').trim().toUpperCase();
            if (supervisorUpper === 'BALCAO' || supervisorUpper === 'BALCÃO') supervisor = 'BALCAO';
            const existingEntry = rcaInfoMap.get(codusur);
            if (!existingEntry) {
                rcaInfoMap.set(codusur, { NOME: nome || 'N/A', SUPERV: supervisor || 'N/A' });
            } else {
                if (nome) existingEntry.NOME = nome;
                if (supervisor) existingEntry.SUPERV = supervisor;
            }
        }

        self.postMessage({ type: 'progress', status: 'Processando e Reatribuindo vendas...', percentage: 50 });
        const reattributeSales = (salesData) => {
            const balcaoSpecialClients = new Set(['6421', '7706', '9814']);
            return salesData.map(sale => {
                const originalCodCli = String(sale['CODCLI'] || '').trim();
                const originalCodUsur = String(sale['CODUSUR'] || '').trim();
                const newSale = { ...sale };

                if (originalCodCli === '9569' && originalCodUsur === '53') {
                    newSale['CODUSUR'] = 'BALCAO_SP'; newSale['NOME'] = 'BALCAO'; newSale['SUPERV'] = 'BALCAO'; newSale['CODCLI'] = '7706'; return newSale;
                }
                if (newSale.CODUSUR === '1002') return newSale;

                if (balcaoSpecialClients.has(originalCodCli)) {
                    newSale['CODUSUR'] = 'BALCAO_SP'; newSale['NOME'] = 'BALCAO'; newSale['SUPERV'] = 'BALCAO'; return newSale;
                }

                const clientData = clientMap.get(originalCodCli);
                if (!clientData) {
                    newSale['CODUSUR'] = 'INATIVO'; newSale['NOME'] = 'Inativo'; newSale['SUPERV'] = 'INATIVOS'; return newSale;
                }
                const rca1 = String(clientData.rca1 || '').trim();
                if (!rca1 || rca1 === '53') {
                    newSale['CODUSUR'] = 'INATIVO'; newSale['NOME'] = 'Inativo'; newSale['SUPERV'] = 'INATIVOS'; return newSale;
                }
                if (rcaInfoMap.has(rca1)) {
                    const newOwnerInfo = rcaInfoMap.get(rca1);
                    newSale['CODUSUR'] = rca1; newSale['NOME'] = newOwnerInfo.NOME; newSale['SUPERV'] = newOwnerInfo.SUPERV;
                } else {
                    newSale['CODUSUR'] = 'INATIVO'; newSale['NOME'] = 'Inativo'; newSale['SUPERV'] = 'INATIVOS';
                }
                return newSale;
            });
        };

        const reattributedPrevYear = reattributeSales(salesPrevYearDataRaw);
        const reattributedCurrYearHist = reattributeSales(salesCurrYearHistDataRaw);
        const reattributedCurrMonth = reattributeSales(salesCurrMonthDataRaw);

        const processedPrevYear = processSalesData(reattributedPrevYear, clientMap, productMasterMap);
        const processedCurrYearHist = processSalesData(reattributedCurrYearHist, clientMap, productMasterMap);
        const processedCurrMonth = processSalesData(reattributedCurrMonth, clientMap, productMasterMap);

        // Branch Override Logic
        const allProcessedSales = [...processedPrevYear, ...processedCurrYearHist, ...processedCurrMonth];
        const clientLastBranch = new Map();
        const clientsWith05Purchase = new Set();
        allProcessedSales.forEach(sale => {
            if (sale.codcli && sale.filial) {
                clientLastBranch.set(sale.codcli, sale.filial);
                if (sale.filial === '05') clientsWith05Purchase.add(sale.codcli);
            }
        });
        const clientBranchOverride = new Map();
        clientsWith05Purchase.forEach(codCli => {
            if (clientLastBranch.get(codCli) === '08') clientBranchOverride.set(codCli, '08');
        });

        const applyBranchOverride = (salesArray) => salesArray.map(sale => {
            const override = clientBranchOverride.get(sale.codcli);
            return (override && sale.filial === '05') ? { ...sale, filial: override } : sale;
        });

        const tiagoSellersToMoveTo08 = new Set(['291', '292', '293', '284', '289', '287', '286']);
        const applyTiagoRule = (salesArray) => salesArray.map(sale =>
            (sale.codsupervisor === '12' && tiagoSellersToMoveTo08.has(sale.codusur)) ? { ...sale, filial: '08' } : sale
        );

        const applyAllRules = (salesData) => applyTiagoRule(applyBranchOverride(salesData));

        const finalPrevYear = applyAllRules(processedPrevYear);
        const finalCurrYearHist = applyAllRules(processedCurrYearHist);
        const finalCurrMonth = applyAllRules(processedCurrMonth);

        // Insert Data
        self.postMessage({ type: 'progress', status: 'Enviando dados para o banco...', percentage: 70 });

        // Prev Year -> data_history
        await batchInsert('data_history', finalPrevYear);

        // Curr Year History -> data_history
        await batchInsert('data_history', finalCurrYearHist);

        // Curr Month -> data_detailed
        await batchInsert('data_detailed', finalCurrMonth);

        self.postMessage({ type: 'result', data: { success: true } });

    } catch (error) {
        self.postMessage({ type: 'error', message: error.message + (error.stack ? `\nStack: ${error.stack}`: '') });
    }
};
