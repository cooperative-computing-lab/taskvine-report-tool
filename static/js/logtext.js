
window.addEventListener('load', function()  {
    const logSelector = window.parent.document.getElementById('log-selector');
    logSelector.addEventListener('change', () => {
        const logName = logSelector.value;
        // load debug log
        const debugFilePath = `logs/${logName}/vine-logs/debug`;
        const debugElementID = 'log-text-debug';
        loadLogFile(debugFilePath, debugElementID);

        // load performance log
        const performanceFilePath = `logs/${logName}/vine-logs/performance`;
        const performanceElementID = 'log-text-performance';
        loadLogFile(performanceFilePath, performanceElementID);

        // load transactions log
        const transactionsFilePath = `logs/${logName}/vine-logs/transactions`;
        const transactionsElementID = 'log-text-transactions';
        loadLogFile(transactionsFilePath, transactionsElementID);
    });
    logSelector.dispatchEvent(new Event('change'));
});

async function loadLogFile(filePath, elementId) {
    try {
        const response = await fetch(filePath);
        if (!response.ok) {
            throw new Error(`Failed to fetch file: ${filePath} (${response.statusText})`);
        }
        const text = await response.text();
        const element = document.getElementById(elementId);
        if (element) {
            element.textContent = text;
        } else {
            console.error(`Element with id "${elementId}" not found`);
        }
    } catch (error) {
        console.error('Error fetching file:', error.message);
        const element_1 = document.getElementById(elementId);
        if (element_1) {
            element_1.textContent = `Error loading file: ${filePath}`;
        } else {
            console.error(`Element with id "${elementId}" not found`);
        }
    }
}

