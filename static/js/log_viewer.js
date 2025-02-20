const logSelector = document.getElementById('log-selector');


async function initializeLogViewer() {
    try {
        const response = await fetch('/api/runtime-templates-list');
        const logFolders = await response.json();
        
        logSelector.innerHTML = '';
        
        logFolders.forEach(folder => {
            const option = document.createElement('option');
            option.value = folder;
            option.textContent = folder;
            logSelector.appendChild(option);
        });

        logSelector.addEventListener('change', handleLogChange);

        if (logSelector.options.length > 0) {
            logSelector.selectedIndex = 0;
            logSelector.dispatchEvent(new Event('change'));
        }
    } catch (error) {
        console.error('Error initializing log viewer:', error);
    }
}

async function handleLogChange() {
    try {
        document.querySelectorAll('.error-tip').forEach(tip => {
            tip.style.visibility = 'hidden';
        });

        const response = await fetch(`/api/change-runtime-template?runtime_template=${logSelector.value}`);
        const result = await response.json();
        
        if (result.success) {
            document.dispatchEvent(new Event('dataLoaded'));
        } else {
            console.error('Failed to change runtime template');
        }
    } catch (error) {
        console.error('Error changing runtime template:', error);
    }
}

document.addEventListener('DOMContentLoaded', initializeLogViewer);
