const logSelector = document.getElementById('log-selector');


async function initializeLogViewer() {
    try {
        const response = await fetch('/api/runtime-template-list');
        const logFolders = await response.json();
        
        logSelector.innerHTML = '';
        
        const defaultOption = document.createElement('option');
        defaultOption.value = '';
        defaultOption.textContent = '--- select log ---';
        defaultOption.selected = true;
        logSelector.appendChild(defaultOption);
        
        logFolders.forEach(folder => {
            const option = document.createElement('option');
            option.value = folder;
            option.textContent = folder;
            logSelector.appendChild(option);
        });

        logSelector.addEventListener('change', handleLogChange);

        // restore previously selected log folder
        const savedFolder = sessionStorage.getItem('selectedLogFolder');
        if (savedFolder && logFolders.includes(savedFolder)) {
            logSelector.value = savedFolder;
            // only trigger change event if there was a saved selection
            logSelector.dispatchEvent(new Event('change'));
        }
    } catch (error) {
        console.error('Error initializing log viewer:', error);
    }
}

async function handleLogChange() {
    // only proceed if a valid option is selected (not the default one)
    if (!logSelector.value) {
        return;
    }
    
    // save current selection to sessionStorage
    sessionStorage.setItem('selectedLogFolder', logSelector.value);
    
    // try to change the runtime template with retry mechanism
    await changeRuntimeTemplateWithRetry();
}

async function changeRuntimeTemplateWithRetry() {
    try {
        document.querySelectorAll('.error-tip').forEach(tip => {
            tip.style.visibility = 'hidden';
        });

        const response = await fetch(`/api/change-runtime-template?runtime_template=${logSelector.value}`);
        const result = await response.json();
        
        if (result.success) {
            document.dispatchEvent(new Event('dataLoaded'));
        } else {
            console.error('Failed to change runtime template, retrying in 8 seconds...');
            // retry after 8 seconds
            setTimeout(() => changeRuntimeTemplateWithRetry(), 8000);
        }
    } catch (error) {
        console.error('Error changing runtime template:', error);
        console.log('Retrying in 8 seconds...');
        // retry after 8 seconds
        setTimeout(() => changeRuntimeTemplateWithRetry(), 8000);
    }
}

document.addEventListener('DOMContentLoaded', initializeLogViewer);
