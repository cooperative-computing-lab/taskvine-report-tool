import { initModules } from './module_definitions.js';


const logSelector = document.getElementById('log-selector');

function hideLoadingSpinner(vizId) {
    const spinner = document.getElementById(`${vizId}-loading`);
    if (spinner) {
        spinner.style.display = 'none';
    }
}

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

async function changeRuntimeTemplateWithRetry(retryCount = 0) {
    try {
        document.querySelectorAll('.error-tip').forEach(tip => {
            tip.style.visibility = 'hidden';
        });

        const response = await fetch(`/api/change-runtime-template?runtime_template=${logSelector.value}`);
        const result = await response.json();
        
        if (result.success) {
            // create a custom event that includes visualization IDs
            const dataLoadedEvent = new CustomEvent('dataLoaded', {
                detail: {
                    hideSpinner: (vizId) => hideLoadingSpinner(vizId)
                }
            });
            document.dispatchEvent(dataLoadedEvent);
        } else {
            console.error('Failed to change runtime template, retrying in 8 seconds...');
            if (retryCount < 3) {
                setTimeout(() => changeRuntimeTemplateWithRetry(retryCount + 1), 8000);
            } else {
                // if all retries failed, hide all spinners
                visualizations.forEach(viz => hideLoadingSpinner(viz));
            }
        }
    } catch (error) {
        console.error('Error changing runtime template:', error);
        console.log('Retrying in 8 seconds...');
        if (retryCount < 3) {
            setTimeout(() => changeRuntimeTemplateWithRetry(retryCount + 1), 8000);
        } else {
            // if all retries failed, hide all spinners
            visualizations.forEach(viz => hideLoadingSpinner(viz));
        }
    }
}

initModules();
document.addEventListener('DOMContentLoaded', initializeLogViewer);
