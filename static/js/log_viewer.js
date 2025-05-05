const logSelector = document.getElementById('log-selector');


async function initializeLogViewer() {
    try {
        const response = await fetch('/api/runtime-template-list');
        const logFolders = await response.json();
        
        logSelector.innerHTML = '';
        
        // Add a default "please select" option
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

        // Restore previously selected log folder from localStorage
        const savedFolder = localStorage.getItem('selectedLogFolder');
        if (savedFolder && logFolders.includes(savedFolder)) {
            logSelector.value = savedFolder;
            // Only trigger change event if there was a saved selection
            logSelector.dispatchEvent(new Event('change'));
        }
    } catch (error) {
        console.error('Error initializing log viewer:', error);
    }
}

async function handleLogChange() {
    // Only proceed if a valid option is selected (not the default one)
    if (!logSelector.value) {
        return;
    }
    
    try {
        document.querySelectorAll('.error-tip').forEach(tip => {
            tip.style.visibility = 'hidden';
        });

        // Save current selection to localStorage
        localStorage.setItem('selectedLogFolder', logSelector.value);

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
