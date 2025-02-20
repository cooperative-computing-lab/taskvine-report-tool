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

        // Restore previously selected log folder from localStorage
        const savedFolder = localStorage.getItem('selectedLogFolder');
        if (savedFolder && logFolders.includes(savedFolder)) {
            logSelector.value = savedFolder;
        } else if (logSelector.options.length > 0) {
            logSelector.selectedIndex = 0;
        }

        // Trigger change event to load the selected log
        logSelector.dispatchEvent(new Event('change'));
    } catch (error) {
        console.error('Error initializing log viewer:', error);
    }
}

async function handleLogChange() {
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
