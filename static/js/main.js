import { moduleClasses, moduleConfigs } from './modules/configs.js';
import { LogManager } from './modules/log_manager.js';
import { updateSidebarButtons } from './modules/utils.js';

const moduleObjects = {};

function addFootnote() {
    const content = document.getElementById('content');

    const footnote = document.createElement('div');
    footnote.className = 'footnote';
    footnote.innerHTML = `
    <p>
        TaskVine Report Tool available on GitHub: 
        <i><a href="https://github.com/cooperative-computing-lab/taskvine-report-tool.git" target="_blank">https://github.com/cooperative-computing-lab/taskvine-report-tool.git</a></i>.
    </p>
    `;

    content.appendChild(footnote);
}

async function fetchAllModulesData(folder) {
    try {
        const tasks = moduleConfigs.map(({ id }) => {
            const module = moduleObjects[id];
            return (async () => {
                module.switchFolder(folder);

                try {
                    await module.fetchDataAndPlot();
                } catch (err) {
                    console.error('Error during module data fetch:', err);
                }
            })();
        });

        /* wait for all tasks to finish */
        await Promise.all(tasks);
    } catch (err) {
        console.error('Error during module data fetch:', err);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const root = document.getElementById('content');
    if (!root) {
        console.error('Content root not found');
        return;
    }

    /* initialize log manager */
    const logManager = new LogManager();
    logManager.init();

    /* init modules */
    moduleConfigs.forEach(({ id, title, api_url }) => {
        const module = new moduleClasses[id](id, title, api_url);
        moduleObjects[id] = module;
        root.appendChild(module.renderSkeleton());
        module.init();
    });

    /* on log change -> fetch all modules */
    logManager.registerLogChangeCallback((folder) => {
        return fetchAllModulesData(folder);
    });

    /* update sidebar buttons */
    updateSidebarButtons();

    /* add footnote */
    addFootnote();
});
