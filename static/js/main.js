import { moduleClasses, moduleConfigs } from './modules/configs.js';
import { LogManager } from './modules/log_manager.js';
import { updateSidebarButtons } from './modules/utils.js';


document.addEventListener('DOMContentLoaded', () => {
    const root = document.getElementById('content');
    if (!root) {
        console.error('Content root not found');
        return;
    }

    /* initialize log manager */
    const logManager = new LogManager();
    logManager.init();

    /* initialize modules */
    const moduleObjects = {};
    moduleConfigs.forEach(({ id, title, api_url }) => {
        /* create dom elements for the module */
        const module = new moduleClasses[id](id, title, api_url);
        moduleObjects[id] = module;
        root.appendChild(module.renderSkeleton());
        module.init();
    
        /* monitor log changes */
        logManager.onChange((folder) => {
            module.fetchData(folder).then(() => {
                module.initLegend();
                module.initResetButton();
                module.initDownloadButton();
                module.initResizeHandler();
                module.plot();
            });
        });
    });

    /* update sidebar buttons */
    updateSidebarButtons();
});
