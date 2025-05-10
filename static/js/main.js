import { moduleClasses, moduleConfigs, moduleObjects } from './modules/configs.js';
import { LogManager } from './modules/log_manager.js';

const debouncedResizeMap = new Map();

export function initModules() {
    const root = document.getElementById('content');
    if (!root) {
        console.error('Content root not found');
        return;
    }

    moduleConfigs.forEach(({ id, title, api_url }) => {
        /* create dom elements for the module */
        moduleObjects[id] = new moduleClasses[id](id, title, api_url);

        /* create new html section for the module */
        const section = moduleObjects[id].renderSkeleton();
        root.appendChild(section);

        /* initialize the module */
        moduleObjects[id].init();

        /* handle resize */
        if (!debouncedResizeMap.has(id)) {
            debouncedResizeMap.set(id, _.debounce(() => moduleObjects[id].onResize?.(), 300));
        }
        window.addEventListener('resize', debouncedResizeMap.get(id));

        console.log(`Module "${id}" initialized successfully`);
    });
}

document.addEventListener('DOMContentLoaded', () => {
    initModules();

    const logManager = new LogManager();
    logManager.init();
    logManager.onChange((folder) => {
        /* let all modules fetch data and plot */
        Object.values(moduleObjects).forEach((module) => {
            /* first fetch data and then plot */
            module.fetchData(folder).then(() => {
                module.initLegend();
                module.plot();
                module.initResetButton();
                module.initDownloadButton();
            });
        });
    });

});
