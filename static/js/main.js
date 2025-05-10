import { moduleClasses, moduleConfigs } from './modules/configs.js';
import { LogManager } from './modules/log_manager.js';


function updateSidebarButtons() {
    const sectionHeaders = Array.from(document.querySelectorAll('.section-header'));
    const sidebar = document.querySelector('#sidebar');
    
    const existingButtons = sidebar.querySelectorAll('.report-scroll-btn');
    existingButtons.forEach(btn => btn.remove());
    
    sectionHeaders.sort((a, b) => {
        return a.getBoundingClientRect().top - b.getBoundingClientRect().top;
    });
    
    sectionHeaders.forEach(header => {
        const title = header.querySelector('.section-title');
        if (title) {
            const button = document.createElement('button');
            button.textContent = title.textContent;
            button.classList.add('report-scroll-btn');
            button.addEventListener('click', () => {
                header.scrollIntoView({ behavior: 'smooth' });
            });
            sidebar.appendChild(button);
        }
    });
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
                module.initZoomTrackingAfterRender();
                module.setupZoomAndScroll();
            });
        });
    });

    /* update sidebar buttons */
    updateSidebarButtons();
});
