import { downloadSVG } from './tools.js';

const buttonDownload = document.getElementById('button-download-subgraph');
const checkboxFailedTask = document.getElementById('plot-failed-task');
const checkboxRecoveryTask = document.getElementById('plot-recovery-task');
const subgraphSelector = document.getElementById('subgraph-selector');
const svgContainer = d3.select('#subgraph-container');
const loadingSpinner = document.getElementById('subgraph-loading');

const state = {
    selectedSubgraphId: 1,
    subgraphSvgContent: null,
    subgraphIdList: [],
    subgraphNumTasksList: [],
    plotFailedTask: true,
    plotRecoveryTask: true
};

function updateSubgraphSelector() {
    subgraphSelector.innerHTML = '';
    state.subgraphIdList.forEach((id, index) => {
        const option = document.createElement('option');
        option.value = id;
        option.text = `Subgraph ${id} (${state.subgraphNumTasksList[index]} tasks)`;
        if (id === state.selectedSubgraphId) {
            option.selected = true;
        }
        subgraphSelector.appendChild(option);
    });
}

function handleDownloadClick() {
    downloadSVG('subgraph-container', `subgraph-${state.selectedSubgraphId}.svg`);
}

async function initialize() {
    try {
        state.subgraphSvgContent = null;
        svgContainer.selectAll("svg").remove();
        
        // Show loading spinner
        loadingSpinner.style.display = 'block';

        const response = await fetch(
            `/api/subgraphs?subgraph_id=${state.selectedSubgraphId}` +
            `&plot_failed_task=${state.plotFailedTask}` +
            `&plot_recovery_task=${state.plotRecoveryTask}`
        );
        
        if (!response.ok) throw new Error('Network response was not ok');
        const data = await response.json();
        
        state.subgraphIdList = data.subgraph_id_list;
        state.subgraphNumTasksList = data.subgraph_num_tasks_list;
        state.subgraphSvgContent = data.subgraph_svg_content;

        updateSubgraphSelector();

        try {
            const svgContent = new DOMParser().parseFromString(state.subgraphSvgContent, 'image/svg+xml');
            svgContainer.node().appendChild(svgContent.documentElement);

            const insertedSVG = svgContainer.select('svg');
            insertedSVG.attr('preserveAspectRatio', 'xMidYMid meet');

        } catch (error) {
            console.error('Error rendering SVG:', error);
        }
        
        buttonDownload.removeEventListener('click', handleDownloadClick);
        buttonDownload.addEventListener('click', handleDownloadClick);
    } catch (error) {
        console.error('Error initializing subgraphs:', error);
    } finally {
        // Hide loading spinner
        loadingSpinner.style.display = 'none';
    }
}

// Event listeners
checkboxFailedTask.addEventListener('change', (event) => {
    state.plotFailedTask = event.target.checked;
    initialize();
});

checkboxRecoveryTask.addEventListener('change', (event) => {
    state.plotRecoveryTask = event.target.checked;
    initialize();
});

subgraphSelector.addEventListener('change', (event) => {
    state.selectedSubgraphId = parseInt(event.target.value);
    initialize();
});

window.document.addEventListener('dataLoaded', () => {
    state.selectedSubgraphId = 1;  // Set initial subgraph ID
    initialize();
});
window.addEventListener('resize', _.debounce(() => initialize(), 300)); 