import { BaseModule } from './base.js';

export class TaskSubgraphsModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        
        this.defineFetchDataParams({
            subgraph_id: 0,
            show_failed_count: false,
            show_recovery_count: false
        });
    }

    _addCustomToolboxItems() {
        const findByFilenameItem = this.toolbox.createInputItem(
            `${this.id}-find-filename`,
            'Find by Filename',
            (id, filename) => {
                this.findSubgraphByFilename(filename);
            },
            'text',
            'Enter File Name'
        );
        
        const findByTaskIdItem = this.toolbox.createInputItem(
            `${this.id}-find-task-id`,
            'Find Task ID',
            (id, taskId) => {
                this.findSubgraphByTaskId(taskId);
            },
            'number',
            'Enter Task ID'
        );
        
        const showFailedCountItem = this.toolbox.createButtonItem(
            `${this.id}-show-failed-count`,
            this.getShowFailedCountButtonText(),
            (id) => {
                this.toggleShowFailedCount();
            }
        );
        
        const showRecoveryCountItem = this.toolbox.createButtonItem(
            `${this.id}-show-recovery-count`,
            this.getShowRecoveryCountButtonText(),
            (id) => {
                this.toggleShowRecoveryCount();
            }
        );
        
        this.addToolboxInputItem(findByFilenameItem);
        this.addToolboxInputItem(findByTaskIdItem);
        this.addToolboxButtonItem(showFailedCountItem);
        this.addToolboxButtonItem(showRecoveryCountItem);
    }

    async findSubgraphByFilename(filename) {
        if (!filename || !filename.trim()) {
            alert('Please enter a filename or substring to search for.');
            return;
        }

        try {
            const params = this.buildSearchParams({ subgraph_id: 0, filename: filename.trim() });
            const response = await fetch(`${this.api_url}?${params.toString()}`);
            const data = await response.json();
            
            if (!response.ok) {
                const errorMsg = data.error || `HTTP ${response.status}: ${response.statusText}`;
                alert(`Error: ${errorMsg}`);
                return;
            }

            if (data && data.error) {
                alert(data.error);
                return;
            }

            if (data && data.subgraph_id && data.subgraph_id > 0) {
                this.data = data;
                this.updateFetchDataParam('subgraph_id', data.subgraph_id);
                this.initLegend();
                this.selectLegendItem(data.subgraph_id.toString());
                this.plot();
            } else {
                alert(`File "${filename.trim()}" not found in any subgraph.`);
            }
        } catch (error) {
            alert(`Network or parsing error: ${error.message}`);
        }
    }

    async findSubgraphByTaskId(taskId) {
        if (!taskId || !taskId.toString().trim()) {
            alert('Please enter a task ID to search for.');
            return;
        }

        const numericTaskId = parseInt(taskId);
        if (isNaN(numericTaskId)) {
            alert('Task ID must be a valid number.');
            return;
        }

        try {
            const params = this.buildSearchParams({ subgraph_id: 0, task_id: numericTaskId.toString() });
            const response = await fetch(`${this.api_url}?${params.toString()}`);
            const data = await response.json();
            
            if (!response.ok) {
                const errorMsg = data.error || `HTTP ${response.status}: ${response.statusText}`;
                alert(`Error: ${errorMsg}`);
                return;
            }

            if (data && data.error) {
                alert(data.error);
                return;
            }

            if (data && data.subgraph_id && data.subgraph_id > 0) {
                this.data = data;
                this.updateFetchDataParam('subgraph_id', data.subgraph_id);
                this.initLegend();
                this.selectLegendItem(data.subgraph_id.toString());
                this.plot();
            } else {
                alert(`Task ID "${numericTaskId}" not found in any subgraph.`);
            }
        } catch (error) {
            alert(`Network or parsing error: ${error.message}`);
        }
    }

    selectLegendItem(subgraphId) {
        const checkboxes = this._queryAllLegendCheckboxes();
        checkboxes.each(function () {
            d3.select(this).property('checked', false);
        });

        const targetCheckbox = this.legendContainer.select(`input[id="${subgraphId}"]`);
        if (!targetCheckbox.empty()) {
            targetCheckbox.property('checked', true);
            targetCheckbox.node().dispatchEvent(new Event('change', { bubbles: true }));
        }
    }

    toggleShowFailedCount() {
        this.updateFetchDataParam('show_failed_count', !this._fetchDataParams.show_failed_count);
        this.updateShowFailedCountButtonText();
        
        if (this._fetchDataParams.subgraph_id > 0) {
            this.fetchDataAndPlot();
        }
    }
    
    getShowFailedCountButtonText() {
        return this._fetchDataParams.show_failed_count ? 'Hide Failed Count' : 'Show Failed Count';
    }
    
    updateShowFailedCountButtonText() {
        const buttonElement = document.getElementById(`${this.id}-show-failed-count`);
        if (buttonElement) {
            buttonElement.textContent = this.getShowFailedCountButtonText();
        }
    }
    
    toggleShowRecoveryCount() {
        this.updateFetchDataParam('show_recovery_count', !this._fetchDataParams.show_recovery_count);
        this.updateShowRecoveryCountButtonText();
        
        if (this._fetchDataParams.subgraph_id > 0) {
            this.fetchDataAndPlot();
        }
    }
    
    getShowRecoveryCountButtonText() {
        return this._fetchDataParams.show_recovery_count ? 'Hide Recovery Count' : 'Show Recovery Count';
    }
    
    updateShowRecoveryCountButtonText() {
        const buttonElement = document.getElementById(`${this.id}-show-recovery-count`);
        if (buttonElement) {
            buttonElement.textContent = this.getShowRecoveryCountButtonText();
        }
    }
    
    buildSearchParams(extraParams = {}) {
        const params = new URLSearchParams({
            folder: this._folder,
            show_failed_count: this._fetchDataParams.show_failed_count,
            ...this._fetchDataParams,
            ...extraParams
        });
        return params;
    }

    resetPlot() {
        // Save current important parameters to preserve state
        const currentSubgraphId = this._fetchDataParams.subgraph_id;
        const currentShowFailedCount = this._fetchDataParams.show_failed_count;
        const currentShowRecoveryCount = this._fetchDataParams.show_recovery_count;
        
        // Clear SVG and custom params only, don't call super.resetPlot() to avoid checkbox reset
        this.clearSVG();
        this.clearCustomParams();
        
        // If we have a selected subgraph, re-fetch and plot with current parameters
        if (currentSubgraphId > 0) {
            this.fetchDataAndPlot();
        } else {
            // If no subgraph selected, just clear the plot
            this.clearSVG();
        }
        
        // Update button texts to reflect current state
        this.updateShowFailedCountButtonText();
        this.updateShowRecoveryCountButtonText();
    }

    legendOnToggle(id, visible) {
        const checkboxes = this._queryAllLegendCheckboxes();
        checkboxes.each(function () {
            const checkbox = d3.select(this);
            if (checkbox.attr('id') !== id) {
                checkbox.property('checked', false);
            }
        });

        if (visible) {
            this.updateFetchDataParam('subgraph_id', id);
            this.fetchDataAndPlot();
        } else {
            this.clearSVG();
        }
    }

    initLegend() {
        if (!this.data || !this.data.legend) return;

        this.createLegendRow(this.data.legend, {
            checkboxName: 'task-subgraph-legend',
            singleSelect: true,
            onToggle: (id, visible) => {
                this.legendOnToggle(id, visible);
            }
        });
    }

    plot() {
        this.clearSVG();
        if (!this.data || !this.data.subgraph_svg_content || this.data.subgraph_id === 0) return;

        const svgElement = new DOMParser().parseFromString(this.data.subgraph_svg_content, 'image/svg+xml').documentElement;
        this.svgNode.parentNode.replaceChild(svgElement, this.svgNode);
        this.svgNode = svgElement;
        this.svgElement = d3.select(this.svgNode);
        this.svgElement.attr('preserveAspectRatio', 'xMidYMid meet');
    }

    _setupZoomAndScroll() {
        // do nothing, we don't support zoom and scroll for task subgraphs
    }
} 