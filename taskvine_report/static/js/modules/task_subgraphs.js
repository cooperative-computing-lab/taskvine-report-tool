import { BaseModule } from './base.js';

export class TaskSubgraphsModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        
        this.defineFetchDataParams({
            subgraph_id: 0
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
            'Enter filename'
        );
        
        const findByTaskIdItem = this.toolbox.createInputItem(
            `${this.id}-find-task-id`,
            'Find Task ID',
            (id, taskId) => {
                this.findSubgraphByTaskId(taskId);
            },
            'number',
            'Enter task ID'
        );
        
        this.addToolboxInputItem(findByFilenameItem);
        this.addToolboxInputItem(findByTaskIdItem);
    }

    async findSubgraphByFilename(filename) {
        if (!filename || !filename.trim()) {
            alert('Please enter a filename or substring to search for.');
            return;
        }

        try {
            // fetch data with filename parameter and subgraph_id=0
            const params = new URLSearchParams({
                folder: this._folder,
                subgraph_id: 0,
                filename: filename.trim()
            });
            
            const response = await fetch(`${this.api_url}?${params.toString()}`);
            const data = await response.json();
            
            if (!response.ok) {
                // Handle HTTP error responses
                const errorMsg = data.error || `HTTP ${response.status}: ${response.statusText}`;
                alert(`Error: ${errorMsg}`);
                console.error('HTTP Error:', response.status, response.statusText, data);
                return;
            }

            if (data && data.error) {
                alert(data.error);
                return;
            }

            if (data && data.subgraph_id && data.subgraph_id > 0) {
                // found the subgraph, update the legend and display
                this.data = data;
                this.updateFetchDataParam('subgraph_id', data.subgraph_id);
                this.initLegend(); // update legend first
                this.selectLegendItem(data.subgraph_id.toString());
                this.plot();
            } else {
                alert(`File "${filename.trim()}" not found in any subgraph.`);
            }
        } catch (error) {
            console.error('Error finding subgraph by filename:', error);
            alert(`Network or parsing error: ${error.message}`);
        }
    }

    async findSubgraphByTaskId(taskId) {
        if (!taskId || !taskId.toString().trim()) {
            alert('Please enter a task ID to search for.');
            return;
        }

        // validate that taskId is a number
        const numericTaskId = parseInt(taskId);
        if (isNaN(numericTaskId)) {
            alert('Task ID must be a valid number.');
            return;
        }

        try {
            // fetch data with task_id parameter and subgraph_id=0
            const params = new URLSearchParams({
                folder: this._folder,
                subgraph_id: 0,
                task_id: numericTaskId.toString()
            });
            
            const response = await fetch(`${this.api_url}?${params.toString()}`);
            const data = await response.json();
            
            if (!response.ok) {
                // Handle HTTP error responses
                const errorMsg = data.error || `HTTP ${response.status}: ${response.statusText}`;
                alert(`Error: ${errorMsg}`);
                console.error('HTTP Error:', response.status, response.statusText, data);
                return;
            }

            if (data && data.error) {
                alert(data.error);
                return;
            }

            if (data && data.subgraph_id && data.subgraph_id > 0) {
                // found the subgraph, update the legend and display
                this.data = data;
                this.updateFetchDataParam('subgraph_id', data.subgraph_id);
                this.initLegend(); // update legend first
                this.selectLegendItem(data.subgraph_id.toString());
                this.plot();
            } else {
                alert(`Task ID "${numericTaskId}" not found in any subgraph.`);
            }
        } catch (error) {
            console.error('Error finding subgraph by task ID:', error);
            alert(`Network or parsing error: ${error.message}`);
        }
    }

    selectLegendItem(subgraphId) {
        // uncheck all legend checkboxes first
        const checkboxes = this._queryAllLegendCheckboxes();
        checkboxes.each(function () {
            d3.select(this).property('checked', false);
        });

        // check the target checkbox using attribute selector to avoid CSS selector issues with numeric IDs
        const targetCheckbox = this.legendContainer.select(`input[id="${subgraphId}"]`);
        if (!targetCheckbox.empty()) {
            targetCheckbox.property('checked', true);
            // trigger the change event to update the plot
            targetCheckbox.node().dispatchEvent(new Event('change', { bubbles: true }));
        }
    }

    resetPlot() {
        super.resetPlot();

        this.data = null;
        this.updateFetchDataParam('subgraph_id', 0);
    }

    legendOnToggle(id, visible) {
        const checkboxes = this._queryAllLegendCheckboxes();
        
        /* if there are other checkboxes, uncheck them */
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
            /* if the current checkbox is unchecked, clear the SVG */
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
} 