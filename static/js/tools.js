export function createModuleRefs(moduleId) {
    const get = (id) => {
        console.log(`getting ${id}`)
        const el = document.getElementById(id);
        if (!el) {
            console.warn(`Missing element: #${id}`);
        }
        return el;
    };

    return {
        svgContainer: get(`${moduleId}-container`),
        loadingSpinner: get(`${moduleId}-loading`),
        svgElement: d3.select(`#${moduleId}`),
        tooltip: get('vine-tooltip'),
        buttonsContainer: get(`${moduleId}-buttons`),
        legendContainer: get(`${moduleId}-legend`),
        resetButton: get(`${moduleId}-reset-button`),
        downloadButton: get(`${moduleId}-download-button`),
    };
}


export function formatUnixTimestamp(unixTimestamp, format = 'YYYY-MM-DD HH:mm:ss.SSS') {
    var date = new Date(unixTimestamp * 1000);
    var year = date.getFullYear();
    var month = ('0' + (date.getMonth() + 1)).slice(-2);
    var day = ('0' + date.getDate()).slice(-2);
    var hours = ('0' + date.getHours()).slice(-2);
    var minutes = ('0' + date.getMinutes()).slice(-2);
    var seconds = ('0' + date.getSeconds()).slice(-2);
    var milliseconds = ('00' + date.getMilliseconds()).slice(-3); 

    switch (format) {
        case 'YYYY-MM-DD':
            return `${year}-${month}-${day}`;
        case 'YYYY-MM-DD HH:mm:ss':
            return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
        case 'MM/DD/YYYY':
            return `${month}/${day}/${year}`;
        case 'YYYY-MM-DD HH:mm:ss.SSS':
            return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
        default:
            return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
    }
}

export async function fetchCSVData(logName, csvFilename) {
    try {
        const response = await axios.get(`/logs/${logName}/get_csv_data`, {
            params: {
                csv_filename: csvFilename
            },
        });
        return response.data;
    } catch (error) {
        console.error('Error fetching data for:', csvFilename, 'Error:', error);
        return null;
    }
}


export async function fetchFile(filePath) {
    try {
        const response = await fetch(filePath);
        if (!response.ok) {
            throw new Error(`Failed to fetch file: ${filePath} (${response.statusText})`);
        }
        return await response.text();
    } catch (error) {
        console.error(error);
        throw error;
    }
}

export function pathJoin(parts, sep) {
    var separator = sep || '/';
    var replace = new RegExp(separator + '{1,}', 'g');
    return parts.join(separator).replace(replace, separator);
}

export function setupZoomAndScroll(svgElementName, svgContainerName) {
    const svgElement = document.querySelector(svgElementName); // Select the SVG element.
    const svgContainer = document.querySelector(svgContainerName); // Select the container of the SVG.

    // Store the initial width and height of the SVG.
    let initialWidth = svgElement.getBoundingClientRect().width;
    let initialHeight = svgElement.getBoundingClientRect().height;

    // Define the maximum and minimum zoom scales.
    const maxWidth = initialWidth * 64;
    const maxHeight = initialHeight * 64; 
    const minWidth = initialWidth * 0.95;
    const minHeight = initialHeight * 0.95;

    svgContainer.addEventListener('wheel', function(event) {
        if (event.ctrlKey) { // Check if the Ctrl key is pressed during scroll.
            event.preventDefault(); // Prevent the default scroll behavior.

            const zoomFactor = event.deltaY < 0 ? 1.1 : 0.9; // Determine the zoom direction.
            let newWidth = initialWidth * zoomFactor; // Calculate the new width based on the zoom factor.
            let newHeight = initialHeight * zoomFactor; // Calculate the new height based on the zoom factor.

            // Check if the new dimensions exceed the zoom limits.
            if ((newWidth >= maxWidth && zoomFactor > 1) || (newWidth <= minWidth && zoomFactor < 1) ||
                (newHeight >= maxHeight && zoomFactor > 1) || (newHeight <= minHeight && zoomFactor < 1)) {
                return; // If the new dimensions are outside the limits, exit the function.
            }

            // Calculate the mouse position relative to the SVG content before scaling.
            const rect = svgElement.getBoundingClientRect(); // Get the current size and position of the SVG.
            const mouseX = event.clientX - rect.left; // Mouse X position within the SVG.
            const mouseY = event.clientY - rect.top; // Mouse Y position within the SVG.

            // Determine the mouse position as a fraction of the SVG's width and height.
            const scaleX = mouseX / rect.width; 
            const scaleY = mouseY / rect.height; 

            // Apply the new dimensions to the SVG element.
            svgElement.style.width = `${newWidth}px`;
            svgElement.style.height = `${newHeight}px`;

            // After scaling, calculate where the mouse position would be relative to the new size.
            const newRect = svgElement.getBoundingClientRect(); // Get the new size and position of the SVG.
            const targetX = scaleX * newRect.width; 
            const targetY = scaleY * newRect.height; 

            // Calculate the scroll offsets needed to keep the mouse-over point visually static.
            const offsetX = targetX - mouseX; 
            const offsetY = targetY - mouseY; 

            // Adjust the scroll position of the container to compensate for the scaling.
            svgContainer.scrollLeft += offsetX;
            svgContainer.scrollTop += offsetY;

            // Update the initial dimensions for the next scaling operation.
            initialWidth = newWidth;
            initialHeight = newHeight;
        }
    });
}

export function downloadSVG(svgElementId, filename = null) {
    const svgElement = document.getElementById(svgElementId);
    if (!svgElement) {
        console.error('SVG element not found');
        return;
    }
    if (!filename) {
        filename = svgElementId.replace(/-/g, '_');
        if (filename.endsWith('svg')) {
            filename = filename.substring(0, filename.length - 4);
        }
        filename = filename + '.svg';
    }

    // apply inline styles
    function applyInlineStyles(element) {
        const style = element.getAttribute('style');
        if (style) {
            console.log(style);
            const styleProperties = style.split(';');
            styleProperties.forEach(property => {
                const [key, value] = property.split(':');
                if (key && value) {
                    element.setAttribute(key.trim(), value.trim());
                }
            });

            element.removeAttribute('style');
        }
        Array.from(element.children).forEach(applyInlineStyles);
    }
    applyInlineStyles(svgElement);


    // serialize and download
    const serializer = new XMLSerializer();
    let svgString = serializer.serializeToString(svgElement);

    const blob = new Blob([svgString], {type: "image/svg+xml"});
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
}


export function getTaskInnerHTML(task) {
    const precision = 3;
    let htmlContent = `
        Task ID: ${task.task_id}<br>
        Try ID: ${task.try_id}<br>
        Worker: ${task.worker_ip}:${task.worker_port}<br>
        Core ID:  ${task.core_id}<br>
        Inputs: ${task.input_files || 'N/A'}<br>
        Outputs: ${task.output_files || 'N/A'}<br>
        When Ready: ${task.when_ready ? task.when_ready.toFixed(precision) : 'N/A'}<br>
        When Running: ${task.when_running ? task.when_running.toFixed(precision) : 'N/A'}<br>
        When Start on Worker: ${task.time_worker_start ? task.time_worker_start.toFixed(precision) : 'N/A'}<br>
        When End on Worker: ${task.time_worker_end ? task.time_worker_end.toFixed(precision) : 'N/A'}<br>
        When Waiting Retrieval: ${task.when_waiting_retrieval ? task.when_waiting_retrieval.toFixed(precision) : 'N/A'}<br>
        When Retrieved: ${task.when_retrieved ? task.when_retrieved.toFixed(precision) : 'N/A'}<br>
        When Done: ${task.when_done ? task.when_done.toFixed(precision) : 'N/A'}<br>
    `;

    return htmlContent;
}

export function resetContainer(divId) {
    const containerId = divId + '-container';
    const spinnerId = divId + '-loading';
    const container = document.getElementById(containerId);
    if (container) {
        const svg = container.querySelector('svg');
        if (svg) {
            while (svg.firstChild) svg.removeChild(svg.firstChild);
        }
    }
    const spinner = document.getElementById(spinnerId);
    if (spinner) spinner.style.display = 'block';
}