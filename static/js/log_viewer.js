import { fetchCSVData } from './tools.js';

// Some global variables
window.xTickFormat = ".0f";
window.xTickFontSize = "12px";
window.yTickFontSize = "12px";

async function handleLogChange() {
    window.logName = this.value;

    // update the url
    history.pushState({}, '', `/logs/${window.logName}`);

    // remove all the svgs
    var svgs = d3.selectAll('svg');
    svgs.each(function() {
        d3.select(this).selectAll('*').remove();
    });

    // hidden some divs
    const headerTips = document.getElementsByClassName('error-tip');
    for (let i = 0; i < headerTips.length; i++) {
        headerTips[i].style.display = 'none';
    }

    document.dispatchEvent(new Event('dataLoaded'));
}

window.addEventListener('load', function() {
    const logSelector = document.getElementById('log-selector');

    // Get the log name from the URL (in case of refresh)
    const currentPath = location.pathname;
    const currentLogName = currentPath.split('/')[2];

    if (currentLogName) {
        window.logName = currentLogName;
        logSelector.value = currentLogName;
    }
    
    // Bind the change event listener to logSelector
    logSelector.addEventListener('change', handleLogChange);

    // Initialize the report iframe if the logSelector has an initial value
    if (logSelector.value) {
        logSelector.dispatchEvent(new Event('change'));
    }
});
